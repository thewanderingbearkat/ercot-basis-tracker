"""Energy waterfall engine: every unit (turbine / inverter) 10-min interval is valued and assigned to one step.

  0 P50 potential        PowerUp modelled output (element 782), 2015-2025 mean for the same hour of year, x k
                         (calibrated once per plant to measured potential: reference/calibration.json)
  1 Resource             P50 potential - potential at actual resource
  2 Potential at actual  wind: reference curve at density-normalised nacelle wind (GADS Expected Generation basis,
                         KB 36000525814); stopped turbines' anemometers read low, so they take the running-neighbour
                         median wind (cf. TheoreticalPowerOption 2), or own wind / f in site-wide stops.
                         solar: Bazefield inverter TheoreticalPower (irradiance-based expected output).
  3 Unmeasured           potential of intervals with no SCADA (site-median, or k x PowerUp for site-wide gaps)
  4 Curtailment          GADS Reserve Shutdown Derated / curtailment categories + OEM setpoints
  5 External stops       GADS Reserve Shutdown (stop by external device)
  6 Availability (PBA)   (PV_)GADS categories with Available = false - the categories Bazefield PBA counts
                         (KB 36000615255, 36000610511)
  7 Other non-PBA        Resource Unavailable + SCADA stops Bazefield did not allocate
  8 Performance          full-performance intervals by SCADA signature; remainder = curve / expected shortfall
  9 Gross (measured)     sum of unit ActivePower
While the site is curtailed, a down unit's potential the running fleet could not deliver moves to steps 4/5
(waterfall order: curtailment before availability; matches Bazefield's ExpectedPower valuation, docs: Core
Monitoring KPIs - Turbine). Bazefield's own lost production is kept alongside for the PBA tie-out.
"""
import logging
import math
import uuid
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

from energy_waterfall import config as C
from energy_waterfall import sources as S

log = logging.getLogger(__name__)
R_DRY, RHO0 = 287.05, 1.225
LT_YEARS = range(2015, 2026)
SITE_WIDE = 100 / 137         # share of units in the same state = site-wide event (100 of 137 at Lakefield)
COMP_YES = "Presumed compensated (IPL economic, price <= threshold)"
COMP_NO = "Not presumed compensated"
COMP_NONE = "Not compensated (no PPA add-back)"

GADS_NAMES = {"Forced/FTH": "Forced outage (FTH)", "Forced Derated/EFDTH": "Forced derate (EFDTH)",
              "Maintenance/MTH": "Maintenance (MTH)", "Planned/PTH": "Planned outage (PTH)",
              "OMC Forced/oFTH": "Outside mgmt control forced (oFTH)",
              "Resource Unavailable/RUTH": "Resource unavailable (RUTH)"}
PERF_SUB = {"NORMAL": "Curve shortfall in normal operation", "STALL_AVOIDANCE": "Stall-avoidance pitch limit",
            "PITCH_OFF_OPTIMUM": "Unexplained pitch derate", "PITCH_IMBALANCE": "Blade pitch imbalance",
            "YAW_MISALIGNED": "Yaw misalignment", "ICING_SUSPECT": "Suspected icing",
            "TEMP_DERATE": "Temperature derate"}
STOP_SUB = {"NOT_AVAILABLE": "Not available", "ALARM_STOP": "Alarm stop", "PITCH_FAULT": "Pitch fault",
            "STANDBY_IN_WIND": "Standby in wind"}


def step_names(plant: str) -> dict:
    p = C.PLANTS[plant]
    n = dict(C.STEP_NAMES)
    if p["tech"] == "wind":
        n.update({2: "Potential at actual wind", 8: "Performance (power curve)", 9: "Turbine gross (measured)"})
    else:
        n.update({2: "Potential at actual irradiance", 8: "Performance (vs expected)", 9: "Inverter gross (measured)"})
    n[4] = p.get("curtail_label", n[4])
    return n


# ----------------------------------------------------------------------------------------------- helpers
def per_stamp(values, stamps, how):
    return pd.Series(values).groupby(stamps).transform(how).values


def overlap_flags(t0: np.ndarray, ivs: pd.DataFrame) -> np.ndarray:
    """True where [t, t+10min) overlaps any interval in ivs (columns start, end)."""
    hit = np.zeros(len(t0), dtype=bool)
    if ivs is None or not len(ivs):
        return hit
    t1 = t0 + np.timedelta64(10, "m")
    for st_, en in zip(ivs["start"].values.astype("datetime64[ns]"), ivs["end"].values.astype("datetime64[ns]")):
        lo, hi = np.searchsorted(t1, st_, side="right"), np.searchsorted(t0, en, side="left")
        hit[lo:hi] = True
    return hit


def map_allocations(idx: pd.DatetimeIndex, al: pd.DataFrame):
    """(PV_)GADS allocation covering each interval midpoint -> (category, available, root)."""
    n = len(idx)
    cat, avail, root = np.full(n, None, dtype=object), np.ones(n, dtype=bool), np.full(n, "", dtype=object)
    if al is None or not len(al):
        return cat, avail, root
    mid = (idx + pd.Timedelta(minutes=5)).values
    st, en = al["start"].values, al["end"].values
    j = np.searchsorted(st, mid, side="right") - 1
    ok = (j >= 0) & (en[np.clip(j, 0, None)] > mid)
    jj = j[ok]
    cat[ok], avail[ok], root[ok] = al["cat"].values[jj], al["available"].values[jj], al["root"].values[jj]
    return cat, avail, root


def allocations_frame(rows: list[dict], now) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["object_id", "start", "end", "cat", "available", "excluded", "root", "lost_kwh"])
    df["start"] = df["start"].map(S.bz_time)
    df["end"] = df["end"].map(S.bz_time).fillna(pd.Timestamp(now))
    return df.dropna(subset=["start"]).sort_values(["object_id", "start"])


def prorate(al: pd.DataFrame, hours: pd.DatetimeIndex) -> np.ndarray:
    out, h0 = np.zeros(len(hours)), (hours - pd.Timedelta(hours=1)).values
    for s, e, kwh in zip(al["start"].values, al["end"].values, al["lost_kwh"].values):
        dur = (e - s) / np.timedelta64(1, "s")
        if dur <= 0 or not kwh:
            continue
        ov = (np.minimum(hours.values, e) - np.maximum(h0, s)) / np.timedelta64(1, "s")
        out += np.clip(ov, 0, None) / dur * kwh / 1000.0
    return out


def meter_series(plant: str, p: dict, start: date, end: date) -> pd.Series:
    """Hourly revenue meter (MWh, hour ending UTC); empty when the source has nothing for the window."""
    try:
        return S.meter_hourly(plant, p, start, end)
    except Exception:
        log.exception("%s meter fetch failed - waterfall continues without the meter step", plant)
        return pd.Series(dtype=float)


def long_term(srcid: str, element: int, hours: pd.DatetimeIndex, utc_stamps: bool) -> pd.Series:
    """Mean over LT_YEARS of the same (month, day, hour) UTC hour-ending value."""
    months = pd.period_range((hours.min() - pd.Timedelta(hours=1)).to_period("M"), hours.max().to_period("M"), freq="M")
    vals = []
    for y in LT_YEARS:
        parts = [S.powerup_history(srcid, element, date(y, m.month, 1),
                                   (pd.Timestamp(y, m.month, 1) + pd.offsets.MonthBegin(1)).date(), utc_stamps)
                 for m in months]
        s = pd.concat(parts)
        s = s[~s.index.duplicated()]
        s.index = pd.MultiIndex.from_arrays([s.index.month, s.index.day, s.index.hour])
        vals.append(s)
    lt = pd.concat(vals, axis=1).mean(axis=1)
    key = pd.MultiIndex.from_arrays([hours.month, np.where((hours.month == 2) & (hours.day == 29), 28, hours.day),
                                     hours.hour])
    return pd.Series(lt.reindex(key).values, index=hours)


# ----------------------------------------------------------------------------------------------- wind
def classify_wind(df: pd.DataFrame, plant: str, ref, alarm_iv: pd.DataFrame | None) -> pd.DataFrame:
    p = C.PLANTS[plant]
    p_site = 101325.0 * (1 - 2.25577e-5 * p["elevation_m"]) ** 5.25588
    rho = p_site / (R_DRY * (df["AmbientTemperature"].fillna(10.0) + 273.15))
    df["ws_n"] = df["WindSpeed"] * ((rho / RHO0) ** (1 / 3) if p.get("density_correct", True) else 1.0)
    df["p_ref"] = ref(df["ws_n"])
    blades = df[["Blade1_PitchAngle", "Blade2_PitchAngle", "Blade3_PitchAngle"]]
    df["pitch"], df["pitch_spread"] = blades.mean(axis=1), blades.max(axis=1) - blades.min(axis=1)
    avail_ok = df["Available"] >= 0.99
    run_ok = avail_ok & (df["Running"] >= 0.99)
    fine_sel = run_ok & df["ws_n"].between(5, 8) & (df["ActivePower"] > 0.033 * p["rated_kw"])
    fine = float(df.loc[fine_sel, "pitch"].median()) if fine_sel.sum() >= 20 else np.nan
    codes = p.get("alarm_codes") or {}
    groups = {"a_curtail": codes.get("curtail", set()), "a_derate": codes.get("temp_derate", set()),
              "a_stall": codes.get("stall", set()), "a_pitch": codes.get("pitch_fault", set())}
    t0 = df.index.values
    known = set().union(*groups.values(), codes.get("info", set()))
    iv_all = alarm_iv[alarm_iv["object_id"] == df["object_id"].iat[0]] if alarm_iv is not None and len(alarm_iv) else None
    for g, cs in groups.items():
        df[g] = overlap_flags(t0, iv_all[iv_all["code"].isin(cs)]) if iv_all is not None and cs else False
    df["a_stop"] = overlap_flags(t0, iv_all[iv_all["root"] & ~iv_all["code"].isin(known)]) if iv_all is not None else False
    wrap = lambda x: (x + 180.0) % 360.0 - 180.0
    north = lambda x: np.minimum(x % 360, 360 - x % 360) < 20
    df["yaw_err"] = wrap(df["WindDirection"] - df["NacelleDirection"]).where(
        ~(north(df["WindDirection"]) | north(df["NacelleDirection"])))
    if p.get("curtail_setpoints"):
        curtailed = np.zeros(len(df), dtype=bool)
        for sp in p["curtail_setpoints"]:
            curtailed |= (df[sp].fillna(0) > 1).values
    elif p.get("setpoint_point"):
        # a setpoint below rating only curtails when it binds: below what the turbine would make at this wind
        sp = df[p["setpoint_point"]]
        curtailed = ((sp < 0.97 * p["rated_kw"]) & (sp < 0.95 * df["p_ref"])).fillna(False).values
    else:
        curtailed = np.zeros(len(df), dtype=bool)
    pw = df["ActivePower"]
    conds = [
        ("NO_DATA", df["WindSpeed"].isna() | pw.isna()),
        ("CURTAILED", pd.Series(curtailed, index=df.index) | df["a_curtail"]),
        ("PITCH_FAULT", df["a_pitch"]),
        ("ALARM_STOP", df["a_stop"]),
        ("NOT_AVAILABLE", ~avail_ok),
        ("TEMP_DERATE", df["a_derate"]),
        ("STALL_AVOIDANCE", df["a_stall"]),
        ("STANDBY_IN_WIND", (df["Running"] < 0.99) & (df["ws_n"] > 4.5) & (pw < 0.5 * df["p_ref"])),
        ("PITCH_IMBALANCE", df["pitch_spread"] > 1.5),
        ("PITCH_OFF_OPTIMUM", (df["ws_n"] >= 5) & (pw > 0.033 * p["rated_kw"]) & (pw < 0.9 * df["p_ref"])
                              & (df["pitch"] > (fine if not math.isnan(fine) else 0) + 2.5)),
        ("YAW_MISALIGNED", df["yaw_err"].abs() > 15),
        ("ICING_SUSPECT", (df["AmbientTemperature"] < 2) & (df["ws_n"] > 5) & (pw < 0.5 * df["p_ref"])),
    ]
    cat = pd.Series("NORMAL", index=df.index)
    assigned = pd.Series(False, index=df.index)
    for name, m in conds:
        m = pd.Series(m, index=df.index).fillna(False).astype(bool) & ~assigned
        cat[m] = name
        assigned |= m
    df["cat"] = cat
    return df


def value_wind(plant, frames, ref, allocs, cal, recalibrate, pu_hourly):
    p = C.PLANTS[plant]
    all_ = pd.concat(frames)
    ts = all_.index
    tsv = ts.values
    he = ts.floor("h") + pd.Timedelta(hours=1)
    n_units = len(frames)
    meas_a = (all_["cat"] != "NO_DATA").values
    run_a = meas_a & (all_["Running"] >= 0.99).values & (all_["Available"] >= 0.99).values
    stop_a = meas_a & (all_["Running"] <= 0.01).values
    wsn = all_["ws_n"].values.astype(float)
    n_run = per_stamp(run_a.astype(int), tsv, "sum")
    n_stop = per_stamp(stop_a.astype(int), tsv, "sum")
    n_rep = per_stamp(meas_a.astype(int), tsv, "sum")
    med_run = per_stamp(np.where(run_a, wsn, np.nan), tsv, "median")
    med_stop = per_stamp(np.where(stop_a, wsn, np.nan), tsv, "median")
    med_any = per_stamp(np.where(meas_a, wsn, np.nan), tsv, "median")

    f_site = cal.get("stopped_anemometer_factor")
    if f_site is None or recalibrate:
        puw = pu_hourly["wind"].reindex(he).values
        ok = (puw >= 5) & (puw <= 15)
        big = SITE_WIDE * n_units
        sel_r, sel_s = ok & (n_run >= big), ok & (n_stop >= big)
        f_raw = (float(np.nanmedian(med_stop[sel_s] / puw[sel_s]) / np.nanmedian(med_run[sel_r] / puw[sel_r]))
                 if sel_s.sum() >= 180 and sel_r.sum() >= 180 else 1.0)
        # only trust a plausible, well-supported factor (Lakefield 0.915 from 2,161 stopped hours); else no correction
        f_site = f_raw if 0.85 <= f_raw <= 1.15 and sel_s.sum() >= 360 else 1.0
        cal["stopped_anemometer_factor"] = f_site
        log.info("%s stopped-anemometer factor %.3f (raw %.3f, %d site-wide stopped stamps)", plant, f_site, f_raw,
                 int(sel_s.sum()))

    pot = all_["p_ref"].values.astype(float).copy()
    partial = stop_a & (n_run >= max(5, 20 / 137 * n_units)) & ~np.isnan(med_run)   # 20 running at Lakefield
    pot[partial] = ref(med_run[partial])
    sw_stop = stop_a & ~partial
    pot[sw_stop] = ref(wsn[sw_stop] / f_site)
    nd = ~meas_a
    sw_gap = nd & (n_rep < max(3, 0.07 * n_units))
    pot[nd & ~sw_gap] = ref(med_any[nd & ~sw_gap])
    pot = np.nan_to_num(pot)
    act = np.where(meas_a, all_["ActivePower"].values.astype(float), 0.0)

    g = all_["gcat"].values
    sc = all_["cat"].values
    rs = per_stamp(((g == "Reserve Shutdown/RSTH") & meas_a).astype(int), tsv, "sum") >= SITE_WIDE * n_units
    rsd = per_stamp(((g == "Reserve Shutdown Derated/ERSDTH") & meas_a).astype(int), tsv, "sum") >= SITE_WIDE * n_units
    step = np.full(len(all_), 8)
    sub = np.array([PERF_SUB.get(c, c) for c in sc], dtype=object)
    det = np.where(pd.isna(g), "No GADS allocation", "").astype(object)
    root = all_["groot"].values
    pba = (~all_["gavail"].values) & ~pd.isna(g)

    def put(mask, s, sb, d):
        nonlocal step, sub, det
        step, sub, det = np.where(mask, s, step), np.where(mask, sb, sub), np.where(mask, d, det)

    full = pd.isna(g) | (g == "Full Performance/IAOGFP")
    put(full & np.isin(sc, list(STOP_SUB)), 7, "SCADA stop not allocated in Bazefield",
        np.array([STOP_SUB.get(c, "") + (" (no GADS allocation)" if x is None else " (GADS full performance)")
                  for c, x in zip(sc, g)], dtype=object))
    put(full & (sc == "CURTAILED"), 4, "Setpoint / curtailment alarm outside GADS reserve shutdown",
        np.where(all_["a_curtail"].values, "Curtailment alarm", "OEM curtailment setpoint"))
    put(g == "Resource Unavailable/RUTH", 7, GADS_NAMES["Resource Unavailable/RUTH"], root)
    other = ~pd.isna(g) & ~np.isin(g, ["Full Performance/IAOGFP", "Resource Unavailable/RUTH", "Reserve Shutdown/RSTH",
                                       "Reserve Shutdown Derated/ERSDTH"]) & ~pba
    put(other, 7, np.array([str(x) for x in g], dtype=object), root)
    put(g == "Reserve Shutdown Derated/ERSDTH", 4,
        np.where(rsd, "Derate - site-wide", "Derate - partial site"), root)
    put(g == "Reserve Shutdown/RSTH", 5, np.where(rs, "Stop - site-wide", "Stop - partial site"), root)
    put(pba, 6, np.array([GADS_NAMES.get(x, str(x)) for x in g], dtype=object), root)
    return dict(all_=all_, ts=ts, tsv=tsv, he=he, meas=meas_a, run=run_a, pot=pot, act=act, step=step, sub=sub,
                det=det, nd=nd, sw_gap=sw_gap, n_units=n_units, is_curt=(g == "Reserve Shutdown Derated/ERSDTH") |
                (full & (sc == "CURTAILED")), is_stop=(g == "Reserve Shutdown/RSTH"), a_curtail=all_["a_curtail"].values)


# ----------------------------------------------------------------------------------------------- solar
def value_solar(plant, frames, allocs):
    all_ = pd.concat(frames)
    ts = all_.index
    tsv = ts.values
    he = ts.floor("h") + pd.Timedelta(hours=1)
    n_units = len(frames)
    meas_a = (all_["ActivePower"].notna() & all_["TheoreticalPower"].notna()).values
    pot = all_["TheoreticalPower"].clip(lower=0).values.astype(float)
    n_rep = per_stamp(meas_a.astype(int), tsv, "sum")
    med_pot = per_stamp(np.where(meas_a, pot, np.nan), tsv, "median")
    nd = ~meas_a
    sw_gap = nd & (n_rep < max(3, 0.07 * n_units))
    pot = np.where(nd & ~sw_gap, med_pot, pot)
    pot = np.nan_to_num(pot)
    act = np.where(meas_a, all_["ActivePower"].values.astype(float), 0.0)
    # clipping level per inverter: its 99.5th percentile output in the window (the AC limit actually reached)
    clip = all_.groupby("object")["ActivePower"].transform(lambda s: s.quantile(0.995)).values
    running = (all_["Running"] >= 0.5).values
    g = all_["gcat"].values
    gname = np.array([str(x) if x is not None else "" for x in g], dtype=object)
    pba = (~all_["gavail"].values) & ~pd.isna(g)
    root = all_["groot"].values
    step = np.full(len(all_), 8)
    sub = np.where((act >= 0.97 * clip) & (pot > act), "Clipping at inverter AC limit",
                   "Shortfall vs expected (soiling, temperature, mismatch)").astype(object)
    det = np.where(pd.isna(g), "No PV_GADS allocation", "").astype(object)

    def put(mask, s, sb, d):
        nonlocal step, sub, det
        step, sub, det = np.where(mask, s, step), np.where(mask, sb, sub), np.where(mask, d, det)

    lower = np.char.lower(gname.astype(str))
    in_service = ~pba & ~np.char.startswith(lower, "resource unavailable") & ~(np.char.find(lower, "reserve") >= 0) \
        & ~(np.char.find(lower, "curtail") >= 0)
    put(meas_a & in_service & ~running & (pot > 0.05 * clip), 7, "SCADA stop not allocated in Bazefield",
        "Inverter not running (PV_GADS in service)")
    put(np.char.startswith(lower, "resource unavailable"), 7, gname, root)
    put((np.char.find(lower, "reserve") >= 0) | (np.char.find(lower, "curtail") >= 0), 4, gname, root)
    put(pba, 6, gname, root)
    return dict(all_=all_, ts=ts, tsv=tsv, he=he, meas=meas_a, run=running & meas_a, pot=pot, act=act, step=step,
                sub=sub, det=det, nd=nd, sw_gap=sw_gap, n_units=n_units,
                is_curt=(np.char.find(lower, "reserve") >= 0) | (np.char.find(lower, "curtail") >= 0),
                is_stop=np.zeros(len(all_), dtype=bool), a_curtail=np.zeros(len(all_), dtype=bool))


# ----------------------------------------------------------------------------------------------- build
def build(plant: str, start: date, end: date, recalibrate: bool = False, bz: S.Bazefield | None = None):
    """Fact (unit x hour x step x sub-category) and site-hour context for [start, end] (UTC days)."""
    p = C.PLANTS[plant]
    bz = bz or S.Bazefield()
    t0 = max(pd.Timestamp(start), pd.Timestamp(p["data_start"]))
    t1 = pd.Timestamp(end) + pd.Timedelta(hours=23, minutes=50)
    grid = pd.date_range(t0, t1, freq="10min")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    objs = bz.plant_objects(plant)
    units, site = objs["units"], objs["site"]
    ids = [u["id"] for u in units]
    log.info("%s: %d units, %s .. %s", plant, len(units), t0, t1)

    # allocations (+1 day margin each side: lists return overlapping events)
    allocs = allocations_frame(bz.allocations(ids, p["allocation_type"], t0.date() - timedelta(days=1), end), now)
    alarm_iv = None
    if p["tech"] == "wind" and p.get("alarm_codes"):
        a = pd.DataFrame(bz.alarms(ids, t0.date() - timedelta(days=1), end))
        if len(a):
            a = a[a["type"] == "Alarm"].copy()
            a["start"] = a["start"].map(S.bz_time)
            a["end"] = a["end"].map(S.bz_time).fillna(pd.Timestamp(now))
            a["code"] = pd.to_numeric(a["code"], errors="coerce").fillna(-1).astype(int)
            alarm_iv = a.dropna(subset=["start"])

    ref = C.reference_curve(plant) if p["tech"] == "wind" else None
    frames = []
    for u in units:
        df = bz.series(u["id"], p["points"], t0.date(), end)
        for col in p["points"]:
            if col not in df:
                df[col] = np.nan
        if p["tech"] == "wind":            # zero-filled placeholder rows (onboarding) are missing data
            ph = (df["WindSpeed"].fillna(0) == 0) & (df["ActivePower"].fillna(0) == 0) & \
                 (df["AmbientTemperature"].fillna(0) == 0)
            df.loc[ph, ["WindSpeed", "ActivePower"]] = np.nan
        df = df.reindex(grid)
        df["object"], df["object_id"] = u["key"], u["id"]
        if p["tech"] == "wind":
            df = classify_wind(df, plant, ref, alarm_iv)
        a = allocs[allocs["object_id"] == u["id"]] if len(allocs) else allocs
        df["gcat"], df["gavail"], df["groot"] = map_allocations(df.index, a)
        frames.append(df)

    # PowerUp: 2026 modelled output + resource for the window, long-term for whole local months (budget shape)
    hours = pd.date_range(t0 + pd.Timedelta(hours=1), t1 + pd.Timedelta(minutes=10), freq="h")
    srcid = p["powerup_srcid"]
    pow_el = 782 if p["tech"] == "wind" else 794          # PowerUp modelled power: wind 782, solar 794
    pu = S.powerup_history(srcid, pow_el, t0.date(), (t1 + pd.Timedelta(days=1)).date(), utc_stamps=False)
    res_el = 26 if p["tech"] == "wind" else 338
    res = S.powerup_history(srcid, res_el, t0.date(), (t1 + pd.Timedelta(days=1)).date(), utc_stamps=True)
    if p["tech"] == "solar":
        # Solar P50 driver = PowerUp irradiance (element 338, J/m2/h -> W/m2), not modelled MW: the custom Holstein
        # farm's modelled history (794) runs ~20% below its 2026 output per unit irradiance (0.21-0.23 vs 0.26
        # MW per W/m2, checked 2026-10-20), while the irradiance series is consistent across years.
        pu = res / 3600.0
    pu_hourly = {"power": pu, "wind": res if p["tech"] == "wind" else pd.Series(dtype=float)}

    cal = dict(C.calibration().get(plant, {}))
    v = (value_wind(plant, frames, ref, allocs, cal, recalibrate, pu_hourly) if p["tech"] == "wind"
         else value_solar(plant, frames, allocs))
    ts, tsv, he, pot, act, meas = v["ts"], v["tsv"], v["he"], v["pot"], v["act"], v["meas"]
    n_units = v["n_units"]

    # calibration k: measured potential / PowerUp modelled output, hours with >= 95% of units measured
    hr = pd.DataFrame({"pot": pot, "meas": meas}, index=he).groupby(level=0).sum()
    if cal.get("powerup_k") is None or recalibrate:
        good = hr[hr["meas"] >= 0.95 * n_units * 6].join(pu.rename("pu"), how="inner").dropna()
        good = good[good["pu"] > 0]
        if len(good) <= 48:
            raise RuntimeError(f"{plant}: only {len(good)} well-measured hours overlap PowerUp - cannot calibrate k")
        k = float((good["pot"] / 6000.0 * n_units * 6 / good["meas"]).sum() / good["pu"].sum())
        cal.update(powerup_k=k, calibrated_on=str(date.today()), window=f"{t0.date()}..{end}")
        C.save_calibration(plant, cal)
    k = cal["powerup_k"]
    sw = v["sw_gap"]
    gap = pu.reindex(he[sw]).values * k * 1000.0 / n_units
    hod = pd.Series(pot[meas]).groupby(ts[meas].hour).mean()
    gap = np.where(np.isnan(gap), hod.reindex(ts[sw].hour).values, gap)
    pot[sw] = np.nan_to_num(gap)
    loss = pot - act

    step, sub, det = v["step"], v["sub"], v["det"]
    nd = v["nd"]
    put_nd = nd
    step = np.where(put_nd, 3, step)
    sub = np.where(put_nd, np.where(sw, "Site-wide comms gap (k x PowerUp)", "Individual unit gap (site median)"), sub)
    det = np.where(put_nd, "", det)
    det = np.where(pd.isna(det) | (det == "None"), "", det)

    # curtailment before availability: share of a down unit's potential the running fleet could not deliver
    run = v["run"]
    act_run = per_stamp(np.where(run, act, 0.0), tsv, "sum")
    pot_run = per_stamp(np.where(run, pot, 0.0), tsv, "sum")
    r = np.clip(np.divide(act_run, pot_run, out=np.zeros_like(act_run), where=pot_run > 0), 0, 1)
    n_c = per_stamp(v["is_curt"].astype(int), tsv, "sum")
    n_s = per_stamp(v["is_stop"].astype(int), tsv, "sum")
    down = np.isin(step, [6, 7]) & meas & ((n_c + n_s) >= max(3, 0.07 * n_units))
    shift = np.where(down, np.clip(pot * (1 - r), 0, np.clip(loss, 0, None)), 0.0)
    shift_step = np.where(n_s > n_c, 5, 4)

    # PPA compensation of curtailment (steps 4, 5)
    comp_rule = p.get("compensation")
    stamps = pd.DatetimeIndex(np.unique(tsv))
    if comp_rule and comp_rule["rule"] == "price_le":
        s5 = S.yes_five_min(t0.date(), (t1 + pd.Timedelta(days=1)).date(), comp_rule["node"])
        start5 = s5.index - pd.Timedelta(minutes=5)
        pmin = s5.groupby(start5.floor("10min")).min().reindex(stamps)
        ph = S.miso_hourly((t0 - pd.Timedelta(days=1)).date(), (t1 + pd.Timedelta(days=1)).date(), comp_rule["node"])
        hp = (ph["RT_LMP"].fillna(ph["DA_LMP"]) if len(ph) and "DA_LMP" in ph else
              (ph["RT_LMP"] if len(ph) and "RT_LMP" in ph else pd.Series(dtype=float)))
        pmin = pmin.fillna(pd.Series(hp.reindex(stamps.floor("h") + pd.Timedelta(hours=1)).values, index=stamps))
        th = comp_rule["threshold"]
        hit = (pmin <= th) | (pmin.shift(1, freq="10min").reindex(stamps) <= th)
        comp = np.where(hit.reindex(ts).fillna(False).values, COMP_YES, COMP_NO)
    else:
        comp = np.full(len(ts), COMP_NONE, dtype=object)
    comp_main = np.where(np.isin(step, [4, 5]), comp, "")

    keys = v["all_"]["object"].values
    iv = pd.DataFrame({"OBJECT_KEY": keys, "HE_UTC": he, "STEP_ORDER": step, "SUBCATEGORY": sub, "DETAIL": det,
                       "COMPENSATION": comp_main, "MWH": (loss - shift) / 6000.0, "TURBINE_HOURS": 1 / 6})
    sh = shift > 0
    iv = pd.concat([iv, pd.DataFrame({
        "OBJECT_KEY": keys[sh], "HE_UTC": he[sh], "STEP_ORDER": shift_step[sh],
        "SUBCATEGORY": "Down units' curtailed share", "DETAIL": sub[sh], "COMPENSATION": comp[sh],
        "MWH": shift[sh] / 6000.0, "TURBINE_HOURS": 0.0})], ignore_index=True)
    fact = iv.groupby(["OBJECT_KEY", "HE_UTC", "STEP_ORDER", "SUBCATEGORY", "DETAIL", "COMPENSATION"],
                      as_index=False)[["MWH", "TURBINE_HOURS"]].sum()
    base = pd.DataFrame({"OBJECT_KEY": keys, "HE_UTC": he, "pot": pot / 6000.0, "act": act / 6000.0,
                         "meas": meas / 6.0}).groupby(["OBJECT_KEY", "HE_UTC"], as_index=False).sum()
    totals = pd.concat([
        base.assign(STEP_ORDER=2, SUBCATEGORY="", DETAIL="", COMPENSATION="", MWH=base["pot"], TURBINE_HOURS=1.0),
        base.assign(STEP_ORDER=9, SUBCATEGORY="", DETAIL="", COMPENSATION="", MWH=base["act"],
                    TURBINE_HOURS=base["meas"])])
    fact = pd.concat([fact, totals[fact.columns]], ignore_index=True)

    # site level: P50 potential (k x PowerUp long-term), resource, budget, Bazefield tie-out
    full_hours = pd.date_range(pd.Timestamp(t0.year, t0.month, 1) - pd.Timedelta(hours=23),
                               pd.Timestamp(t1.year, t1.month, 1) + pd.offsets.MonthBegin(1) + pd.Timedelta(days=1),
                               freq="h")
    lt_full = (long_term(srcid, pow_el, full_hours, utc_stamps=False) if p["tech"] == "wind"
               else long_term(srcid, 338, full_hours, utc_stamps=True) / 3600.0) * k
    lt = lt_full.reindex(hours)
    site_pot = fact[fact["STEP_ORDER"] == 2].groupby("HE_UTC")["MWH"].sum().reindex(hours).fillna(0)
    budget = pd.read_csv(C.REF / "budget_generation_monthly.csv", parse_dates=["MONTH"])
    budget = budget[budget["PLANT_CODE"] == plant].set_index("MONTH")["BUDGET_GROSS_MWH"]
    loc_full = full_hours.tz_localize("UTC").tz_convert(p["tz"]) - pd.Timedelta(hours=1)
    month_key = pd.DatetimeIndex(loc_full.tz_localize(None).to_period("M").to_timestamp())
    shape = lt_full / lt_full.groupby(month_key).transform("sum")
    budget_h = (shape * budget.reindex(month_key).values).reindex(hours)
    res_lt = long_term(srcid, res_el, hours, utc_stamps=True)
    res_act = res.reindex(hours)
    if p["tech"] == "solar":                                  # J/m2 per hour -> W/m2
        res_lt, res_act = res_lt / 3600.0, res_act / 3600.0
    site_pts = (["ActivePowerWTG", "TheoreticalPower"] if p["tech"] == "wind" else ["ActivePowerINV", "TheoreticalPower"])         + ["ActivePowerMeter"]
    sd = bz.series(site["id"], site_pts, t0.date(), end, interval="1h") if site else pd.DataFrame(index=hours[:0])
    sd.index = pd.DatetimeIndex(sd.index) + pd.Timedelta(hours=1)
    sd = sd.reindex(hours)
    # revenue meter (Tenaska settlement / IPL MV90) + Bazefield's check meter as an independent cross-check. The
    # Bazefield meter point only counts on days it tracks plant output (McCrae's went live 2026-10-13; before that
    # it read ~0 / station load): daily meter / unit gross >= 0.5.
    meter = meter_series(plant, p, t0.date(), end).reindex(hours)
    try:
        related = S.related_meter_hourly(p, t0.date(), end).reindex(hours)
    except Exception:
        log.exception("%s related meter fetch failed", plant)
        related = pd.Series(np.nan, index=hours)
    bzm = sd.get("ActivePowerMeter", pd.Series(np.nan, index=hours))
    unit_gross = site_pot * 0 + fact[fact["STEP_ORDER"] == 9].groupby("HE_UTC")["MWH"].sum().reindex(hours).fillna(0)
    day = (hours - pd.Timedelta(hours=6)).date
    ok = (bzm.groupby(day).transform("sum") >= 0.5 * unit_gross.groupby(day).transform("sum")) & (unit_gross.groupby(day).transform("sum") > 0)
    bzm = bzm.where(ok.values)
    al = allocs[(allocs["end"] > t0) & (allocs["start"] < t1 + pd.Timedelta(minutes=10))] if len(allocs) else allocs
    unav = prorate(al[~al["available"] & (al["excluded"] == 0)], hours) if len(al) else np.zeros(len(hours))
    lost_all = prorate(al, hours) if len(al) else np.zeros(len(hours))
    site_df = pd.DataFrame({
        "HE_UTC": hours, "LT_POTENTIAL_MWH": lt.values, "BUDGET_GROSS_MWH": budget_h.values,
        "PU_HUB_WIND_MS": res_act.values if p["tech"] == "wind" else np.nan,
        "PU_HUB_WIND_LT_MS": res_lt.values if p["tech"] == "wind" else np.nan,
        "RESOURCE_ACTUAL": res_act.values, "RESOURCE_LT": res_lt.values,
        "RESOURCE_UNIT": "m/s hub wind" if p["tech"] == "wind" else "W/m2 irradiance",
        "BZ_ACTUAL_MWH": sd.get(site_pts[0], pd.Series(np.nan, index=hours)).values,
        "BZ_THEORETICAL_MWH": sd.get("TheoreticalPower", pd.Series(np.nan, index=hours)).values,
        "BZ_UNAVAILABLE_MWH": unav, "BZ_LOST_ALL_MWH": lost_all,
        "TURBINES_MEASURED": (hr["meas"] / 6).reindex(hours).values, "TURBINES_TOTAL": n_units,
        "RT_LMP": np.nan, "DA_LMP": np.nan, "PRICE_SOURCE": "RT 5-min (Yes Energy)" if comp_rule else "n/a",
        "METER_MWH": meter.values, "METER_SOURCE": (p.get("meter") or {}).get("label", ""),
        "BZ_METER_MWH": bzm.values, "RELATED_METER_MWH": related.values})
    site_rows = pd.concat([
        pd.DataFrame({"OBJECT_KEY": "SITE", "HE_UTC": hours, "STEP_ORDER": 0, "SUBCATEGORY": "", "DETAIL": "",
                      "COMPENSATION": "", "MWH": lt.values, "TURBINE_HOURS": np.nan}),
        pd.DataFrame({"OBJECT_KEY": "SITE", "HE_UTC": hours, "STEP_ORDER": 1, "SUBCATEGORY": "Resource vs long-term",
                      "DETAIL": "", "COMPENSATION": "", "MWH": lt.values - site_pot.values, "TURBINE_HOURS": np.nan})])
    fact = pd.concat([fact, site_rows], ignore_index=True)
    fact["STEP"] = fact["STEP_ORDER"].map(step_names(plant))
    for d in (fact, site_df):
        d.insert(0, "PLANT_CODE", plant)
        d.insert(2, "LOCAL_DATE", (pd.DatetimeIndex(d["HE_UTC"]).tz_localize("UTC").tz_convert(p["tz"])
                                   - pd.Timedelta(hours=1)).date)
    fact = fact[["PLANT_CODE", "HE_UTC", "LOCAL_DATE", "OBJECT_KEY", "STEP_ORDER", "STEP", "SUBCATEGORY", "DETAIL",
                 "COMPENSATION", "MWH", "TURBINE_HOURS"]]
    fact["MWH"] = fact["MWH"].astype(float).round(6)
    fact = fact[fact["MWH"].notna()]
    build_row = {"BUILD_ID": uuid.uuid4().hex[:16].upper(), "PLANT_CODE": plant, "WINDOW_START_UTC": str(t0),
                 "WINDOW_END_UTC": str(t1 + pd.Timedelta(minutes=10)), "BUILT_AT": now.isoformat(timespec="seconds"),
                 "POWERUP_K": k, "STOPPED_ANEMOMETER_FACTOR": cal.get("stopped_anemometer_factor"),
                 "LT_YEARS": f"{LT_YEARS.start}-{LT_YEARS.stop - 1}",
                 "METHOD": "ercot-basis-tracker energy_waterfall/engine.py"}
    log.info("%s: %d fact rows, %d Bazefield calls", plant, len(fact), bz.calls)
    return fact, site_df, build_row
