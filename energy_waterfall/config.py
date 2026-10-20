"""Energy waterfall: per-plant configuration.

Every plant is valued interval by interval (10-minute Bazefield SCADA) and each interval is assigned to one waterfall
step by the Bazefield GADS / PV_GADS allocation covering it, then split by SCADA signature. See engine.py.
Bazefield facts used here are documented in the bazefield skill (docs.bazefield.com, support KB) and in
powerup-bronze's references; the reference curves are rebuilt from each site's Bazefield TheoreticalPower.
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REF = HERE / "reference"

# Waterfall steps (positive MWh = energy lost; P50 potential - losses = gross, every hour).
STEP_NAMES = {0: "P50 potential", 1: "Resource", 2: "Potential at actual resource", 3: "Unmeasured (no SCADA data)",
              4: "Curtailment", 5: "External stops", 6: "Availability (PBA)", 7: "Other non-PBA downtime",
              8: "Performance", 9: "Gross (measured)"}

GE_PITCH_FAULT_CODES = {144, 344, 274, 141, 359, 164}

PLANTS = {
    "LFW01": {
        "name": "Lakefield", "tech": "wind", "tz": "America/Chicago", "units": 137, "rated_kw": 1500.0,
        "capacity_mw": 205.5, "elevation_m": 456.0, "curve": "ge15_lfw01_curve.json",
        "powerup_srcid": "POWERUP_WF_ID_21911", "data_start": "2026-09-15", "allocation_type": "GADS",
        # GE 1.5: curtailment shows as OEM setpoints (logged on change) + alarms 221 / 58 (skyvest-notes.md)
        "points": ["WindSpeed", "ActivePower", "Blade1_PitchAngle", "Blade2_PitchAngle", "Blade3_PitchAngle",
                   "WindDirection", "NacelleDirection", "AmbientTemperature", "Running", "Available",
                   "ExternalCurtailmentSetpoint.10m", "UPE_WFMSCurtailmentSetpoint.10m"],
        "curtail_setpoints": ["ExternalCurtailmentSetpoint.10m", "UPE_WFMSCurtailmentSetpoint.10m"],
        "alarm_codes": {"curtail": {221, 58}, "temp_derate": {277}, "stall": {371}, "pitch_fault": GE_PITCH_FAULT_CODES,
                        "info": {153, 146, 218}},
        "curtail_label": "Curtailment (MISO dispatch)",
        # IPL PPA (2009-06-22) s.3.9(c): IPL economic curtailment is paid; IPL's DIR offer is $0.00/MWh
        # (powerup-bronze analysis/dir_threshold.py). A 10-min interval is presumed compensated when any 5-minute
        # ALTW.LKFLD.IPL LMP in it or the previous 10 minutes is <= $0.
        "compensation": {"rule": "price_le", "node": "ALTW.LKFLD.IPL", "threshold": 0.0},
    },
    "MCW02": {
        "name": "McCrae", "tech": "wind", "tz": "America/Chicago", "units": 47, "rated_kw": 3450.0,
        "capacity_mw": 162.15, "elevation_m": 823.0, "curve": "v126_mcw02_curve.json",
        "powerup_srcid": "POWERUP_WF_ID_21718", "data_start": "2026-08-03", "allocation_type": "GADS",
        # Vestas V126: curtailment = SetpointActivePower below rated (3,450 kW when unconstrained)
        "points": ["WindSpeed", "ActivePower", "Blade1_PitchAngle", "Blade2_PitchAngle", "Blade3_PitchAngle",
                   "WindDirection", "NacelleDirection", "AmbientTemperature", "Running", "Available",
                   "SetpointActivePower"],
        "setpoint_point": "SetpointActivePower",
        # The V126 curve Bazefield applies at McCrae already reflects site air density (823 m): its TheoreticalPower
        # (110.4 GWh Aug 3 - Oct 19) sits 8% above measured gross, while re-normalising wind to ISO density put the
        # potential BELOW gross. So no second density correction here (Lakefield, 456 m, keeps IEC normalisation).
        "density_correct": False,
        "alarm_codes": {},
        "curtail_label": "Curtailment (ERCOT dispatch)",
        "compensation": None,                      # Tyler 2026-10-20: McCrae curtailment is not compensated
    },
    "HOS01": {
        "name": "Holstein", "tech": "solar", "tz": "America/Chicago", "units": 66, "rated_kw": 3225.0,
        "capacity_mw": 200.0, "powerup_srcid": "POWERUP_SF_ID_29569", "data_start": "2026-09-14",
        "allocation_type": "PV_GADS",
        "points": ["ActivePower", "TheoreticalPower", "Available", "Running", "Irradiance"],
        "curtail_label": "Curtailment",
        "compensation": None,
    },
    "NOW01": {
        "name": "Northwest Ohio", "tech": "wind", "tz": "America/New_York", "units": 42, "rated_kw": 2500.0,
        "capacity_mw": 105.0, "elevation_m": 223.0, "curve": None,          # built once enabled (TheoreticalPower)
        "powerup_srcid": "POWERUP_WF_ID_29565", "data_start": "2026-10-15", "allocation_type": "GADS",
        "points": ["WindSpeed", "ActivePower", "Blade1_PitchAngle", "Blade2_PitchAngle", "Blade3_PitchAngle",
                   "WindDirection", "NacelleDirection", "AmbientTemperature", "Running", "Available"],
        "alarm_codes": {},
        "curtail_label": "Curtailment (PJM / grid)",
        "compensation": None,
        # Bazefield onboarding mid-Oct 2026: switch on automatically once there are this many days of data
        "enable_after_days": 28,
    },
}


def enabled_plants(today) -> list[str]:
    """Plants to build: all with a reference (curve or solar), plus NOW01 once it has enough history."""
    from datetime import date
    out = []
    for code, p in PLANTS.items():
        if p["tech"] == "wind" and not p.get("curve"):
            continue
        need = p.get("enable_after_days")
        if need and (today - date.fromisoformat(p["data_start"])).days < need:
            continue
        out.append(code)
    return out


def reference_curve(plant: str):
    import numpy as np
    d = json.loads((REF / PLANTS[plant]["curve"]).read_text())
    c = np.array(d["curve"], dtype=float)
    cut = float(d["cut_out_ms"])

    def f(ws):
        ws = np.asarray(ws, dtype=float)
        out = np.interp(ws, c[:, 0], c[:, 1], left=0.0, right=0.0)
        return np.where(ws > cut, 0.0, out)
    return f


def calibration() -> dict:
    f = REF / "calibration.json"
    return json.loads(f.read_text()) if f.exists() else {}


def save_calibration(plant: str, values: dict):
    cal = calibration()
    cal.setdefault(plant, {}).update(values)
    (REF / "calibration.json").write_text(json.dumps(cal, indent=1, default=str))
