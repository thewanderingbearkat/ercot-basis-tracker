"""Thin client for the Vaisala PowerUp API (site-level production forecasts).

This is a SECOND, separate API from the Xweather data API (data.api.xweather.com):
  - Base URL:  https://app.powerup.xweather.com/PowerupAPI/
  - Auth:      UserGUID / UserSecret HTTP headers, from the environment:
                   VAISALA_POWERUP_USER_GUID
                   VAISALA_POWERUP_USER_SECRET
  - Spec:      data/wx_trial/powerup_swagger.json (openapi), official SDK
               'XweatherPowerupAPI' on their private pip index (dev reference only --
               deliberately NOT a dependency of this repo).

Forecast flow: PowerUp issues ensemble forecasts twice daily (00Z / 12Z), two
providers: id 15 = 51-member ensemble, id 17 = deterministic single member.
GetList -> pick newest -> GetProductionForecast per farm srcid. The 51 members
give a real predictive distribution: we reduce to mean + percentiles per hour,
which feeds bid sizing far better than a point forecast.

Element IDs (from the official SDK enums -- integers, not discoverable via API):
  history:  782 = HourlyWindPowerModelled, 119 = HourlyWindPowerMetered,
            7 = HourlyWindSpeedForWindFarms, 794/778 = solar modelled/metered
  forecast: 784 = HourlyWindPowerModelled, 796 = HourlySolarPowerModelled

Farm srcids (their database, verified 2026-08-31; specs match the real plants):
  BKI  -> POWERUP_WF_ID_21813 (196.65 MW, V126/3450 x57, hub 87m)
  BKII -> POWERUP_WF_ID_21718 (162.15 MW, V126/3450 x47, hub 87m)
  HOLSTEIN -> pending (farm created in the app 2026-08-31, awaiting approval)

NOTE: measDates timezone is assumed UTC (forecast issues are 00Z/12Z). Verify
against the diurnal shape of the first real data before trusting hour alignment.
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from statistics import mean, quantiles
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://app.powerup.xweather.com/PowerupAPI/"
_CT = ZoneInfo("America/Chicago")

FARM_IDS = {
    "BKI": "POWERUP_WF_ID_21813",
    "BKII": "POWERUP_WF_ID_21718",
    "HOLSTEIN": None,  # fill once Vaisala approves the farm created 2026-08-31
}

ELEM_FCST = {"wind": 784, "solar": 796}
ELEM_HIST_MODELLED = {"wind": 782, "solar": 794}
ELEM_HIST_METERED = {"wind": 119, "solar": 778}

PROVIDER_ENSEMBLE = 15   # 51 members
PROVIDER_DETERMINISTIC = 17


class PowerUpError(RuntimeError):
    """Auth/config problems worth surfacing loudly."""


def _headers() -> dict:
    guid = os.getenv("VAISALA_POWERUP_USER_GUID")
    secret = os.getenv("VAISALA_POWERUP_USER_SECRET")
    if not guid or not secret:
        raise PowerUpError("VAISALA_POWERUP_USER_GUID / _SECRET not set in environment")
    return {"UserGUID": guid, "UserSecret": secret, "Content-Type": "application/json"}


def _sdt(d: datetime) -> dict:
    """Their SafeSerializationDateTime JSON shape."""
    return {"year": d.year, "month": d.month, "day": d.day,
            "hour": d.hour, "minute": d.minute, "second": 0, "hasBeenSet": True}


def _from_sdt(d: dict) -> datetime | None:
    if not d or not d.get("hasBeenSet", True):
        return None
    try:
        return datetime(d["year"], d["month"], d["day"], d.get("hour", 0),
                        d.get("minute", 0), tzinfo=timezone.utc)
    except (KeyError, ValueError):
        return None


def _hour_key(dt_utc: datetime) -> str:
    ct = dt_utc.astimezone(_CT)
    return f"{ct:%Y-%m-%d} HE{ct.hour + 1:02d}"


def _unwrap(resp: requests.Response, what: str):
    if resp.status_code == 401:
        raise PowerUpError(f"PowerUp 401 on {what} (credentials or entitlement)")
    if resp.status_code != 200:
        logger.warning("PowerUp HTTP %s on %s: %s", resp.status_code, what, resp.text[:200])
        return None
    body = resp.json()
    hdr = body.get("header") or {}
    if hdr.get("hasError"):
        logger.warning("PowerUp error on %s: %s", what, hdr.get("error"))
        return None
    return body


def list_forecasts(days_back: int = 3) -> list[dict]:
    """Available ensemble forecasts, oldest->newest. Each has idEnsembleForecast,
    idEnsembleProvider, numberOfMembers, forecastIssueDate (parsed to 'issued' dt)."""
    req = {
        "header": {},
        "forecastIssueMinDate": _sdt(datetime.now(timezone.utc) - timedelta(days=days_back)),
        "forecastIssueMaxDate": _sdt(datetime.now(timezone.utc) + timedelta(days=1)),
    }
    resp = requests.post(BASE_URL + "Download/Forecast/GetList", headers=_headers(),
                         json=req, timeout=60)
    body = _unwrap(resp, "Forecast/GetList")
    out = []
    for fc in (body or {}).get("forecasts") or []:
        issued = _from_sdt(fc.get("forecastIssueDate") or {})
        if issued:
            fc["issued"] = issued
            out.append(fc)
    out.sort(key=lambda f: f["issued"])
    return out


def newest_forecast(provider: int = PROVIDER_ENSEMBLE) -> dict | None:
    fcs = [f for f in list_forecasts() if f.get("idEnsembleProvider") == provider]
    return fcs[-1] if fcs else None


def farm_production_forecast(srcid: str, id_forecast: int, tech: str) -> dict:
    """Ensemble production forecast for one farm -> {hour_key: {mean, p10, p25, p50,
    p75, p90, n_members}}. Empty dict if the farm has no data (e.g. not yet entitled)."""
    req = {"header": {}, "srcid": srcid, "idForecast": id_forecast,
           "idRequiredElement": ELEM_FCST[tech]}
    resp = requests.post(BASE_URL + "Download/Farm/GetProductionForecast",
                         headers=_headers(), json=req, timeout=120)
    body = _unwrap(resp, f"GetProductionForecast({srcid})")
    fc = (body or {}).get("forecast") or {}
    dates = fc.get("measDates") or []
    vals = fc.get("measValues") or []
    out = {}
    for i, d in enumerate(dates):
        dt = _from_sdt(d) if isinstance(d, dict) else None
        if dt is None or i >= len(vals):
            continue
        row = vals[i]
        members = [float(v) for v in (row if isinstance(row, list) else [row])
                   if v is not None]
        if not members:
            continue
        rec = {"mean": round(mean(members), 2), "n_members": len(members)}
        if len(members) >= 5:
            q = quantiles(sorted(members), n=20)  # 5% steps
            rec.update(p10=round(q[1], 2), p25=round(q[4], 2), p50=round(q[9], 2),
                       p75=round(q[14], 2), p90=round(q[17], 2))
        out[_hour_key(dt)] = rec
    return out


def farm_history(srcid: str, tech: str, start_date: str, end_date: str,
                 metered: bool = False) -> dict:
    """Historical hourly farm data -> {hour_key: value}. Modelled by default."""
    elem = (ELEM_HIST_METERED if metered else ELEM_HIST_MODELLED)[tech]
    resp = requests.get(
        BASE_URL + f"Download/history/{srcid}/{elem}/{start_date}/{end_date}",
        headers=_headers(), timeout=60)
    body = _unwrap(resp, f"history({srcid}, elem={elem})")
    data = (body or {}).get("data") or {}
    dates = data.get("measDates") or []
    vals = data.get("measValues") or []
    out = {}
    for i, d in enumerate(dates):
        dt = _from_sdt(d) if isinstance(d, dict) else None
        if dt is not None and i < len(vals) and vals[i] is not None:
            out[_hour_key(dt)] = float(vals[i])
    return out


def fetch_asset_ensembles(asset_tech: dict[str, str]) -> dict:
    """For {asset: tech}: newest ensemble forecast per farm.
    Returns {asset: {'issued': iso, 'id_forecast': int, 'hours': {...}}} ({} per
    asset on any failure -- callers treat missing as 'no data', never crash)."""
    out = {a: {} for a in asset_tech}
    try:
        fc = newest_forecast()
    except PowerUpError as e:
        logger.warning("%s", e)
        return out
    if not fc:
        logger.warning("PowerUp: no ensemble forecasts available")
        return out
    for asset, tech in asset_tech.items():
        srcid = FARM_IDS.get(asset)
        if not srcid:
            continue
        try:
            hours = farm_production_forecast(srcid, fc["idEnsembleForecast"], tech)
        except (PowerUpError, requests.RequestException) as e:
            logger.warning("PowerUp forecast failed for %s: %s", asset, e)
            continue
        if hours:
            out[asset] = {"issued": fc["issued"].isoformat(),
                          "id_forecast": fc["idEnsembleForecast"],
                          "n_members": fc.get("numberOfMembers"),
                          "hours": hours}
        else:
            logger.info("PowerUp: no forecast data for %s (%s) -- not yet entitled?",
                        asset, srcid)
    return out
