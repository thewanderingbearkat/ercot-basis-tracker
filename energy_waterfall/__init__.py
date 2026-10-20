"""Energy waterfall (P50 potential -> resource -> losses -> gross) for the Bazefield sites.

engine.py builds the turbine/inverter x hour fact from Bazefield SCADA, GADS allocations, PowerUp and MISO prices;
refresh.py is the weekly Render cron job that rebuilds the trailing window into SkyVest Snowflake GOLD.PERFORMANCE;
web.py is the dashboard tab. Ported from powerup-bronze (analysis/energy_waterfall.py) on 2026-10-20.
"""
