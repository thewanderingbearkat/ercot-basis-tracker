"""Hail monitor for Holstein Solar -- a standalone tab backed by the Vaisala
XWeather hail API. Independent of the ERCOT basis-tracker / shadow-trader logic;
it only shares the host Flask app (see app.py register_blueprint) and the site
coordinates."""
