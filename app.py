from flask import Flask, jsonify, request, redirect, session
from flask_cors import CORS
from gridstatus import Ercot
import requests
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from functools import wraps
import logging
import os
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-change-this')

# Configuration - ERCOT
NODE_1 = "NBOHR_RN"
NODE_2 = "HOLSTEIN_ALL"
HUB = "HB_WEST"

# Configuration - PJM
PJM_NODE = "HAVILAND34.5 KV NTHWSTWF"
PJM_HUB = "AEP-DAYTON HUB"
PJM_NODE_ID = 1318144721
PJM_HUB_ID = 34497127

# Thresholds
ALERT_THRESHOLD = 100
GREEN_THRESHOLD = -100
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD', 'SV2026!!!')

# Storage file for PJM historical data
PJM_HISTORY_FILE = 'pjm_history.json'

# Global state
data_lock = threading.Lock()
latest_data = {
    # ERCOT data
    "node1_price": None,
    "node2_price": None,
    "hub_price": None,
    "basis1": None,  # NODE_1 vs HUB
    "basis2": None,  # NODE_2 vs HUB
    "status1": "initializing",
    "status2": "initializing",
    "history": [],

    # PJM data
    "pjm_node_price": None,
    "pjm_hub_price": None,
    "pjm_basis": None,  # PJM_NODE vs PJM_HUB
    "pjm_status": "initializing",
    "pjm_history": [],

    # Metadata
    "last_update": None,
    "data_time": None,
}
last_basis_time = None
last_pjm_time = None

# Login decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated_function

# PJM History Storage Functions
def save_pjm_history(history):
    """Save PJM history to JSON file."""
    try:
        with open(PJM_HISTORY_FILE, 'w') as f:
            json.dump(history, f)
        logger.info(f"Saved {len(history)} PJM historical points to {PJM_HISTORY_FILE}")
    except Exception as e:
        logger.error(f"Error saving PJM history: {e}")

def load_pjm_history():
    """Load PJM history from JSON file."""
    try:
        if os.path.exists(PJM_HISTORY_FILE):
            with open(PJM_HISTORY_FILE, 'r') as f:
                history = json.load(f)
            logger.info(f"Loaded {len(history)} PJM historical points from {PJM_HISTORY_FILE}")
            return history
        else:
            logger.info(f"No existing PJM history file found at {PJM_HISTORY_FILE}")
            return []
    except Exception as e:
        logger.error(f"Error loading PJM history: {e}")
        return []

# PJM API Helper Functions
def get_pjm_subscription_key():
    """Get the public PJM API subscription key."""
    try:
        response = requests.get("http://dataminer2.pjm.com/config/settings.json", timeout=10)
        settings = response.json()
        return settings.get("subscriptionKey")
    except Exception as e:
        logger.error(f"Error getting PJM subscription key: {e}")
        return None

def get_pjm_lmp_data(hours_back=4):
    """Fetch PJM LMP data from the unverified 5-minute feed."""
    try:
        # Get subscription key
        key = get_pjm_subscription_key()
        if not key:
            logger.error("Could not get PJM subscription key")
            return []

        headers = {"Ocp-Apim-Subscription-Key": key}

        # Find the unverified 5-minute feed
        response = requests.get("https://api.pjm.com/api/v1/", headers=headers, timeout=10)
        feeds = response.json()

        feed_url = None
        for item in feeds.get('items', []):
            if item.get('name') == 'rt_unverified_fivemin_lmps':
                feed_url = item['links'][0]['href']
                break

        if not feed_url:
            logger.error("Could not find PJM unverified 5-minute LMP feed")
            return []

        # Fetch data from the last N hours
        from datetime import timezone as tz
        now = datetime.now(tz.utc)
        start_time = (now - timedelta(hours=hours_back)).strftime('%Y-%m-%dT%H:%M:%S')
        end_time = now.strftime('%Y-%m-%dT%H:%M:%S')

        params = {
            'datetime_beginning_utc': f'{start_time} to {end_time}',
            'startRow': 1,
            'rowCount': 10000
        }

        response = requests.get(feed_url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            logger.error(f"PJM API returned status {response.status_code}")
            return []

        data = response.json()
        all_items = data.get('items', [])

        if not all_items:
            logger.warning("No PJM data available")
            return []

        # Filter for our specific nodes
        node_items = [item for item in all_items if item.get('pnode_id') == PJM_NODE_ID]
        hub_items = [item for item in all_items if item.get('pnode_id') == PJM_HUB_ID]

        # Create merged data
        history = []
        for node_item in node_items:
            time_utc = node_item.get('datetime_beginning_utc')
            # Find matching hub item
            hub_item = next((h for h in hub_items if h.get('datetime_beginning_utc') == time_utc), None)

            if hub_item:
                node_lmp = node_item.get('total_lmp_rt', 0)
                hub_lmp = hub_item.get('total_lmp_rt', 0)
                basis = node_lmp - hub_lmp
                status = "safe" if basis > 0 else ("caution" if basis >= -30 else "alert")

                history.append({
                    'time': time_utc,
                    'node_price': round(float(node_lmp), 2),
                    'hub_price': round(float(hub_lmp), 2),
                    'basis': round(float(basis), 2),
                    'status': status
                })

        # Sort by time
        history.sort(key=lambda x: x['time'])

        logger.info(f"Fetched {len(history)} PJM historical data points")
        return history

    except Exception as e:
        logger.error(f"Error fetching PJM data: {e}")
        return []

# ERCOT Helper Functions
def get_historical_prices(hours_back=4):
    try:
        cst_tz = ZoneInfo("US/Central")
        today_cst = datetime.now(cst_tz).date()
        
        logger.info(f"Fetching ERCOT data for {today_cst}")
        ercot = Ercot()
        lmp_data = ercot.get_lmp(date=str(today_cst), location_type="settlement point")
        
        if lmp_data is None or len(lmp_data) == 0:
            logger.warning(f"No LMP data available for {today_cst}")
            return []
        
        node1_data = lmp_data[lmp_data['Location'] == NODE_1].copy()
        node2_data = lmp_data[lmp_data['Location'] == NODE_2].copy()
        hub_data = lmp_data[lmp_data['Location'] == HUB].copy()
        
        if len(node1_data) == 0 or len(node2_data) == 0 or len(hub_data) == 0:
            logger.warning(f"No data found for nodes {NODE_1}, {NODE_2}, or {HUB}")
            return []
        
        merged = node1_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_1_LMP'}).merge(
            node2_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_2_LMP'}),
            on='Interval Start'
        ).merge(
            hub_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'HUB_LMP'}),
            on='Interval Start'
        )
        
        merged['BASIS_1'] = merged['NODE_1_LMP'] - merged['HUB_LMP']
        merged['BASIS_2'] = merged['NODE_2_LMP'] - merged['HUB_LMP']
        merged = merged.sort_values('Interval Start')
        
        cutoff_time = datetime.now(cst_tz) - __import__('datetime').timedelta(hours=hours_back)
        merged['Interval Start'] = __import__('pandas').to_datetime(merged['Interval Start'])
        merged = merged[merged['Interval Start'] >= cutoff_time]
        
        logger.info(f"Fetched {len(merged)} historical data points for {NODE_1}, {NODE_2} vs {HUB}")
        
        history = []
        for _, row in merged.iterrows():
            basis1 = row['BASIS_1']
            basis2 = row['BASIS_2']
            status1 = "safe" if basis1 > 0 else ("caution" if basis1 >= -100 else "alert")
            status2 = "safe" if basis2 > 0 else ("caution" if basis2 >= -30 else "alert")
            history.append({
                'time': row['Interval Start'],
                'node1_price': round(float(row['NODE_1_LMP']), 2),
                'node2_price': round(float(row['NODE_2_LMP']), 2),
                'hub_price': round(float(row['HUB_LMP']), 2),
                'basis1': round(float(basis1), 2),
                'basis2': round(float(basis2), 2),
                'status1': status1,
                'status2': status2
            })
        
        return history
        
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return []

def background_data_fetch():
    global last_basis_time, last_pjm_time, latest_data

    # Fetch initial ERCOT data
    logger.info("Fetching initial ERCOT historical data...")
    initial_history = get_historical_prices()

    logger.info(f"Got {len(initial_history)} ERCOT points")
    if initial_history:
        logger.info(f"First ERCOT point: {initial_history[0]}")
        logger.info(f"Last ERCOT point: {initial_history[-1]}")

    # Load existing PJM data from file
    logger.info("Loading existing PJM historical data from file...")
    stored_pjm_history = load_pjm_history()

    # Fetch fresh PJM data from API
    logger.info("Fetching fresh PJM historical data from API...")
    fresh_pjm_history = get_pjm_lmp_data()

    # Merge stored and fresh data, avoiding duplicates
    if stored_pjm_history:
        # Create set of existing timestamps
        existing_times = {point['time'] for point in stored_pjm_history}
        # Add only new points from fresh data
        for point in fresh_pjm_history:
            if point['time'] not in existing_times:
                stored_pjm_history.append(point)
        # Sort by time
        stored_pjm_history.sort(key=lambda x: x['time'])
        # Keep last 2000 points (about 7 days of 5-min data)
        stored_pjm_history = stored_pjm_history[-2000:]
        initial_pjm_history = stored_pjm_history
        logger.info(f"Merged PJM data: {len(stored_pjm_history)} total points")
    else:
        initial_pjm_history = fresh_pjm_history
        logger.info(f"No stored history, using {len(fresh_pjm_history)} fresh points")

    # Save merged history
    if initial_pjm_history:
        save_pjm_history(initial_pjm_history)
        logger.info(f"First PJM point: {initial_pjm_history[0]}")
        logger.info(f"Last PJM point: {initial_pjm_history[-1]}")

    with data_lock:
        # Update ERCOT data
        latest_data["history"] = initial_history
        if initial_history:
            last_point = initial_history[-1]
            latest_data["node1_price"] = last_point['node1_price']
            latest_data["node2_price"] = last_point['node2_price']
            latest_data["hub_price"] = last_point['hub_price']
            latest_data["basis1"] = last_point['basis1']
            latest_data["basis2"] = last_point['basis2']
            latest_data["status1"] = last_point['status1']
            latest_data["status2"] = last_point['status2']
            latest_data["data_time"] = str(last_point['time'])
            last_basis_time = last_point['time']
            logger.info(f"Updated ERCOT latest_data: node1=${latest_data['node1_price']}, basis1=${latest_data['basis1']}")

        # Update PJM data
        latest_data["pjm_history"] = initial_pjm_history
        if initial_pjm_history:
            last_pjm_point = initial_pjm_history[-1]
            latest_data["pjm_node_price"] = last_pjm_point['node_price']
            latest_data["pjm_hub_price"] = last_pjm_point['hub_price']
            latest_data["pjm_basis"] = last_pjm_point['basis']
            latest_data["pjm_status"] = last_pjm_point['status']
            last_pjm_time = last_pjm_point['time']
            logger.info(f"Updated PJM latest_data: node=${latest_data['pjm_node_price']}, basis=${latest_data['pjm_basis']}")

        latest_data["last_update"] = datetime.now().isoformat()

    logger.info(f"Loaded {len(initial_history)} ERCOT + {len(initial_pjm_history)} PJM historical data points")

    # Signal that initial data is ready
    logger.info("Initial data ready, entering update loop")
    
    while True:
        try:
            # Fetch ERCOT data
            ercot = Ercot()
            lmp_data = ercot.get_lmp(date="latest", location_type="settlement point")

            if lmp_data is not None and len(lmp_data) > 0:
                latest_time = lmp_data['Interval Start'].max()

                if latest_time != last_basis_time:
                    latest_data_df = lmp_data[lmp_data['Interval Start'] == latest_time]

                    node1_data = latest_data_df[latest_data_df['Location'] == NODE_1]
                    node2_data = latest_data_df[latest_data_df['Location'] == NODE_2]
                    hub_data = latest_data_df[latest_data_df['Location'] == HUB]

                    if len(node1_data) > 0 and len(node2_data) > 0 and len(hub_data) > 0:
                        node1_price = float(node1_data['LMP'].values[0])
                        node2_price = float(node2_data['LMP'].values[0])
                        hub_price = float(hub_data['LMP'].values[0])
                        basis1 = node1_price - hub_price
                        basis2 = node2_price - hub_price
                        status1 = "safe" if basis1 > 0 else ("caution" if basis1 >= -100 else "alert")
                        status2 = "safe" if basis2 > 0 else ("caution" if basis2 >= -30 else "alert")

                        new_point = {
                            'time': latest_time,
                            'node1_price': round(node1_price, 2),
                            'node2_price': round(node2_price, 2),
                            'hub_price': round(hub_price, 2),
                            'basis1': round(basis1, 2),
                            'basis2': round(basis2, 2),
                            'status1': status1,
                            'status2': status2
                        }

                        with data_lock:
                            latest_data["node1_price"] = new_point['node1_price']
                            latest_data["node2_price"] = new_point['node2_price']
                            latest_data["hub_price"] = new_point['hub_price']
                            latest_data["basis1"] = new_point['basis1']
                            latest_data["basis2"] = new_point['basis2']
                            latest_data["last_update"] = datetime.now().isoformat()
                            latest_data["data_time"] = str(latest_time)
                            latest_data["status1"] = status1
                            latest_data["status2"] = status2
                            latest_data["history"].append(new_point)
                            latest_data["history"] = latest_data["history"][-100:]

                        last_basis_time = latest_time
                        logger.info(f"ERCOT update: {NODE_1}=${new_point['node1_price']}, {NODE_2}=${new_point['node2_price']}, Basis1=${new_point['basis1']}, Basis2=${new_point['basis2']}")
            else:
                logger.warning("No ERCOT real-time data available")

            # Fetch PJM data (fetches last 1 hour to get latest)
            pjm_data = get_pjm_lmp_data(hours_back=1)
            if pjm_data and len(pjm_data) > 0:
                # Get the most recent PJM point
                latest_pjm_point = pjm_data[-1]
                latest_pjm_time = latest_pjm_point['time']

                if latest_pjm_time != last_pjm_time:
                    with data_lock:
                        latest_data["pjm_node_price"] = latest_pjm_point['node_price']
                        latest_data["pjm_hub_price"] = latest_pjm_point['hub_price']
                        latest_data["pjm_basis"] = latest_pjm_point['basis']
                        latest_data["pjm_status"] = latest_pjm_point['status']
                        latest_data["pjm_history"].append(latest_pjm_point)
                        # Keep last 2000 points (about 7 days of 5-min data)
                        latest_data["pjm_history"] = latest_data["pjm_history"][-2000:]
                        latest_data["last_update"] = datetime.now().isoformat()

                        # Save updated history to file
                        save_pjm_history(latest_data["pjm_history"])

                    last_pjm_time = latest_pjm_time
                    logger.info(f"PJM update: {PJM_NODE}=${latest_pjm_point['node_price']}, {PJM_HUB}=${latest_pjm_point['hub_price']}, Basis=${latest_pjm_point['basis']}")
            else:
                logger.warning("No PJM data available")

            time.sleep(120)

        except Exception as e:
            logger.error(f"Error in background fetch: {e}")
            # Keep last known good data instead of setting status to error
            time.sleep(60)

# Flag to track if background thread has started in this process
_background_thread_started = False

def start_background_thread_if_needed():
    """Start the background thread if not already running in this process."""
    global _background_thread_started
    if not _background_thread_started:
        _background_thread_started = True
        fetch_thread = threading.Thread(target=background_data_fetch, daemon=True)
        fetch_thread.start()
        logger.info(f"Background data fetch thread started in process {os.getpid()}")

# Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == DASHBOARD_PASSWORD:
            session['authenticated'] = True
            return redirect('/')
        else:
            return '''<html><body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); color: #0E2C51; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
                <div style="background: white; padding: 48px; border-radius: 2px; border: 1px solid #e5e5e5; box-shadow: 0 2px 8px rgba(0,0,0,0.08); max-width: 400px; width: 100%;">
                    <h1 style="margin-top: 0; font-family: Georgia, serif; color: #0E2C51; font-weight: 700; letter-spacing: -0.02em;">ERCOT Basis Tracker</h1>
                    <p style="color: #ef4444; margin-bottom: 24px; font-size: 14px; background: #ffebee; padding: 12px; border-radius: 2px; border-left: 3px solid #ef4444;">Invalid password. Try again.</p>
                    <form method="post"><input type="password" name="password" placeholder="Enter password" autofocus style="padding: 12px; border: 1px solid #d0d0d0; border-radius: 2px; background: #ffffff; color: #0E2C51; width: 100%; box-sizing: border-box; margin-bottom: 12px; font-size: 14px;">
                    <button type="submit" style="padding: 12px; background: #2291EB; color: white; border: none; border-radius: 2px; cursor: pointer; width: 100%; font-weight: 600; font-size: 14px; transition: background 0.2s;" onmouseover="this.style.background='#0E2C51'" onmouseout="this.style.background='#2291EB'">Login</button></form>
                </div></body></html>'''

    return '''<html><body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%); color: #0E2C51; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
        <div style="background: white; padding: 48px; border-radius: 2px; border: 1px solid #e5e5e5; box-shadow: 0 2px 8px rgba(0,0,0,0.08); max-width: 400px; width: 100%;">
            <h1 style="margin-top: 0; font-family: Georgia, serif; color: #0E2C51; font-weight: 700; letter-spacing: -0.02em;">ERCOT Basis Tracker</h1>
            <p style="color: #666; margin-bottom: 24px; font-size: 14px;">Enter the password to access the dashboard</p>
            <form method="post"><input type="password" name="password" placeholder="Enter password" autofocus style="padding: 12px; border: 1px solid #d0d0d0; border-radius: 2px; background: #ffffff; color: #0E2C51; width: 100%; box-sizing: border-box; margin-bottom: 12px; font-size: 14px;">
            <button type="submit" style="padding: 12px; background: #2291EB; color: white; border: none; border-radius: 2px; cursor: pointer; width: 100%; font-weight: 600; font-size: 14px; transition: background 0.2s;" onmouseover="this.style.background='#0E2C51'" onmouseout="this.style.background='#2291EB'">Login</button></form>
        </div></body></html>'''

@app.route('/api/basis', methods=['GET'])
@login_required
def get_basis():
    # Ensure background thread is running in this worker process
    start_background_thread_if_needed()

    with data_lock:
        # Return current state (includes both ERCOT and PJM data)
        logger.info(f"API called - ERCOT: node1=${latest_data['node1_price']}, basis1=${latest_data['basis1']}, history={len(latest_data['history'])} | PJM: node=${latest_data['pjm_node_price']}, basis=${latest_data['pjm_basis']}, history={len(latest_data['pjm_history'])}")
        return jsonify(latest_data)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "ercot_status1": latest_data["status1"],
        "ercot_status2": latest_data["status2"],
        "pjm_status": latest_data["pjm_status"]
    })

@app.route('/', methods=['GET'])
@login_required
def dashboard():
    return '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ERCOT + PJM Basis Tracker</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        /* SkyVest Color Palette */
        :root {
            --skyvest-navy: #0E2C51;
            --skyvest-light-blue: #A7D3F7;
            --skyvest-gray: #D9D9D9;
            --skyvest-gold: #FFD966;
            --skyvest-blue: #2291EB;
            --skyvest-navy-light: #1a4370;
        }

        /* Operational Dashboard Typography */
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            letter-spacing: -0.01em;
        }

        .dashboard-title {
            font-family: Georgia, "Times New Roman", serif;
            font-weight: 700;
            letter-spacing: -0.02em;
        }

        .metric-label {
            font-size: 0.6875rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 600;
        }

        .card {
            background: white;
            border: 2px solid #e5e5e5;
            transition: all 0.3s ease;
        }

        .card:hover {
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        }

        /* Basis card status styling */
        .basis-card {
            transition: background-color 0.3s ease, border-color 0.3s ease, box-shadow 0.3s ease;
        }

        /* Alert pulse animation */
        @keyframes alertPulse {
            0%, 100% {
                box-shadow: 0 0 20px rgba(239, 68, 68, 0.5), 0 4px 12px rgba(0, 0, 0, 0.1);
            }
            50% {
                box-shadow: 0 0 30px rgba(239, 68, 68, 0.8), 0 4px 16px rgba(0, 0, 0, 0.15);
            }
        }

        .alert-pulse {
            animation: alertPulse 2s ease-in-out infinite;
        }
    </style>
</head>
<body style="background-color: #f8f9fa;">
    <div class="p-3 md:p-4" style="background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);">
        <div class="max-w-7xl mx-auto">
            <!-- Header -->
            <div class="mb-3 pb-2" style="border-bottom: 2px solid var(--skyvest-navy);">
                <div class="flex justify-between items-start">
                    <div>
                        <h1 class="dashboard-title text-2xl mb-1" style="color: var(--skyvest-navy);">ERCOT + PJM Basis Tracker</h1>
                        <p class="metric-label text-xs" style="color: var(--skyvest-blue);">Real-time Multi-Market Basis Analysis</p>
                    </div>
                    <div class="text-right">
                        <div id="connection" class="text-xs font-semibold px-2 py-1 rounded-full" style="background-color: var(--skyvest-light-blue); color: var(--skyvest-navy);">Connecting...</div>
                    </div>
                </div>
            </div>

            <!-- ERCOT Section -->
            <div class="mb-2">
                <h2 class="text-lg font-bold mb-2" style="color: var(--skyvest-navy); border-left: 3px solid var(--skyvest-blue); padding-left: 8px;">ERCOT</h2>
            </div>

            <!-- ERCOT Price Cards -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">NBOHR_RN</p>
                    <span id="node1" class="text-2xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">HOLSTEIN_ALL</p>
                    <span id="node2" class="text-2xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-3" style="background-color: var(--skyvest-navy);">
                    <p class="metric-label mb-1" style="color: var(--skyvest-light-blue);">HB_WEST (Hub)</p>
                    <span id="hub" class="text-2xl font-light text-white">N/A</span>
                </div>
            </div>

            <!-- ERCOT Basis Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mb-3">
                <div id="basis-card-1" class="card basis-card rounded-sm p-3">
                    <div class="flex justify-between items-start mb-2">
                        <div>
                            <p id="basis1-label" class="metric-label mb-1" style="color: #666;">NBOHR_RN Basis</p>
                            <span id="basis1" class="text-3xl font-bold" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p id="status1-label" class="metric-label mb-1" style="color: #999;">Status</p>
                            <p id="status1" class="text-xl font-bold" style="color: #666;">N/A</p>
                        </div>
                    </div>
                    <p id="basis1-subtitle" class="text-xs" style="color: #999;">vs HB_WEST</p>
                </div>

                <div id="basis-card-2" class="card basis-card rounded-sm p-3">
                    <div class="flex justify-between items-start mb-2">
                        <div>
                            <p id="basis2-label" class="metric-label mb-1" style="color: #666;">HOLSTEIN_ALL Basis</p>
                            <span id="basis2" class="text-3xl font-bold" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p id="status2-label" class="metric-label mb-1" style="color: #999;">Status</p>
                            <p id="status2" class="text-xl font-bold" style="color: #666;">N/A</p>
                        </div>
                    </div>
                    <p id="basis2-subtitle" class="text-xs" style="color: #999;">vs HB_WEST</p>
                </div>
            </div>

            <!-- ERCOT Charts -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">NBOHR_RN Basis Trend</h3>
                    <div id="chart-container-1"></div>
                </div>

                <div class="card rounded-sm p-3">
                    <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">HOLSTEIN_ALL Basis Trend</h3>
                    <div id="chart-container-2"></div>
                </div>
            </div>

            <!-- PJM Section -->
            <div class="mb-2">
                <h2 class="text-lg font-bold mb-2" style="color: var(--skyvest-navy); border-left: 3px solid var(--skyvest-blue); padding-left: 8px;">PJM</h2>
            </div>

            <!-- PJM Price Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <p class="metric-label mb-1" style="color: #666;">HAVILAND34.5 KV NTHWSTWF</p>
                    <span id="pjm-node" class="text-2xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-3" style="background-color: var(--skyvest-navy);">
                    <p class="metric-label mb-1" style="color: var(--skyvest-light-blue);">AEP-DAYTON HUB</p>
                    <span id="pjm-hub" class="text-2xl font-light text-white">N/A</span>
                </div>
            </div>

            <!-- PJM Basis Card -->
            <div class="grid grid-cols-1 gap-2 mb-3">
                <div id="pjm-basis-card" class="card basis-card rounded-sm p-3">
                    <div class="flex justify-between items-start mb-2">
                        <div>
                            <p id="pjm-basis-label" class="metric-label mb-1" style="color: #666;">HAVILAND Basis</p>
                            <span id="pjm-basis" class="text-3xl font-bold" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p id="pjm-status-label" class="metric-label mb-1" style="color: #999;">Status</p>
                            <p id="pjm-status" class="text-xl font-bold" style="color: #666;">N/A</p>
                        </div>
                    </div>
                    <p id="pjm-basis-subtitle" class="text-xs" style="color: #999;">vs AEP-DAYTON HUB</p>
                </div>
            </div>

            <!-- PJM Chart -->
            <div class="grid grid-cols-1 gap-2 mb-3">
                <div class="card rounded-sm p-3">
                    <h3 class="text-sm font-semibold mb-2" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 4px;">HAVILAND Basis Trend</h3>
                    <div id="pjm-chart-container"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const API_URL = window.location.protocol + '//' + window.location.host + '/api/basis';
        
        async function fetchData() {
            try {
                const response = await fetch(API_URL);
                const data = await response.json();
                
                // Update ERCOT data
                document.getElementById('node1').textContent = data.node1_price ? '$' + data.node1_price.toFixed(2) : 'N/A';
                document.getElementById('node2').textContent = data.node2_price ? '$' + data.node2_price.toFixed(2) : 'N/A';
                document.getElementById('hub').textContent = data.hub_price ? '$' + data.hub_price.toFixed(2) : 'N/A';
                document.getElementById('basis1').textContent = data.basis1 ? '$' + data.basis1.toFixed(2) : 'N/A';
                document.getElementById('basis2').textContent = data.basis2 ? '$' + data.basis2.toFixed(2) : 'N/A';

                // Update PJM data
                document.getElementById('pjm-node').textContent = data.pjm_node_price ? '$' + data.pjm_node_price.toFixed(2) : 'N/A';
                document.getElementById('pjm-hub').textContent = data.pjm_hub_price ? '$' + data.pjm_hub_price.toFixed(2) : 'N/A';
                document.getElementById('pjm-basis').textContent = data.pjm_basis ? '$' + data.pjm_basis.toFixed(2) : 'N/A';

                // Apply status styling to ERCOT basis card 1
                if (data.status1) {
                    const card1 = document.getElementById('basis-card-1');
                    const style1 = getCardStyle(data.status1);
                    applyCardStyling(card1, style1, 'basis1', 'status1', data.status1);
                }

                // Apply status styling to ERCOT basis card 2
                if (data.status2) {
                    const card2 = document.getElementById('basis-card-2');
                    const style2 = getCardStyle(data.status2);
                    applyCardStyling(card2, style2, 'basis2', 'status2', data.status2);
                }

                // Apply status styling to PJM basis card
                if (data.pjm_status) {
                    const pjmCard = document.getElementById('pjm-basis-card');
                    const pjmStyle = getCardStyle(data.pjm_status);
                    applyCardStyling(pjmCard, pjmStyle, 'pjm-basis', 'pjm-status', data.pjm_status);
                }
                
                const connEl = document.getElementById('connection');
                connEl.textContent = 'Connected';
                connEl.style.backgroundColor = '#2291EB';
                connEl.style.color = 'white';

                // Render ERCOT charts
                if (data.history && data.history.length > 0) {
                    renderChart(data.history, 'chart-container-1', 'basis1');
                    renderChart(data.history, 'chart-container-2', 'basis2');
                }

                // Render PJM chart
                if (data.pjm_history && data.pjm_history.length > 0) {
                    renderPJMChart(data.pjm_history, 'pjm-chart-container');
                }
            } catch (error) {
                console.error('Error:', error);
                const connEl = document.getElementById('connection');
                connEl.textContent = 'Connection Error';
                connEl.style.backgroundColor = '#ef4444';
                connEl.style.color = 'white';
            }
        }

        // Get full card styling based on status
        function getCardStyle(status) {
            if (status === 'safe') {
                return {
                    bgColor: '#2291EB',      // SkyVest blue
                    textColor: 'white',
                    labelColor: 'rgba(255, 255, 255, 0.9)',
                    subtitleColor: 'rgba(255, 255, 255, 0.8)',
                    statusColor: 'white',
                    borderColor: '#1a7bc4',
                    glow: false
                };
            } else if (status === 'caution') {
                return {
                    bgColor: '#FFD966',      // SkyVest gold
                    textColor: '#0E2C51',    // Navy for contrast
                    labelColor: '#5a4a1f',
                    subtitleColor: '#6b5a2f',
                    statusColor: '#8B6914',  // Darker gold
                    borderColor: '#e6c44d',
                    glow: false
                };
            } else if (status === 'alert') {
                return {
                    bgColor: '#ef4444',      // Red alert
                    textColor: 'white',
                    labelColor: 'rgba(255, 255, 255, 0.95)',
                    subtitleColor: 'rgba(255, 255, 255, 0.85)',
                    statusColor: 'white',
                    borderColor: '#dc2626',   // Darker red
                    glow: true                // Enable pulse animation
                };
            } else {
                return {
                    bgColor: 'white',
                    textColor: 'var(--skyvest-navy)',
                    labelColor: '#666',
                    subtitleColor: '#999',
                    statusColor: '#666',
                    borderColor: '#e5e5e5',
                    glow: false
                };
            }
        }

        // Apply styling to a basis card
        function applyCardStyling(card, style, basisId, statusId, statusText) {
            // Card background and border
            card.style.backgroundColor = style.bgColor;
            card.style.borderColor = style.borderColor;

            // Apply or remove glow effect
            if (style.glow) {
                card.classList.add('alert-pulse');
                card.style.boxShadow = '0 0 20px rgba(239, 68, 68, 0.5)';
            } else {
                card.classList.remove('alert-pulse');
                card.style.boxShadow = '';
            }

            // Update all text elements
            const basisEl = document.getElementById(basisId);
            const statusEl = document.getElementById(statusId);
            const labelEl = document.getElementById(basisId + '-label');
            const subtitleEl = document.getElementById(basisId + '-subtitle');
            const statusLabelEl = document.getElementById(statusId + '-label');

            if (basisEl) basisEl.style.color = style.textColor;
            if (statusEl) {
                statusEl.textContent = statusText.toUpperCase();
                statusEl.style.color = style.statusColor;
            }
            if (labelEl) labelEl.style.color = style.labelColor;
            if (subtitleEl) subtitleEl.style.color = style.subtitleColor;
            if (statusLabelEl) statusLabelEl.style.color = style.labelColor;
        }

        function getStatusColor(status) {
            if (status === 'safe') return '#2291EB';
            if (status === 'caution') return '#FFD966';
            if (status === 'alert') return '#ef4444';
            return '#999';
        }

        function getStatusColorHex(status) {
            if (status === 'safe') return '#2291EB';
            if (status === 'caution') return '#FFD966';
            if (status === 'alert') return '#ef4444';
            return '#999';
        }
        
        function renderChart(history, containerId, basisField) {
            const container = document.getElementById(containerId);
            if (!container) return;
            
            container.innerHTML = '';
            
            const values = history.map(p => p[basisField]);
            const minVal = Math.min(...values);
            const maxVal = Math.max(...values);
            
            // Auto-scale based purely on actual data range with small padding
            const padding = Math.max(5, (maxVal - minVal) * 0.1); // 10% padding or minimum $5
            const yMin = Math.floor((minVal - padding) / 5) * 5;
            const yMax = Math.ceil((maxVal + padding) / 5) * 5;
            const yRange = yMax - yMin || 10;
            
            // Determine step size based on range
            let step = 5;
            if (yRange > 100) step = 20;
            else if (yRange > 50) step = 10;
            
            const chartWrapper = document.createElement('div');
            chartWrapper.style.display = 'flex';
            chartWrapper.style.flexDirection = 'column';
            chartWrapper.style.gap = '4px';
            
            const chart = document.createElement('div');
            chart.style.display = 'grid';
            chart.style.gridTemplateColumns = '60px 1fr';
            chart.style.gap = '8px';
            
            const yAxis = document.createElement('div');
            yAxis.style.display = 'flex';
            yAxis.style.flexDirection = 'column';
            yAxis.style.justifyContent = 'space-between';
            yAxis.style.textAlign = 'right';
            yAxis.style.paddingRight = '12px';
            yAxis.style.fontSize = '11px';
            yAxis.style.color = '#999';
            yAxis.style.fontWeight = '400';
            yAxis.style.borderRight = '1px solid #e5e5e5';

            for (let i = yMax; i >= yMin; i -= step) {
                const label = document.createElement('div');
                label.textContent = '$' + i;
                label.style.padding = '4px 0';
                yAxis.appendChild(label);
            }

            const bars = document.createElement('div');
            bars.style.display = 'flex';
            bars.style.alignItems = 'flex-end';
            bars.style.gap = '2px';
            bars.style.borderBottom = '1px solid #e5e5e5';
            bars.style.paddingBottom = '8px';
            bars.style.minHeight = '120px';
            bars.style.position = 'relative';

            // Add horizontal gridlines
            const gridContainer = document.createElement('div');
            gridContainer.style.position = 'absolute';
            gridContainer.style.width = '100%';
            gridContainer.style.height = '100%';
            gridContainer.style.pointerEvents = 'none';

            for (let i = yMax; i >= yMin; i -= step) {
                const gridLine = document.createElement('div');
                const position = ((i - yMin) / yRange) * 100;
                gridLine.style.position = 'absolute';
                gridLine.style.bottom = position + '%';
                gridLine.style.width = '100%';
                gridLine.style.height = '1px';
                gridLine.style.backgroundColor = '#f5f5f5';
                gridContainer.appendChild(gridLine);
            }
            bars.appendChild(gridContainer);

            history.forEach((point, idx) => {
                const basisValue = point[basisField];
                const heightPercent = ((basisValue - yMin) / yRange) * 100;
                const statusField = basisField === 'basis1' ? 'status1' : 'status2';
                const color = getStatusColorHex(point[statusField]);

                const time = new Date(point.time);
                const timeStr = time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

                const bar = document.createElement('div');
                bar.style.flex = '1';
                bar.style.height = Math.max(heightPercent, 5) + '%';
                bar.style.backgroundColor = color;
                bar.style.opacity = '0.85';
                bar.style.borderRadius = '2px 2px 0 0';
                bar.style.minHeight = '5px';
                bar.style.position = 'relative';
                bar.style.transition = 'opacity 0.2s';
                bar.title = timeStr + ': $' + basisValue.toFixed(2);
                bar.addEventListener('mouseenter', () => bar.style.opacity = '1');
                bar.addEventListener('mouseleave', () => bar.style.opacity = '0.85');
                bars.appendChild(bar);
            });
            
            chart.appendChild(yAxis);
            chart.appendChild(bars);
            
            // Add time labels
            const timeLabels = document.createElement('div');
            timeLabels.style.display = 'grid';
            timeLabels.style.gridTemplateColumns = '60px 1fr';
            timeLabels.style.gap = '8px';
            timeLabels.style.marginTop = '4px';
            
            const spacer = document.createElement('div');
            
            const timeContainer = document.createElement('div');
            timeContainer.style.display = 'flex';
            timeContainer.style.justifyContent = 'space-between';
            timeContainer.style.fontSize = '10px';
            timeContainer.style.color = '#999';
            timeContainer.style.paddingLeft = '4px';
            timeContainer.style.paddingRight = '4px';
            timeContainer.style.fontWeight = '400';
            timeContainer.style.textTransform = 'uppercase';
            timeContainer.style.letterSpacing = '0.05em';

            if (history.length > 0) {
                const firstTime = new Date(history[0].time);
                const lastTime = new Date(history[history.length - 1].time);

                const startLabel = document.createElement('span');
                startLabel.textContent = firstTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

                const endLabel = document.createElement('span');
                endLabel.textContent = lastTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

                timeContainer.appendChild(startLabel);
                timeContainer.appendChild(endLabel);
            }
            
            timeLabels.appendChild(spacer);
            timeLabels.appendChild(timeContainer);
            
            chartWrapper.appendChild(chart);
            chartWrapper.appendChild(timeLabels);
            container.appendChild(chartWrapper);
        }

        function renderPJMChart(history, containerId) {
            const container = document.getElementById(containerId);
            if (!container) return;

            container.innerHTML = '';

            if (!history || history.length === 0) {
                container.innerHTML = '<p style="color: #999; text-align: center; padding: 40px;">No data available</p>';
                return;
            }

            const chartWrapper = document.createElement('div');

            // Calculate Y-axis range
            const basisValues = history.map(p => p.basis);
            const minBasis = Math.min(...basisValues);
            const maxBasis = Math.max(...basisValues);
            const padding = Math.max(10, (maxBasis - minBasis) * 0.1);
            const yMin = Math.floor((minBasis - padding) / 10) * 10;
            const yMax = Math.ceil((maxBasis + padding) / 10) * 10;
            const yRange = yMax - yMin;
            const step = Math.max(10, Math.ceil(yRange / 5 / 10) * 10);

            const chart = document.createElement('div');
            chart.style.display = 'grid';
            chart.style.gridTemplateColumns = '60px 1fr';
            chart.style.gap = '8px';

            const yAxis = document.createElement('div');
            yAxis.style.display = 'flex';
            yAxis.style.flexDirection = 'column';
            yAxis.style.justifyContent = 'space-between';
            yAxis.style.textAlign = 'right';
            yAxis.style.paddingRight = '12px';
            yAxis.style.fontSize = '11px';
            yAxis.style.color = '#999';
            yAxis.style.fontWeight = '400';
            yAxis.style.borderRight = '1px solid #e5e5e5';

            for (let i = yMax; i >= yMin; i -= step) {
                const label = document.createElement('div');
                label.textContent = '$' + i;
                label.style.padding = '4px 0';
                yAxis.appendChild(label);
            }

            const bars = document.createElement('div');
            bars.style.display = 'flex';
            bars.style.alignItems = 'flex-end';
            bars.style.gap = '2px';
            bars.style.borderBottom = '1px solid #e5e5e5';
            bars.style.paddingBottom = '8px';
            bars.style.minHeight = '120px';
            bars.style.position = 'relative';

            // Add horizontal gridlines
            const gridContainer = document.createElement('div');
            gridContainer.style.position = 'absolute';
            gridContainer.style.width = '100%';
            gridContainer.style.height = '100%';
            gridContainer.style.pointerEvents = 'none';

            for (let i = yMax; i >= yMin; i -= step) {
                const gridLine = document.createElement('div');
                const position = ((i - yMin) / yRange) * 100;
                gridLine.style.position = 'absolute';
                gridLine.style.bottom = position + '%';
                gridLine.style.width = '100%';
                gridLine.style.height = '1px';
                gridLine.style.backgroundColor = '#f5f5f5';
                gridContainer.appendChild(gridLine);
            }
            bars.appendChild(gridContainer);

            history.forEach((point, idx) => {
                const basisValue = point.basis;
                const heightPercent = ((basisValue - yMin) / yRange) * 100;
                const color = getStatusColorHex(point.status);

                const time = new Date(point.time);
                const timeStr = time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

                const bar = document.createElement('div');
                bar.style.flex = '1';
                bar.style.height = Math.max(heightPercent, 5) + '%';
                bar.style.backgroundColor = color;
                bar.style.opacity = '0.85';
                bar.style.borderRadius = '2px 2px 0 0';
                bar.style.minHeight = '5px';
                bar.style.position = 'relative';
                bar.style.transition = 'opacity 0.2s';
                bar.title = timeStr + ': $' + basisValue.toFixed(2);
                bar.addEventListener('mouseenter', () => bar.style.opacity = '1');
                bar.addEventListener('mouseleave', () => bar.style.opacity = '0.85');
                bars.appendChild(bar);
            });

            chart.appendChild(yAxis);
            chart.appendChild(bars);

            // Add time labels
            const timeLabels = document.createElement('div');
            timeLabels.style.display = 'grid';
            timeLabels.style.gridTemplateColumns = '60px 1fr';
            timeLabels.style.gap = '8px';
            timeLabels.style.marginTop = '4px';

            const spacer = document.createElement('div');

            const timeContainer = document.createElement('div');
            timeContainer.style.display = 'flex';
            timeContainer.style.justifyContent = 'space-between';
            timeContainer.style.fontSize = '10px';
            timeContainer.style.color = '#999';
            timeContainer.style.paddingLeft = '4px';
            timeContainer.style.paddingRight = '4px';
            timeContainer.style.fontWeight = '400';
            timeContainer.style.textTransform = 'uppercase';
            timeContainer.style.letterSpacing = '0.05em';

            if (history.length > 0) {
                const firstTime = new Date(history[0].time);
                const lastTime = new Date(history[history.length - 1].time);

                const startLabel = document.createElement('span');
                startLabel.textContent = firstTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

                const endLabel = document.createElement('span');
                endLabel.textContent = lastTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });

                timeContainer.appendChild(startLabel);
                timeContainer.appendChild(endLabel);
            }

            timeLabels.appendChild(spacer);
            timeLabels.appendChild(timeContainer);

            chartWrapper.appendChild(chart);
            chartWrapper.appendChild(timeLabels);
            container.appendChild(chartWrapper);
        }

        fetchData();
        setInterval(fetchData, 30000);
    </script>
</body>
</html>'''

if __name__ == '__main__':
    # Only start background thread here for local development
    start_background_thread_if_needed()
    app.run(debug=False, host='0.0.0.0', port=int(os.getenv('PORT', 5000)))