from flask import Flask, jsonify, request, redirect, session
from flask_cors import CORS
from gridstatus import Ercot
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from functools import wraps
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
app.secret_key = os.getenv('SECRET_KEY', 'your-secret-key-change-this')

# Configuration
NODE_1 = "NBOHR_RN"
NODE_2 = "HOLSTEIN_ALL"
HUB = "HB_WEST"
ALERT_THRESHOLD = 100
GREEN_THRESHOLD = -100
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD', 'arclight2024')

# Global state
data_lock = threading.Lock()
latest_data = {
    "node1_price": None,
    "node2_price": None,
    "hub_price": None,
    "basis1": None,  # NODE_1 vs HUB
    "basis2": None,  # NODE_2 vs HUB
    "last_update": None,
    "data_time": None,
    "status1": "initializing",
    "status2": "initializing",
    "history": []
}
last_basis_time = None

# Login decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated_function

# Helper functions
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
            status2 = "safe" if basis2 > 0 else ("caution" if basis2 >= -100 else "alert")
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
    global last_basis_time, latest_data
    
    logger.info("Fetching initial historical data...")
    initial_history = get_historical_prices()
    
    logger.info(f"Got {len(initial_history)} points from get_historical_prices")
    if initial_history:
        logger.info(f"First point: {initial_history[0]}")
        logger.info(f"Last point: {initial_history[-1]}")
    
    with data_lock:
        latest_data["history"] = initial_history
        if initial_history:
            # FIX: Populate the latest price fields from the most recent historical data point
            # This ensures the API returns valid data immediately after startup
            last_point = initial_history[-1]
            latest_data["node1_price"] = last_point['node1_price']
            latest_data["node2_price"] = last_point['node2_price']
            latest_data["hub_price"] = last_point['hub_price']
            latest_data["basis1"] = last_point['basis1']
            latest_data["basis2"] = last_point['basis2']
            latest_data["status1"] = last_point['status1']
            latest_data["status2"] = last_point['status2']
            latest_data["data_time"] = str(last_point['time'])
            latest_data["last_update"] = datetime.now().isoformat()
            last_basis_time = last_point['time']
            logger.info(f"Updated latest_data: node1={latest_data['node1_price']}, basis1={latest_data['basis1']}, history_count={len(latest_data['history'])}")
    
    logger.info(f"Loaded {len(initial_history)} historical data points")
    
    # Signal that initial data is ready
    logger.info("Initial data ready, entering update loop")
    
    while True:
        try:
            ercot = Ercot()
            lmp_data = ercot.get_lmp(date="latest", location_type="settlement point")
            
            if lmp_data is None or len(lmp_data) == 0:
                logger.warning("No real-time data available")
                time.sleep(120)
                continue
            
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
                    status2 = "safe" if basis2 > 0 else ("caution" if basis2 >= -100 else "alert")
                    
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
                    logger.info(f"New point: {NODE_1}=${new_point['node1_price']}, {NODE_2}=${new_point['node2_price']}, {HUB}=${new_point['hub_price']}, Basis1=${new_point['basis1']}, Basis2=${new_point['basis2']}, Total history: {len(latest_data['history'])}")
            
            time.sleep(120)
            
        except Exception as e:
            logger.error(f"Error in background fetch: {e}")
            with data_lock:
                latest_data["status1"] = "error"
                latest_data["status2"] = "error"
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
        # Return current state (may be empty if still loading)
        logger.info(f"API called - data: node1={latest_data['node1_price']}, node2={latest_data['node2_price']}, hub={latest_data['hub_price']}, basis1={latest_data['basis1']}, basis2={latest_data['basis2']}, history_count={len(latest_data['history'])}")
        return jsonify(latest_data)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "data_status1": latest_data["status1"], "data_status2": latest_data["status2"]})

@app.route('/', methods=['GET'])
@login_required
def dashboard():
    return '''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ERCOT Basis Tracker</title>
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

        /* NYT-inspired typography */
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            letter-spacing: -0.01em;
        }

        .nyt-title {
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
            border: 1px solid #e5e5e5;
            transition: box-shadow 0.2s ease;
        }

        .card:hover {
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        }
    </style>
</head>
<body style="background-color: #f8f9fa;">
    <div class="min-h-screen p-6 md:p-12" style="background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);">
        <div class="max-w-7xl mx-auto">
            <!-- Header -->
            <div class="mb-12 pb-6" style="border-bottom: 3px solid var(--skyvest-navy);">
                <div class="flex justify-between items-start">
                    <div>
                        <h1 class="nyt-title text-5xl mb-2" style="color: var(--skyvest-navy);">ERCOT Basis Tracker</h1>
                        <p class="metric-label" style="color: var(--skyvest-blue);">Real-time Settlement Point Analysis</p>
                    </div>
                    <div class="text-right">
                        <div id="connection" class="text-xs font-semibold px-3 py-1 rounded-full" style="background-color: var(--skyvest-light-blue); color: var(--skyvest-navy);">Connecting...</div>
                    </div>
                </div>
            </div>

            <!-- Price Cards -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-10">
                <div class="card rounded-sm p-6">
                    <p class="metric-label mb-3" style="color: #666;">NBOHR_RN</p>
                    <span id="node1" class="text-4xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-6">
                    <p class="metric-label mb-3" style="color: #666;">HOLSTEIN_ALL</p>
                    <span id="node2" class="text-4xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                </div>
                <div class="card rounded-sm p-6" style="background-color: var(--skyvest-navy);">
                    <p class="metric-label mb-3" style="color: var(--skyvest-light-blue);">HB_WEST (Hub)</p>
                    <span id="hub" class="text-4xl font-light text-white">N/A</span>
                </div>
            </div>

            <!-- Basis Cards -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-10">
                <div class="card rounded-sm p-6">
                    <div class="flex justify-between items-start mb-6">
                        <div>
                            <p class="metric-label mb-3" style="color: #666;">NBOHR_RN Basis</p>
                            <span id="basis1" class="text-4xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p class="metric-label mb-2" style="color: #999;">Status</p>
                            <p id="status1" class="text-sm font-bold px-3 py-1 rounded-full" style="background-color: #f0f0f0; color: #666;">N/A</p>
                        </div>
                    </div>
                    <p class="text-xs" style="color: #999;">vs HB_WEST</p>
                </div>

                <div class="card rounded-sm p-6">
                    <div class="flex justify-between items-start mb-6">
                        <div>
                            <p class="metric-label mb-3" style="color: #666;">HOLSTEIN_ALL Basis</p>
                            <span id="basis2" class="text-4xl font-light" style="color: var(--skyvest-navy);">N/A</span>
                        </div>
                        <div class="text-right">
                            <p class="metric-label mb-2" style="color: #999;">Status</p>
                            <p id="status2" class="text-sm font-bold px-3 py-1 rounded-full" style="background-color: #f0f0f0; color: #666;">N/A</p>
                        </div>
                    </div>
                    <p class="text-xs" style="color: #999;">vs HB_WEST</p>
                </div>
            </div>

            <!-- Charts -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-10">
                <div class="card rounded-sm p-6">
                    <h3 class="text-lg font-semibold mb-6" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 8px;">NBOHR_RN Basis Trend</h3>
                    <div id="chart-container-1"></div>
                </div>

                <div class="card rounded-sm p-6">
                    <h3 class="text-lg font-semibold mb-6" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 8px;">HOLSTEIN_ALL Basis Trend</h3>
                    <div id="chart-container-2"></div>
                </div>
            </div>

            <!-- Combined Comparison Chart -->
            <div class="card rounded-sm p-6">
                <h3 class="text-lg font-semibold mb-6" style="color: var(--skyvest-navy); border-bottom: 1px solid #e5e5e5; padding-bottom: 8px;">Basis Comparison</h3>
                <div id="chart-container-combined"></div>
            </div>
        </div>
    </div>
    
    <script>
        const API_URL = window.location.protocol + '//' + window.location.host + '/api/basis';
        
        async function fetchData() {
            try {
                const response = await fetch(API_URL);
                const data = await response.json();
                
                document.getElementById('node1').textContent = data.node1_price ? '$' + data.node1_price.toFixed(2) : 'N/A';
                document.getElementById('node2').textContent = data.node2_price ? '$' + data.node2_price.toFixed(2) : 'N/A';
                document.getElementById('hub').textContent = data.hub_price ? '$' + data.hub_price.toFixed(2) : 'N/A';
                document.getElementById('basis1').textContent = data.basis1 ? '$' + data.basis1.toFixed(2) : 'N/A';
                document.getElementById('basis2').textContent = data.basis2 ? '$' + data.basis2.toFixed(2) : 'N/A';
                
                const status1El = document.getElementById('status1');
                if (data.status1) {
                    status1El.textContent = data.status1.toUpperCase();
                    const style1 = getStatusStyle(data.status1);
                    status1El.style.backgroundColor = style1.bgColor;
                    status1El.style.color = style1.textColor;
                }

                const status2El = document.getElementById('status2');
                if (data.status2) {
                    status2El.textContent = data.status2.toUpperCase();
                    const style2 = getStatusStyle(data.status2);
                    status2El.style.backgroundColor = style2.bgColor;
                    status2El.style.color = style2.textColor;
                }
                
                const connEl = document.getElementById('connection');
                connEl.textContent = 'Connected';
                connEl.style.backgroundColor = '#2291EB';
                connEl.style.color = 'white';

                if (data.history && data.history.length > 0) {
                    renderChart(data.history, 'chart-container-1', 'basis1');
                    renderChart(data.history, 'chart-container-2', 'basis2');
                    renderCombinedChart(data.history);
                }
            } catch (error) {
                console.error('Error:', error);
                const connEl = document.getElementById('connection');
                connEl.textContent = 'Connection Error';
                connEl.style.backgroundColor = '#ef4444';
                connEl.style.color = 'white';
            }
        }

        function getStatusColor(status) {
            if (status === 'safe') return '#2291EB';
            if (status === 'caution') return '#FFD966';
            if (status === 'alert') return '#ef4444';
            return '#999';
        }

        function getStatusStyle(status) {
            const color = getStatusColor(status);
            let bgColor, textColor;

            if (status === 'safe') {
                bgColor = '#e3f2fd';
                textColor = '#0E2C51';
            } else if (status === 'caution') {
                bgColor = '#fff8e1';
                textColor = '#8B6914';
            } else if (status === 'alert') {
                bgColor = '#ffebee';
                textColor = '#c62828';
            } else {
                bgColor = '#f0f0f0';
                textColor = '#666';
            }

            return { bgColor, textColor };
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
            bars.style.minHeight = '200px';
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
        
        function renderCombinedChart(history) {
            const container = document.getElementById('chart-container-combined');
            if (!container) return;
            
            container.innerHTML = '';
            
            const allValues = [...history.map(p => p.basis1), ...history.map(p => p.basis2)];
            const minVal = Math.min(...allValues);
            const maxVal = Math.max(...allValues);
            
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

            const barsContainer = document.createElement('div');
            barsContainer.style.display = 'flex';
            barsContainer.style.alignItems = 'flex-end';
            barsContainer.style.gap = '4px';
            barsContainer.style.borderBottom = '1px solid #e5e5e5';
            barsContainer.style.paddingBottom = '8px';
            barsContainer.style.minHeight = '300px';
            barsContainer.style.position = 'relative';

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
            barsContainer.appendChild(gridContainer);
            
            history.forEach((point, idx) => {
                const pairContainer = document.createElement('div');
                pairContainer.style.flex = '1';
                pairContainer.style.display = 'flex';
                pairContainer.style.gap = '1px';
                pairContainer.style.alignItems = 'flex-end';
                pairContainer.style.height = '100%';
                
                const time = new Date(point.time);
                const timeStr = time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
                
                // Bar for basis1 (NBOHR_RN)
                const heightPercent1 = ((point.basis1 - yMin) / yRange) * 100;
                const color1 = getStatusColorHex(point.status1);
                const bar1 = document.createElement('div');
                bar1.style.flex = '1';
                bar1.style.height = Math.max(heightPercent1, 5) + '%';
                bar1.style.backgroundColor = color1;
                bar1.style.opacity = '0.85';
                bar1.style.borderRadius = '2px 2px 0 0';
                bar1.style.minHeight = '5px';
                bar1.style.position = 'relative';
                bar1.style.transition = 'opacity 0.2s';
                bar1.title = timeStr + ' - NBOHR: $' + point.basis1.toFixed(2);
                bar1.addEventListener('mouseenter', () => bar1.style.opacity = '1');
                bar1.addEventListener('mouseleave', () => bar1.style.opacity = '0.85');

                // Bar for basis2 (HOLSTEIN_ALL)
                const heightPercent2 = ((point.basis2 - yMin) / yRange) * 100;
                const color2 = getStatusColorHex(point.status2);
                const bar2 = document.createElement('div');
                bar2.style.flex = '1';
                bar2.style.height = Math.max(heightPercent2, 5) + '%';
                bar2.style.backgroundColor = color2;
                bar2.style.opacity = '0.7';
                bar2.style.borderRadius = '2px 2px 0 0';
                bar2.style.minHeight = '5px';
                bar2.style.position = 'relative';
                bar2.style.transition = 'opacity 0.2s';
                bar2.title = timeStr + ' - HOLSTEIN: $' + point.basis2.toFixed(2);
                bar2.addEventListener('mouseenter', () => bar2.style.opacity = '1');
                bar2.addEventListener('mouseleave', () => bar2.style.opacity = '0.7');
                
                pairContainer.appendChild(bar1);
                pairContainer.appendChild(bar2);
                barsContainer.appendChild(pairContainer);
            });
            
            chart.appendChild(yAxis);
            chart.appendChild(barsContainer);
            
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
            
            // Add legend
            const legend = document.createElement('div');
            legend.style.display = 'flex';
            legend.style.gap = '24px';
            legend.style.marginTop = '16px';
            legend.style.justifyContent = 'center';
            legend.style.fontSize = '11px';
            legend.style.paddingTop = '16px';
            legend.style.borderTop = '1px solid #e5e5e5';

            const legend1 = document.createElement('div');
            legend1.style.display = 'flex';
            legend1.style.alignItems = 'center';
            legend1.style.gap = '8px';
            const box1 = document.createElement('div');
            box1.style.width = '20px';
            box1.style.height = '12px';
            box1.style.background = 'linear-gradient(to right, #2291EB 0%, #FFD966 50%, #ef4444 100%)';
            box1.style.opacity = '0.85';
            box1.style.borderRadius = '1px';
            const text1 = document.createElement('span');
            text1.textContent = 'NBOHR_RN (left bar)';
            text1.style.color = '#666';
            text1.style.fontWeight = '500';
            legend1.appendChild(box1);
            legend1.appendChild(text1);

            const legend2 = document.createElement('div');
            legend2.style.display = 'flex';
            legend2.style.alignItems = 'center';
            legend2.style.gap = '8px';
            const box2 = document.createElement('div');
            box2.style.width = '20px';
            box2.style.height = '12px';
            box2.style.background = 'linear-gradient(to right, #2291EB 0%, #FFD966 50%, #ef4444 100%)';
            box2.style.opacity = '0.7';
            box2.style.borderRadius = '1px';
            const text2 = document.createElement('span');
            text2.textContent = 'HOLSTEIN_ALL (right bar)';
            text2.style.color = '#666';
            text2.style.fontWeight = '500';
            legend2.appendChild(box2);
            legend2.appendChild(text2);

            legend.appendChild(legend1);
            legend.appendChild(legend2);
            
            chartWrapper.appendChild(legend);
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