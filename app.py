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
NODE_2 = "HB_WEST"
ALERT_THRESHOLD = 100
GREEN_THRESHOLD = -100
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD', 'arclight2024')

# Global state
data_lock = threading.Lock()
latest_data = {
    "node1_price": None,
    "node2_price": None,
    "basis": None,
    "last_update": None,
    "data_time": None,
    "status": "initializing",
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
        
        if len(node1_data) == 0 or len(node2_data) == 0:
            logger.warning(f"No data found for nodes {NODE_1} or {NODE_2}")
            return []
        
        merged = node1_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_1_LMP'}).merge(
            node2_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_2_LMP'}),
            on='Interval Start'
        )
        
        merged['BASIS'] = merged['NODE_1_LMP'] - merged['NODE_2_LMP']
        merged = merged.sort_values('Interval Start')
        
        cutoff_time = datetime.now(cst_tz) - __import__('datetime').timedelta(hours=hours_back)
        merged['Interval Start'] = __import__('pandas').to_datetime(merged['Interval Start'])
        merged = merged[merged['Interval Start'] >= cutoff_time]
        
        logger.info(f"Fetched {len(merged)} historical data points for {NODE_1} vs {NODE_2}")
        
        history = []
        for _, row in merged.iterrows():
            basis = row['BASIS']
            status = "safe" if basis > 0 else ("caution" if basis >= -100 else "alert")
            history.append({
                'time': row['Interval Start'],
                'node1_price': round(float(row['NODE_1_LMP']), 2),
                'node2_price': round(float(row['NODE_2_LMP']), 2),
                'basis': round(float(basis), 2),
                'status': status
            })
        
        return history
        
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return []

def background_data_fetch():
    global last_basis_time
    
    logger.info("Fetching initial historical data...")
    initial_history = get_historical_prices()
    
    with data_lock:
        latest_data["history"] = initial_history
        if initial_history:
            last_basis_time = initial_history[-1]['time']
    
    logger.info(f"Loaded {len(initial_history)} historical data points")
    
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
                
                if len(node1_data) > 0 and len(node2_data) > 0:
                    node1_price = float(node1_data['LMP'].values[0])
                    node2_price = float(node2_data['LMP'].values[0])
                    basis = node1_price - node2_price
                    status = "safe" if basis > 0 else ("caution" if basis >= -100 else "alert")
                    
                    new_point = {
                        'time': latest_time,
                        'node1_price': round(node1_price, 2),
                        'node2_price': round(node2_price, 2),
                        'basis': round(basis, 2),
                        'status': status
                    }
                    
                    with data_lock:
                        latest_data["node1_price"] = new_point['node1_price']
                        latest_data["node2_price"] = new_point['node2_price']
                        latest_data["basis"] = new_point['basis']
                        latest_data["last_update"] = datetime.now().isoformat()
                        latest_data["data_time"] = str(latest_time)
                        latest_data["status"] = status
                        latest_data["history"].append(new_point)
                        latest_data["history"] = latest_data["history"][-100:]
                    
                    last_basis_time = latest_time
                    logger.info(f"New point: {NODE_1}=${new_point['node1_price']}, {NODE_2}=${new_point['node2_price']}, Basis=${new_point['basis']}, Status={status}, Total history: {len(latest_data['history'])}")
            
            time.sleep(120)
            
        except Exception as e:
            logger.error(f"Error in background fetch: {e}")
            with data_lock:
                latest_data["status"] = "error"
            time.sleep(60)

# Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password', '')
        if password == DASHBOARD_PASSWORD:
            session['authenticated'] = True
            return redirect('/')
        else:
            return '''<html><body style="font-family: Arial; background: #1f2937; color: white; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
                <div style="background: #374151; padding: 40px; border-radius: 8px; border: 1px solid #4b5563; max-width: 400px; width: 100%;">
                    <h1 style="margin-top: 0;">ERCOT Basis Tracker</h1>
                    <p style="color: #ef4444; margin-bottom: 20px;">Invalid password. Try again.</p>
                    <form method="post"><input type="password" name="password" placeholder="Enter password" autofocus style="padding: 10px; border: 1px solid #6b7280; border-radius: 4px; background: #1f2937; color: white; width: 100%; box-sizing: border-box; margin-bottom: 10px;">
                    <button type="submit" style="padding: 10px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer; width: 100%;">Login</button></form>
                </div></body></html>'''
    
    return '''<html><body style="font-family: Arial; background: #1f2937; color: white; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0;">
        <div style="background: #374151; padding: 40px; border-radius: 8px; border: 1px solid #4b5563; max-width: 400px; width: 100%;">
            <h1 style="margin-top: 0;">ERCOT Basis Tracker</h1>
            <p style="color: #9ca3af; margin-bottom: 20px;">Enter the password to access</p>
            <form method="post"><input type="password" name="password" placeholder="Enter password" autofocus style="padding: 10px; border: 1px solid #6b7280; border-radius: 4px; background: #1f2937; color: white; width: 100%; box-sizing: border-box; margin-bottom: 10px;">
            <button type="submit" style="padding: 10px; background: #3b82f6; color: white; border: none; border-radius: 4px; cursor: pointer; width: 100%;">Login</button></form>
        </div></body></html>'''

@app.route('/api/basis', methods=['GET'])
@login_required
def get_basis():
    with data_lock:
        logger.info(f"API called - data: node1={latest_data['node1_price']}, node2={latest_data['node2_price']}, basis={latest_data['basis']}, history_count={len(latest_data['history'])}")
        return jsonify(latest_data)

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "data_status": latest_data["status"]})

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
</head>
<body>
    <div class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-8">
        <div class="max-w-6xl mx-auto">
            <div class="flex justify-between items-center mb-8">
                <h1 class="text-4xl font-bold text-white">ERCOT Basis Tracker</h1>
                <div id="connection" class="text-blue-400 font-semibold">Connecting...</div>
            </div>
            
            <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                <div class="bg-slate-700 rounded-lg p-6 border border-slate-600">
                    <p class="text-slate-400 text-sm font-medium mb-2">NBOHR_RN</p>
                    <span id="node1" class="text-3xl font-bold text-white">N/A</span>
                </div>
                <div class="bg-slate-700 rounded-lg p-6 border border-slate-600">
                    <p class="text-slate-400 text-sm font-medium mb-2">HB_WEST</p>
                    <span id="node2" class="text-3xl font-bold text-white">N/A</span>
                </div>
                <div class="bg-slate-700 rounded-lg p-6 border border-slate-600">
                    <p class="text-slate-400 text-sm font-medium mb-2">Basis</p>
                    <span id="basis" class="text-3xl font-bold text-white">N/A</span>
                </div>
            </div>
            
            <div class="bg-slate-700 rounded-lg p-6 border border-slate-600 mb-8">
                <h3 class="text-white font-semibold mb-4">Status</h3>
                <p id="status" class="text-2xl font-bold text-yellow-400">N/A</p>
            </div>
            
            <div class="bg-slate-700 rounded-lg p-6 border border-slate-600">
                <h3 class="text-white font-semibold mb-4">Basis Trend</h3>
                <div id="chart-container"></div>
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
                document.getElementById('basis').textContent = data.basis ? '$' + data.basis.toFixed(2) : 'N/A';
                document.getElementById('status').textContent = data.status ? data.status.toUpperCase() : 'N/A';
                document.getElementById('connection').textContent = 'Connected';
                document.getElementById('connection').className = 'text-green-400';
                
                if (data.history && data.history.length > 0) {
                    renderChart(data.history);
                }
            } catch (error) {
                console.error('Error:', error);
                document.getElementById('connection').textContent = 'Connection Error';
                document.getElementById('connection').className = 'text-red-400';
            }
        }
        
        function renderChart(history) {
            const container = document.getElementById('chart-container');
            if (!container) return;
            
            container.innerHTML = '';
            
            const values = history.map(p => p.basis);
            const minVal = Math.min(...values);
            const maxVal = Math.max(...values);
            const yMin = Math.floor(minVal / 5) * 5;
            const yMax = Math.ceil(maxVal / 5) * 5;
            const yRange = yMax - yMin || 1;
            
            const chart = document.createElement('div');
            chart.style.display = 'grid';
            chart.style.gridTemplateColumns = '60px 1fr';
            chart.style.gap = '8px';
            
            const yAxis = document.createElement('div');
            yAxis.style.display = 'flex';
            yAxis.style.flexDirection = 'column';
            yAxis.style.justifyContent = 'space-between';
            yAxis.style.textAlign = 'right';
            yAxis.style.paddingRight = '8px';
            yAxis.style.fontSize = '12px';
            yAxis.style.color = '#9ca3af';
            yAxis.style.borderRight = '2px solid #6b7280';
            
            for (let i = yMax; i >= yMin; i -= 5) {
                const label = document.createElement('div');
                label.textContent = '$' + i;
                yAxis.appendChild(label);
            }
            
            const bars = document.createElement('div');
            bars.style.display = 'flex';
            bars.style.alignItems = 'flex-end';
            bars.style.gap = '4px';
            bars.style.borderBottom = '2px solid #6b7280';
            bars.style.paddingBottom = '8px';
            bars.style.minHeight = '200px';
            
            history.forEach((point, idx) => {
                const heightPercent = ((point.basis - yMin) / yRange) * 100;
                let color = '#22c55e';
                if (point.basis < 0 && point.basis >= -100) color = '#eab308';
                else if (point.basis < -100) color = '#ef4444';
                
                const bar = document.createElement('div');
                bar.style.flex = '1';
                bar.style.height = Math.max(heightPercent, 5) + '%';
                bar.style.backgroundColor = color;
                bar.style.opacity = '0.7';
                bar.style.borderRadius = '4px 4px 0 0';
                bar.style.minHeight = '5px';
                bar.title = '$' + point.basis.toFixed(2);
                bars.appendChild(bar);
            });
            
            chart.appendChild(yAxis);
            chart.appendChild(bars);
            container.appendChild(chart);
        }
        
        fetchData();
        setInterval(fetchData, 30000);
    </script>
</body>
</html>'''

# Start background thread
fetch_thread = threading.Thread(target=background_data_fetch, daemon=True)
fetch_thread.start()
logger.info("Background data fetch thread started")

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
