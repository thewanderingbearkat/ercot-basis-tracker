from flask import Flask, jsonify
from flask_cors import CORS
from gridstatus import Ercot
import threading
import time
from datetime import datetime
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend requests

# Node names
NODE_1 = "NBOHR_RN"  # Wind node
NODE_2 = "HB_WEST"   # Hub node
ALERT_THRESHOLD = 100  # $/MWh
GREEN_THRESHOLD = -100  # $/MWh - alert is green if basis > this value

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

def get_historical_prices(hours_back=4):
    """Fetch historical LMP prices from ERCOT for the last N hours"""
    try:
        from datetime import timedelta
        from zoneinfo import ZoneInfo
        ercot = Ercot()
        
        # Get today's date in Central Time
        cst_tz = ZoneInfo("US/Central")
        today_cst = datetime.now(cst_tz).date()
        
        # Fetch data for today
        logger.info(f"Fetching ERCOT data for {today_cst}")
        lmp_data = ercot.get_lmp(date=str(today_cst), location_type="settlement point")
        
        if lmp_data is None or len(lmp_data) == 0:
            logger.warning(f"No LMP data available for {today_cst}")
            return []
        
        # Filter for our two nodes
        node1_data = lmp_data[lmp_data['Location'] == NODE_1].copy()
        node2_data = lmp_data[lmp_data['Location'] == NODE_2].copy()
        
        if len(node1_data) == 0 or len(node2_data) == 0:
            logger.warning(f"No data found for nodes {NODE_1} or {NODE_2}")
            return []
        
        # Merge on interval to get matching prices
        merged = node1_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_1_LMP'}).merge(
            node2_data[['Interval Start', 'LMP']].rename(columns={'LMP': 'NODE_2_LMP'}),
            on='Interval Start'
        )
        
        if len(merged) == 0:
            logger.warning("No matching intervals between nodes")
            return []
        
        # Convert Interval Start to datetime if it's not already
        merged['Interval Start'] = pd.to_datetime(merged['Interval Start'])
        
        # Make cutoff time timezone-aware (US/Central)
        cutoff_time = datetime.now(cst_tz) - timedelta(hours=hours_back)
        
        # Filter for intervals within the last N hours
        merged = merged[merged['Interval Start'] >= cutoff_time]
        
        # Calculate basis and sort by time (most recent last)
        merged['BASIS'] = merged['NODE_1_LMP'] - merged['NODE_2_LMP']
        merged = merged.sort_values('Interval Start')
        
        logger.info(f"Fetched {len(merged)} historical data points for {NODE_1} vs {NODE_2}")
        
        # Convert to list of dicts
        history = []
        for _, row in merged.iterrows():
            basis = row['BASIS']
            status = "safe" if basis > GREEN_THRESHOLD else "alert"
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
        import traceback
        logger.error(traceback.format_exc())
        return []

def calculate_basis(price1, price2):
    """Calculate basis spread between the two nodes"""
    if price1 is not None and price2 is not None:
        return price1 - price2
    return None

def format_price(price):
    """Format price for display"""
    return f"${price:,.2f}" if price is not None else "N/A"

def get_basis_alert_status(basis):
    """
    Determine alert status based on basis value.
    Green: basis > 0 (safe, no concerns)
    Yellow: -100 <= basis <= 0 (caution, potential concerns)
    Red: basis < -100 (alert condition)
    """
    if basis is None:
        return "UNKNOWN", "unknown"
    
    if basis > 0:
        return "OK", "safe"
    elif basis >= -100:
        return "CAUTION", "caution"
    else:
        return "ALERT", "alert"

def background_data_fetch():
    """Background thread that continuously fetches ERCOT data"""
    last_interval = None
    
    while True:
        try:
            # Fetch full historical data (last few hours)
            history = get_historical_prices()
            
            if history and len(history) > 0:
                # Get the latest point
                latest_point = history[-1]
                
                with data_lock:
                    latest_data["node1_price"] = latest_point['node1_price']
                    latest_data["node2_price"] = latest_point['node2_price']
                    latest_data["basis"] = latest_point['basis']
                    latest_data["last_update"] = datetime.now().isoformat()
                    latest_data["data_time"] = str(latest_point['time'])
                    latest_data["status"] = latest_point['status']
                    # Keep last 100 data points for chart (several hours of data)
                    latest_data["history"] = history[-100:]
                
                logger.info(f"Updated: {NODE_1}=${latest_point['node1_price']}, {NODE_2}=${latest_point['node2_price']}, Basis=${latest_point['basis']}, Status={latest_point['status']}")
            else:
                with data_lock:
                    latest_data["status"] = "no_data"
                logger.warning("No price data available")
            
            # Wait 2 minutes between checks (ERCOT publishes every 5 min)
            time.sleep(120)
            
        except Exception as e:
            logger.error(f"Error in background fetch: {e}")
            with data_lock:
                latest_data["status"] = "error"
            time.sleep(60)  # Retry after 1 minute on error

# Start background data fetcher thread
fetch_thread = threading.Thread(target=background_data_fetch, daemon=True)
fetch_thread.start()

@app.route('/api/basis', methods=['GET'])
def get_basis():
    """API endpoint to get latest basis data"""
    with data_lock:
        return jsonify(latest_data)

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "data_status": latest_data["status"]
    })

@app.route('/', methods=['GET'])
def dashboard():
    """Serve the dashboard HTML"""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ERCOT Basis Tracker</title>
        <script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
        <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://unpkg.com/@lucide/icons"></script>
    </head>
    <body>
        <div id="root"></div>
        <script type="module">
            import React, { useState, useEffect } from 'https://esm.sh/react@18';
            import ReactDOM from 'https://esm.sh/react-dom@18/client';
            
            const BasisDashboard = () => {
              const [basis, setBasis] = useState(null);
              const [node1Price, setNode1Price] = useState(null);
              const [node2Price, setNode2Price] = useState(null);
              const [lastUpdate, setLastUpdate] = useState(null);
              const [dataTime, setDataTime] = useState(null);
              const [history, setHistory] = useState([]);
              const [status, setStatus] = useState('loading');
              const [connectionStatus, setConnectionStatus] = useState('connecting');
            
              const GREEN_THRESHOLD = -100;
              const RED_THRESHOLD = -100;
              const API_URL = 'http://' + window.location.hostname + ':5000/api/basis';
            
              useEffect(() => {
                const fetchData = async () => {
                  try {
                    const response = await fetch(API_URL);
                    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                    
                    const data = await response.json();
                    
                    if (data.node1_price !== null && data.node2_price !== null && data.basis !== null) {
                      setNode1Price(data.node1_price);
                      setNode2Price(data.node2_price);
                      setBasis(data.basis);
                      setStatus(data.status);
                      setLastUpdate(new Date(data.last_update));
                      setDataTime(data.data_time);
                      setConnectionStatus('connected');
            
                      // Use historical data from server if available
                      if (data.history && data.history.length > 0) {
                        setHistory(data.history.map(point => ({
                          basis: point.basis,
                          time: new Date(point.time)
                        })));
                      } else {
                        setHistory(prev => {
                          const newHistory = [...prev.slice(-99), { basis: data.basis, time: new Date() }];
                          return newHistory;
                        });
                      }
                    } else {
                      setConnectionStatus('no_data');
                    }
                  } catch (error) {
                    console.error('Error fetching data:', error);
                    setConnectionStatus('error');
                  }
                };
            
                fetchData();
                const interval = setInterval(fetchData, 30000);
            
                return () => clearInterval(interval);
              }, []);
            
              const getStatusInfo = (basisValue) => {
                if (basisValue === null) return { status: 'Unknown', color: 'gray', bgColor: 'bg-gray-100', textColor: 'text-gray-700' };
                if (basisValue > GREEN_THRESHOLD) {
                  return { status: 'Safe', color: 'green', bgColor: 'bg-green-50', textColor: 'text-green-700' };
                } else {
                  return { status: 'Alert', color: 'red', bgColor: 'bg-red-50', textColor: 'text-red-700' };
                }
              };
            
              const getConnectionStatusDisplay = () => {
                if (connectionStatus === 'connected') return { text: 'Connected', color: 'text-green-400' };
                if (connectionStatus === 'error') return { text: 'Connection Error', color: 'text-red-400' };
                if (connectionStatus === 'no_data') return { text: 'No Data', color: 'text-yellow-400' };
                return { text: 'Connecting...', color: 'text-blue-400' };
              };
            
              const statusInfo = getStatusInfo(basis);
              const connectionDisplay = getConnectionStatusDisplay();
            
              return React.createElement('div', { className: 'min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-8' },
                React.createElement('div', { className: 'max-w-4xl mx-auto' },
                  React.createElement('div', { className: 'mb-8 flex justify-between items-start' },
                    React.createElement('div', null,
                      React.createElement('h1', { className: 'text-4xl font-bold text-white mb-2' }, 'ERCOT Basis Tracker'),
                      React.createElement('p', { className: 'text-slate-400' }, 'Real-time basis spread monitoring between NBOHR_RN and HB_WEST')
                    ),
                    React.createElement('div', { className: 'text-right' },
                      React.createElement('div', { className: `flex items-center gap-2 ${connectionDisplay.color} font-semibold text-sm` },
                        connectionStatus === 'connecting' && React.createElement('span', null, '⟳'),
                        React.createElement('span', null, connectionDisplay.text)
                      ),
                      dataTime && React.createElement('p', { className: 'text-slate-500 text-xs mt-2' }, dataTime)
                    )
                  ),
                  React.createElement('div', { className: 'grid grid-cols-1 md:grid-cols-3 gap-6 mb-8' },
                    React.createElement('div', { className: 'bg-slate-700 rounded-lg p-6 border border-slate-600' },
                      React.createElement('p', { className: 'text-slate-400 text-sm font-medium mb-2' }, 'NBOHR_RN (Wind Node)'),
                      React.createElement('div', { className: 'flex items-baseline gap-2' },
                        React.createElement('span', { className: 'text-3xl font-bold text-white' }, node1Price ? `${node1Price.toFixed(2)}` : 'N/A'),
                        React.createElement('span', { className: 'text-slate-400' }, '/MWh')
                      )
                    ),
                    React.createElement('div', { className: 'bg-slate-700 rounded-lg p-6 border border-slate-600' },
                      React.createElement('p', { className: 'text-slate-400 text-sm font-medium mb-2' }, 'HB_WEST (Hub Node)'),
                      React.createElement('div', { className: 'flex items-baseline gap-2' },
                        React.createElement('span', { className: 'text-3xl font-bold text-white' }, node2Price ? `${node2Price.toFixed(2)}` : 'N/A'),
                        React.createElement('span', { className: 'text-slate-400' }, '/MWh')
                      )
                    ),
                    React.createElement('div', { className: `rounded-lg p-6 border-2 ${statusInfo.bgColor} border-${statusInfo.color}-200` },
                      React.createElement('p', { className: `text-sm font-medium mb-2 ${statusInfo.textColor}` }, 'Basis Spread'),
                      React.createElement('div', { className: 'flex items-baseline gap-2 mb-3' },
                        React.createElement('span', { className: `text-3xl font-bold ${statusInfo.textColor}` }, basis ? `${basis.toFixed(2)}` : 'N/A'),
                        React.createElement('span', { className: 'text-slate-500' }, '/MWh')
                      )
                    )
                  ),
                  React.createElement('div', { className: `rounded-lg p-8 mb-8 border-2 ${statusInfo.bgColor} border-${statusInfo.color}-300` },
                    React.createElement('div', { className: 'flex items-center gap-4' },
                      React.createElement('div', null,
                        React.createElement('h2', { className: `text-2xl font-bold ${statusInfo.textColor} mb-1` }, statusInfo.status),
                        React.createElement('p', { className: 'text-slate-600' },
                          statusInfo.status === 'Safe' 
                            ? `Basis is within safe zone (> ${GREEN_THRESHOLD}/MWh) — Nothing to worry about`
                            : statusInfo.status === 'Unknown'
                            ? 'Waiting for data from server...'
                            : 'Basis exceeds alert threshold — Monitor closely'
                        )
                      )
                    )
                  ),
                  React.createElement('div', { className: 'grid grid-cols-1 md:grid-cols-2 gap-6 mb-8' },
                    React.createElement('div', { className: 'bg-green-900/20 rounded-lg p-4 border border-green-700/30' },
                      React.createElement('div', { className: 'flex gap-2 items-start' },
                        React.createElement('div', { className: 'w-2 h-2 bg-green-500 rounded-full mt-2' }),
                        React.createElement('div', null,
                          React.createElement('p', { className: 'text-green-400 font-semibold text-sm' }, 'Safe Zone'),
                          React.createElement('p', { className: 'text-green-300/70 text-xs' }, `Basis > ${GREEN_THRESHOLD}/MWh`)
                        )
                      )
                    ),
                    React.createElement('div', { className: 'bg-red-900/20 rounded-lg p-4 border border-red-700/30' },
                      React.createElement('div', { className: 'flex gap-2 items-start' },
                        React.createElement('div', { className: 'w-2 h-2 bg-red-500 rounded-full mt-2' }),
                        React.createElement('div', null,
                          React.createElement('p', { className: 'text-red-400 font-semibold text-sm' }, 'Alert Zone'),
                          React.createElement('p', { className: 'text-red-300/70 text-xs' }, `Basis ≤ ${RED_THRESHOLD}/MWh`)
                        )
                      )
                    )
                  ),
                  history.length > 0 && React.createElement('div', { className: 'bg-slate-700 rounded-lg p-6 border border-slate-600 mb-8' },
                    React.createElement('h3', { className: 'text-white font-semibold mb-4' }, 'Basis Trend (Last 20 Updates)'),
                    React.createElement('svg', { width: '100%', height: '250', style: { border: '1px solid #4b5563' } },
                      React.createElement('defs', null,
                        React.createElement('linearGradient', { id: 'areaGradient', x1: '0%', y1: '0%', x2: '0%', y2: '100%' },
                          React.createElement('stop', { offset: '0%', stopColor: '#22c55e', stopOpacity: 0.3 }),
                          React.createElement('stop', { offset: '100%', stopColor: '#22c55e', stopOpacity: 0 })
                        )
                      ),
                      (() => {
                        const width = 800;
                        const height = 250;
                        const padding = 40;
                        const plotWidth = width - (padding * 2);
                        const plotHeight = height - (padding * 2);
                        
                        // Find min and max basis values, but use a narrow range centered around 0
                        const values = history.map(p => p.basis);
                        const min = Math.min(...values);
                        const max = Math.max(...values);
                        const mid = (min + max) / 2;
                        const range = Math.max(Math.abs(min - mid), Math.abs(max - mid)) * 1.5; // Add 50% padding
                        const yMin = mid - range;
                        const yMax = mid + range;
                        
                        // Calculate points for line and area
                        const points = history.map((point, idx) => {
                          const x = padding + (idx / (history.length - 1 || 1)) * plotWidth;
                          const normalizedY = (point.basis - yMin) / (yMax - yMin);
                          const y = padding + (1 - normalizedY) * plotHeight;
                          return { x, y, basis: point.basis };
                        });
                        
                        // Create smooth line path using quadratic curves
                        let pathD = `M ${points[0].x} ${points[0].y}`;
                        for (let i = 1; i < points.length; i++) {
                          const xc = (points[i - 1].x + points[i].x) / 2;
                          const yc = (points[i - 1].y + points[i].y) / 2;
                          pathD += ` Q ${points[i - 1].x} ${points[i - 1].y} ${xc} ${yc}`;
                        }
                        pathD += ` L ${points[points.length - 1].x} ${points[points.length - 1].y}`;
                        
                        // Create area path
                        let areaD = pathD + ` L ${points[points.length - 1].x} ${height - padding} L ${points[0].x} ${height - padding} Z`;
                        
                        return [
                          React.createElement('g', null,
                            // Grid lines
                            React.createElement('line', { x1: padding, y1: padding, x2: width - padding, y2: padding, stroke: '#4b5563', strokeDasharray: '5,5' }),
                            React.createElement('line', { x1: padding, y1: padding + plotHeight / 2, x2: width - padding, y2: padding + plotHeight / 2, stroke: '#4b5563', strokeDasharray: '5,5' }),
                            React.createElement('line', { x1: padding, y1: height - padding, x2: width - padding, y2: height - padding, stroke: '#4b5563', strokeWidth: 2 }),
                            // Y-axis
                            React.createElement('line', { x1: padding, y1: padding, x2: padding, y2: height - padding, stroke: '#6b7280', strokeWidth: 2 }),
                            // Y-axis labels
                            React.createElement('text', { x: padding - 10, y: padding - 5, fill: '#9ca3af', fontSize: '12', textAnchor: 'end' }, `${yMax.toFixed(2)}`),
                            React.createElement('text', { x: padding - 10, y: padding + plotHeight / 2 + 4, fill: '#9ca3af', fontSize: '12', textAnchor: 'end' }, `${mid.toFixed(2)}`),
                            React.createElement('text', { x: padding - 10, y: height - padding + 4, fill: '#9ca3af', fontSize: '12', textAnchor: 'end' }, `${yMin.toFixed(2)}`),
                            // Safe zone indicator
                            React.createElement('rect', { x: padding, y: padding, width: plotWidth, height: ((-100 - yMin) / (yMax - yMin)) * plotHeight, fill: '#22c55e', opacity: 0.1 }),
                            React.createElement('text', { x: width - padding - 5, y: padding + 15, fill: '#22c55e', fontSize: '11', textAnchor: 'end', opacity: 0.7 }, 'Safe Zone')
                          ),
                          // Area fill
                          React.createElement('path', { d: areaD, fill: 'url(#areaGradient)', stroke: 'none' }),
                          // Line
                          React.createElement('path', { d: pathD, stroke: '#22c55e', strokeWidth: 2, fill: 'none', strokeLinecap: 'round', strokeLinejoin: 'round' }),
                          // Data points
                          points.map((point, idx) =>
                            React.createElement('circle', {
                              key: idx,
                              cx: point.x,
                              cy: point.y,
                              r: 4,
                              fill: '#22c55e',
                              stroke: '#1f2937',
                              strokeWidth: 2
                            })
                          )
                        ];
                      })()
                    ),
                    React.createElement('div', { className: 'flex justify-between text-xs text-slate-400 mt-2' },
                      React.createElement('span', null, 'Oldest'),
                      React.createElement('span', null, 'Latest')
                    )
                  ),
                  lastUpdate && React.createElement('div', { className: 'text-center' },
                    React.createElement('p', { className: 'text-slate-500 text-sm' },
                      `Last updated: ${lastUpdate.toLocaleTimeString()}`
                    )
                  )
                )
              );
            };
            
            const root = ReactDOM.createRoot(document.getElementById('root'));
            root.render(React.createElement(BasisDashboard));
        </script>
    </body>
    </html>
    """
    return html

if __name__ == '__main__':
    print("=" * 80)
    print(f"ERCOT BASIS TRACKER: {NODE_1} vs {NODE_2}")
    print(f"Red Alert Threshold: Spread <= ${-ALERT_THRESHOLD}/MWh")
    print(f"Green Safe Zone: Basis > ${GREEN_THRESHOLD}/MWh (nothing to worry about)")
    print("=" * 80)
    print("\nBackend Server Starting...")
    print(f"Server running on http://localhost:5000")
    print("API endpoint: http://localhost:5000/api/basis")
    print("Health check: http://localhost:5000/api/health")
    print("\nFetching data from ERCOT via gridstatus...")
    print("Press Ctrl+C to stop\n")
    print("=" * 80)
    
    # Run Flask app
    app.run(debug=False, host='0.0.0.0', port=5000)
