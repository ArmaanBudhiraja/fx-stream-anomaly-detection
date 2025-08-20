#!/usr/bin/env python3
"""
FX Anomaly Dashboard

A simple web dashboard to view FX anomalies stored in the database.
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
import sqlite3

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.utils import load_config
from consumer.database_storage import DatabaseStorage

# Flask imports
try:
    from flask import Flask, render_template_string, jsonify
    from flask_cors import CORS
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False
    print("Flask not available. Install with: pip install flask flask-cors")


class AnomalyDashboard:
    """Simple dashboard for viewing FX anomalies."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the dashboard.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = load_config(config_path)
        self.db_storage = DatabaseStorage(config_path)
        self.app = None
        
        if FLASK_AVAILABLE:
            self._setup_flask_app()
    
    def _setup_flask_app(self):
        """Setup Flask web application."""
        self.app = Flask(__name__)
        CORS(self.app)
        
        @self.app.route('/')
        def dashboard():
            """Main dashboard page."""
            data = self.get_dashboard_data()
            html = self.generate_html_dashboard()
            return html
        
        @self.app.route('/api/data')
        def api_data():
            """API endpoint for dashboard data."""
            data = self.get_dashboard_data()
            return jsonify(data)
        
        @self.app.route('/api/anomalies')
        def api_anomalies():
            """API endpoint for recent anomalies."""
            anomalies = self.db_storage.get_anomalies(limit=100)
            return jsonify(anomalies)
    
    def run_dashboard(self, host: str = 'localhost', port: int = 5000, debug: bool = False):
        """
        Run the Flask web server for the dashboard.
        
        Args:
            host: Host to bind the server to
            port: Port to bind the server to
            debug: Enable debug mode
        """
        try:
            print(f"Starting FX Anomaly Dashboard on http://{host}:{port}")
            print("Press Ctrl+C to stop the server")
            
            # Initialize database connection
            self.db_storage._initialize_database()
            
            # Run Flask app
            self.app.run(host=host, port=port, debug=debug, use_reloader=False)
            
        except Exception as e:
            print(f"Error starting dashboard: {e}")
            raise
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """
        Get data for the dashboard.
        
        Returns:
            Dictionary containing dashboard data
        """
        try:
            # Get anomaly statistics
            stats = self.db_storage.get_anomaly_stats()
            
            # Get recent anomalies
            recent_anomalies = self.db_storage.get_anomalies(limit=50)
            
            # Get anomalies by currency pair
            currency_stats = stats.get('by_currency_pair', {})
            
            # Calculate trend (anomalies in last hour vs previous hour)
            now = datetime.now()
            one_hour_ago = now - timedelta(hours=1)
            two_hours_ago = now - timedelta(hours=2)
            
            # This is a simplified trend calculation
            # In a real implementation, you'd query the database for time-based stats
            trend = "stable"  # Placeholder
            
            dashboard_data = {
                'total_anomalies': stats.get('total_anomalies', 0),
                'recent_anomalies': stats.get('recent_anomalies', 0),
                'currency_stats': currency_stats,
                'recent_anomalies_list': recent_anomalies,
                'trend': trend,
                'last_updated': datetime.now().isoformat()
            }
            
            return dashboard_data
            
        except Exception as e:
            print(f"Error getting dashboard data: {e}")
            return {}
    
    def generate_html_dashboard(self) -> str:
        """
        Generate HTML dashboard.
        
        Returns:
            HTML string for the dashboard
        """
        data = self.get_dashboard_data()
        
        html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FX Anomaly Detection Dashboard</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            padding: 30px;
        }}
        .stat-card {{
            background: white;
            border-radius: 10px;
            padding: 25px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
            border-left: 4px solid #667eea;
        }}
        .stat-number {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
            margin-bottom: 10px;
        }}
        .stat-label {{
            color: #666;
            font-size: 1.1em;
        }}
        .anomalies-section {{
            padding: 30px;
        }}
        .anomalies-section h2 {{
            color: #333;
            margin-bottom: 20px;
        }}
        .anomaly-table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .anomaly-table th {{
            background-color: #667eea;
            color: white;
            padding: 15px;
            text-align: left;
        }}
        .anomaly-table td {{
            padding: 12px 15px;
            border-bottom: 1px solid #eee;
        }}
        .anomaly-table tr:hover {{
            background-color: #f8f9fa;
        }}
        .score-high {{
            color: #dc3545;
            font-weight: bold;
        }}
        .score-medium {{
            color: #ffc107;
            font-weight: bold;
        }}
        .score-low {{
            color: #28a745;
            font-weight: bold;
        }}
        .currency-chart {{
            padding: 30px;
        }}
        .chart-container {{
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .footer {{
            background-color: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #666;
            border-top: 1px solid #eee;
        }}
        .refresh-btn {{
            background-color: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 1em;
        }}
        .refresh-btn:hover {{
            background-color: #5a6fd8;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚨 FX Anomaly Detection Dashboard</h1>
            <p>Real-time monitoring of foreign exchange anomalies</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{data.get('total_anomalies', 0)}</div>
                <div class="stat-label">Total Anomalies</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{data.get('recent_anomalies', 0)}</div>
                <div class="stat-label">Anomalies (24h)</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(data.get('currency_stats', {}))}</div>
                <div class="stat-label">Currency Pairs</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{data.get('trend', 'Stable')}</div>
                <div class="stat-label">Trend</div>
            </div>
        </div>
        
        <div class="currency-chart">
            <div class="chart-container">
                <h2>Anomalies by Currency Pair</h2>
                <div style="display: flex; flex-wrap: wrap; gap: 10px;">
        """
        
        # Add currency pair stats
        for currency, count in data.get('currency_stats', {}).items():
            html += f"""
                    <div style="background: #f8f9fa; padding: 10px; border-radius: 5px; min-width: 120px;">
                        <strong>{currency}</strong><br>
                        <span style="color: #667eea; font-size: 1.2em;">{count}</span>
                    </div>
            """
        
        html += """
                </div>
            </div>
        </div>
        
        <div class="anomalies-section">
            <h2>Recent Anomalies</h2>
            <button class="refresh-btn" onclick="location.reload()">🔄 Refresh</button>
            <table class="anomaly-table">
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Currency Pair</th>
                        <th>Price</th>
                        <th>Amount</th>
                        <th>Anomaly Score</th>
                        <th>Data Source</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Add anomaly rows
        for anomaly in data.get('recent_anomalies_list', []):
            score = anomaly.get('anomaly_score', 0)
            score_class = 'score-high' if score < -0.5 else 'score-medium' if score < -0.2 else 'score-low'
            
            # Format timestamp
            timestamp = anomaly.get('detected_at', '')
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    formatted_time = timestamp
            else:
                formatted_time = 'N/A'
            
            html += f"""
                    <tr>
                        <td>{formatted_time}</td>
                        <td><strong>{anomaly.get('currency_pair', 'N/A')}</strong></td>
                        <td>{anomaly.get('price', 0):.4f}</td>
                        <td>{anomaly.get('amount', 0):,.0f}</td>
                        <td class="{score_class}">{anomaly.get('anomaly_score', 0):.4f}</td>
                        <td>{anomaly.get('data_source', 'unknown')}</td>
                    </tr>
            """
        
        html += """
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>Last updated: {data.get('last_updated', 'N/A')}</p>
            <p>FX Anomaly Detection System - Real-time monitoring</p>
        </div>
    </div>
    
    <script>
        // Auto-refresh every 30 seconds
        setTimeout(function() {{
            location.reload();
        }}, 30000);
    </script>
</body>
</html>
        """
        
        return html
    
    def save_dashboard(self, output_path: str = "dashboard/anomaly_dashboard.html"):
        """
        Save the dashboard HTML to a file.
        
        Args:
            output_path: Path to save the HTML file
        """
        try:
            # Create dashboard directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            html = self.generate_html_dashboard()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html)
            
            print(f"Dashboard saved to: {output_path}")
            print(f"Open {output_path} in your web browser to view the dashboard")
            
        except Exception as e:
            print(f"Error saving dashboard: {e}")
    
    def print_console_dashboard(self):
        """Print a simple console-based dashboard."""
        data = self.get_dashboard_data()
        
        print("\n" + "="*80)
        print("🚨 FX ANOMALY DETECTION DASHBOARD")
        print("="*80)
        print(f"Total Anomalies: {data.get('total_anomalies', 0)}")
        print(f"Recent Anomalies (24h): {data.get('recent_anomalies', 0)}")
        print(f"Currency Pairs: {len(data.get('currency_stats', {}))}")
        print(f"Trend: {data.get('trend', 'Stable')}")
        print("="*80)
        
        print("\n📊 Anomalies by Currency Pair:")
        for currency, count in data.get('currency_stats', {}).items():
            print(f"  {currency}: {count}")
        
        print(f"\n🕒 Recent Anomalies (last 10):")
        for i, anomaly in enumerate(data.get('recent_anomalies_list', [])[:10]):
            print(f"  {i+1}. {anomaly.get('currency_pair', 'N/A')} @ {anomaly.get('price', 0):.4f} "
                  f"(Score: {anomaly.get('anomaly_score', 0):.4f})")
        
        print(f"\nLast updated: {data.get('last_updated', 'N/A')}")
        print("="*80 + "\n")


def main():
    """Main function to run the dashboard."""
    import argparse
    
    parser = argparse.ArgumentParser(description='FX Anomaly Dashboard')
    parser.add_argument('--output', type=str, default='dashboard/anomaly_dashboard.html',
                       help='Output path for HTML dashboard')
    parser.add_argument('--console', action='store_true',
                       help='Show console dashboard instead of HTML')
    parser.add_argument('--web', action='store_true', default=True,
                       help='Run web server (default)')
    parser.add_argument('--host', type=str, default='localhost',
                       help='Host for web server')
    parser.add_argument('--port', type=int, default=5000,
                       help='Port for web server')
    parser.add_argument('--config', type=str, default='config/config.yaml',
                       help='Path to configuration file')
    
    args = parser.parse_args()
    
    try:
        dashboard = AnomalyDashboard(config_path=args.config)
        
        if args.console:
            dashboard.print_console_dashboard()
        elif args.web:
            dashboard.run_dashboard(host=args.host, port=args.port)
        else:
            dashboard.save_dashboard(args.output)
            
    except Exception as e:
        print(f"Error running dashboard: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 