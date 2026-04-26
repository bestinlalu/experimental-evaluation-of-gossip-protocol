import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': '152.7.178.142',
    'database': 'gossipdb',
    'user': 'root',
    'password': 'password',
    'port': 3306,
    'connect_timeout': 10
}

engine = create_engine(f"mysql+mysqlconnector://root:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

def get_db_connection():
    try:
        conn = create_engine(f"mysql+mysqlconnector://root:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}").connect()
        conn.autocommit = True
        return conn
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def fetch_data_from_mysql():
    try:
        # 1. Use a context manager (the 'with' statement) 
        # This handles opening AND closing the connection automatically
        with engine.connect() as connection:
            query = "SELECT * FROM MetricsEvent ORDER BY timestamp ASC"
            
            # 2. Pass the connection object directly to pandas
            df = pd.read_sql(query, connection)
            
            if df.empty:
                print("⚠️ Table is empty.")
                return None
            return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def plot_metrics(df):
    if df is None or df.empty:
        print("No data found to plot.")
        return

    # 1. Pre-processing
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    # 2. Setup Figure
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Gossip Cluster Health: 200 PUSHPULL Node Fleet Overview', fontsize=18, fontweight='bold', y=0.98)

    # Configuration: (Column, Label, Axis, Primary Color)
    metrics_config = [
        ('cpuPercentage', 'CPU Usage (%)', axes[0, 0], '#e74c3c'),    # Red
        ('memoryPercentage', 'Memory Usage (%)', axes[0, 1], '#3498db'), # Blue
        ('io_read_mbytes', 'I/O Read (bps)', axes[1, 0], '#2ecc71'),   # Green
        ('io_write_mbytes', 'I/O Write (bps)', axes[1, 1], '#9b59b6')  # Purple
    ]

    for col, ylabel, ax, color in metrics_config:
        # --- STATISTICAL AGGREGATION ---
        # Group by timestamp to calculate the behavior of the fleet as a single unit
        stats = df.groupby('timestamp')[col].agg(['mean', 'std', 'max']).reset_index()
        
        # SMOOTHING: Apply a rolling window to remove the "shag carpet" jitter
        # A window of 5-10 is usually perfect for gossip intervals
        stats['mean_smooth'] = stats['mean'].rolling(window=7, min_periods=1).mean()
        stats['std_smooth'] = stats['std'].rolling(window=7, min_periods=1).mean()
        stats['max_smooth'] = stats['max'].rolling(window=7, min_periods=1).mean()

        # --- PLOTTING ---
        # A. Shaded Area: Represents the 1-Standard Deviation spread (the 100-node "Cloud")
        ax.fill_between(
            stats['timestamp'], 
            stats['mean_smooth'] - stats['std_smooth'], 
            stats['mean_smooth'] + stats['std_smooth'], 
            color=color, alpha=0.2, label='Cluster Spread (1σ)'
        )

        # B. Average Line: The central trend of the cluster
        ax.plot(
            stats['timestamp'], stats['mean_smooth'], 
            color='black', linewidth=2, label='Fleet Average', linestyle='-'
        )

        # C. Max Outlier: A faint line showing the absolute peak node at any time
        ax.plot(
            stats['timestamp'], stats['max_smooth'], 
            color=color, linewidth=1, alpha=0.4, linestyle=':', label='Max Outlier'
        )

        # --- FORMATTING ---
        ax.set_ylabel(ylabel, fontsize=12, fontweight='semibold')
        ax.set_xlabel('Time', fontsize=10)
        ax.grid(True, which='both', linestyle='--', alpha=0.5)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        
        # Legend: Compact and clear
        ax.legend(loc='upper right', fontsize='small', frameon=True, shadow=True)

    # 3. Final Adjustments
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    
    # Save and Show
    output_name = 'gossip_fleet_metrics_pro.png'
    plt.savefig(output_name, dpi=300)
    print(f"✅ Professional graph saved as {output_name}")
    plt.show()

if __name__ == "__main__":
    data = fetch_data_from_mysql()
    plot_metrics(data)