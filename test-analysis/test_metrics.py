import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': '152.7.179.141',
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
        print(f"❌ Error connecting to MySQL: {e}")
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
        print(f"❌ Error fetching data: {e}")
        return None

def plot_metrics(df):
    if df is None or df.empty:
        print("No data found to plot.")
        return

    # Ensure timestamp is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Create the figure
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Real-time Gossip Node Metrics (MySQL Source)', fontsize=16)

    nodes = df['nodeAddress'].unique()
    
    # Configuration for the 4 subplots
    metrics = [
        ('cpuPercentage', 'CPU Usage (%)', axes[0, 0]),
        ('memoryPercentage', 'Memory Usage (MB)', axes[0, 1]),
        ('io_read_mbytes', 'I/O Read (MB/s)', axes[1, 0]),
        ('io_write_mbytes', 'I/O Write (MB/s)', axes[1, 1])
    ]

    for col_name, ylabel, ax in metrics:
        for node in nodes:
            node_df = df[df['nodeAddress'] == node]
            ax.plot(node_df['timestamp'], node_df[col_name], label=node)
        
        ax.set_ylabel(ylabel)
        ax.set_xlabel('Time')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        
        # Adding a legend only to the CPU plot to keep it clean
        if col_name == 'cpuPercentage':
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('mysql_gossip_metrics.png')
    print("✅ Graph saved as mysql_gossip_metrics.png")
    plt.show()

if __name__ == "__main__":
    data = fetch_data_from_mysql()
    plot_metrics(data)