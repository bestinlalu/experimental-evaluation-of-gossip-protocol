import mysql.connector
from mysql.connector import Error

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': '152.7.178.142',
    'database': 'gossipdb',
    'user': 'root',
    'password': 'password',
    'port': 3306
}

def clear_gossip_tables():
    connection = None
    try:
        print(f"Connecting to {DB_CONFIG['host']} to clear records...")
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Disabling foreign key checks is a safety measure if you have relationships
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            
            # Truncate tables to wipe all data and reset increments
            tables = ["ActionRecord", "GossipRecord"]
            
            for table in tables:
                print(f"Clearing {table}...")
                cursor.execute(f"TRUNCATE TABLE {table};")
            
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            connection.commit()
            
            print("Database tables are now empty and ready for a new run.")

    except Error as e:
        print(f"❌ Error while clearing database: {e}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    clear_gossip_tables()