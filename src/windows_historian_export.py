import PyADO
import os
import datetime
import pandas as pd
import numpy as np
from datetime import datetime as datetimestr
import glob

# Configure paths - Using only local paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORT_PATH = os.path.join(SCRIPT_DIR, "historian_exports")
LOG_FILE = os.path.join(SCRIPT_DIR, "historian_export_log.txt")

def ensure_directory(path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        return f"Created directory: {path}"
    return f"Using existing directory: {path}"

def log_message(message):
    """Write log message to file and print to console"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"{timestamp} - {message}"
    print(log_line)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_line + "\n")
    except Exception as e:
        print(f"Warning: Could not write to log file: {str(e)}")

def convert_timestamp(timestamp):
    """Convert timestamp to string format"""
    if timestamp is None:
        return None
    try:
        if not isinstance(timestamp, datetime.datetime):
            timestamp = pd.to_datetime(timestamp)
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(timestamp)

def get_tags(cursor):
    """Get list of tags from Historian"""
    try:
        cursor.execute("SELECT tagname FROM ihtags")
        rows = cursor.fetchall()
        tags = [row[0] for row in rows]
        log_message(f"Retrieved {len(tags)} tags from Historian")
        return tags
    except Exception as e:
        log_message(f"Error getting tags: {str(e)}")
        return []

def fetch_data_in_chunks(cursor, tag, date_from, date_to):
    all_rows = []
    current_timestamp = date_from
    query_step_time = 6
    while current_timestamp < date_to:
        start_date = current_timestamp
        end_date = current_timestamp + datetime.timedelta(hours=query_step_time) - datetime.timedelta(seconds=1)
        query = f"SELECT timestamp, value FROM ihrawdata WHERE tagname = {tag} AND samplingmode=interpolated AND intervalmilliseconds=5s AND timestamp>= '{start_date}' AND timestamp < '{end_date}'"
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows and len(rows) > 0:  # Verificar que hay resultados
                for row in rows:
                    if row and len(row) > 0:  # Verificar que la fila tiene datos
                        row[0] = row[0].replace(tzinfo=None)
                        all_rows.extend([row])  # Extender con la fila como lista
            else:
                log_message(f"No data found for tag {tag} between {start_date} and {end_date}")
        except PyADO.adError as e:
            log_message(f"Error fetching data for tag {tag}: {e}")
            continue
        current_timestamp += datetime.timedelta(hours=query_step_time)
    return all_rows
    
def export_to_csv(tag_name, data, current_batch):
    """Export data to CSV file with fixed filename and maintain only last 3 months of data"""
    try:
        # Create filename without timestamp
        filename = os.path.join(EXPORT_PATH, f"{tag_name}.csv")
        
        # Convert new data to DataFrame
        new_df = pd.DataFrame(data, columns=['timestamp', 'value'])
        new_df['timestamp'] = pd.to_datetime(new_df['timestamp'])
        
        # If file exists, read it and append/update
        if os.path.exists(filename):
            try:
                existing_df = pd.read_csv(filename)
                existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                
                # Combine existing and new data
                combined_df = pd.concat([existing_df, new_df])
                
                # Remove duplicates keeping latest value
                combined_df = combined_df.drop_duplicates(subset='timestamp', keep='last')
                
                # Sort by timestamp
                combined_df = combined_df.sort_values('timestamp')
                
                # Keep only last 3 months of data
                cutoff_date = datetime.datetime.now() - datetime.timedelta(days=10)
                combined_df = combined_df[combined_df['timestamp'] > cutoff_date]
                
            except Exception as e:
                log_message(f"Error processing existing file {filename}: {str(e)}")
                combined_df = new_df
        else:
            combined_df = new_df
        
        # Save to CSV
        combined_df.to_csv(filename, index=False)
        log_message(f"Updated {filename} with {len(data)} new records")
        return filename
    except Exception as e:
        log_message(f"Error exporting to CSV: {str(e)}")
        return None

def main():
    print("\n" + "="*50)
    print("Historian Data Export Tool")
    print("="*50 + "\n")
    
    try:
        # Setup directories
        log_message(ensure_directory(EXPORT_PATH))
        
        # Connect to Historian
        log_message("Connecting to Historian database...")
        conn = PyADO.connect(None, host='100.114.78.86', user='administrador', 
                           password='Melissa2014', provider='iHOLEDB.iHistorian.1')
        cursor = conn.cursor()
        log_message("Connected to Historian database successfully")
        
        # Get list of tags
        log_message("Retrieving tag list...")
        tags = get_tags(cursor)
        if not tags:
            log_message("No tags found. Exiting.")
            return
            
        # Set time range for previous day
        end_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Inicio del día actual
        start_time = end_time - datetime.timedelta(days=1)  # Inicio del día anterior
        end_time = start_time + datetime.timedelta(days=1)
        
        log_message(f"Time range: {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Process each tag
        total_tags = len(tags)
        successful_exports = 0
        failed_exports = 0
        current_batch = 1
        
        for index, tag in enumerate(tags, 1):
            log_message(f"Processing tag {index}/{total_tags}: {tag}")
            
            # Fetch data
            data = fetch_data_in_chunks(cursor, tag, start_time, end_time)
            if data:
                # Export to CSV
                csv_file = export_to_csv(tag, data, current_batch)
                if csv_file:
                    successful_exports += 1
                    log_message(f"Successfully processed tag {tag}")
                else:
                    failed_exports += 1
                    log_message(f"Failed to export data for tag {tag}")
        
        # Summary
        log_message("\nExport Summary:")
        log_message(f"Total tags processed: {total_tags}")
        log_message(f"Successful exports: {successful_exports}")
        log_message(f"Failed exports: {failed_exports}")
        log_message(f"Export directory: {EXPORT_PATH}")
        log_message("Export process completed")
        
    except Exception as e:
        log_message(f"Error in main process: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            log_message("Database connection closed")
        
        print("\n" + "="*50)
        print(f"Files exported to: {EXPORT_PATH}")
"""         print("Process completed. Press Enter to exit...")
        input() """

if __name__ == "__main__":
    main()