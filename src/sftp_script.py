import paramiko
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = RotatingFileHandler(
        'logs/transfer.log',
        maxBytes=10*1024*1024,
        backupCount=30
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def check_and_cleanup_stale_lock():
    try:
        if not os.path.exists("transfer.lock"):
            return False
            
        with open("transfer.lock", "r") as f:
            timestamp_str = f.read().strip()
            lock_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
            
        if (datetime.now() - lock_time).total_seconds() > 1800:
            os.remove("transfer.lock")
            logging.info("Removed stale lock file")
            return False
            
        return True
    except Exception as e:
        logging.error(f"Error checking lock file: {e}")
        if os.path.exists("transfer.lock"):
            os.remove("transfer.lock")
        return False

def create_lock_file():
    with open("transfer.lock", "w") as f:
        f.write(str(datetime.now()))

def remove_lock_file():
    try:
        if os.path.exists("transfer.lock"):
            os.remove("transfer.lock")
    except Exception as e:
        logging.error(f"Error removing lock file: {e}")

class HistorianTransfer:
    def __init__(self):
        self.hostname = "100.114.78.86"
        self.username = "administrador"
        self.password = "Melissa2014"
        self.remote_path = r"C:\Users\Administrador\Desktop\PROCESS_SERVER_BACKUP_SCRIPT\historian_exports"
        self.local_path = "/home/mpp/historian_export/historian_exports"
        self.temp_path = "/home/mpp/historian_export/temp"
        os.makedirs(self.local_path, exist_ok=True)
        os.makedirs(self.temp_path, exist_ok=True)

    def merge_csv_files(self, temp_file, local_file):
        try:
            # Read the new data
            new_data = pd.read_csv(temp_file)
            new_data['timestamp'] = pd.to_datetime(new_data['timestamp'])
            
            # If local file exists, merge with existing data
            if os.path.exists(local_file):
                existing_data = pd.read_csv(local_file)
                existing_data['timestamp'] = pd.to_datetime(existing_data['timestamp'])
                
                # Concatenate and remove duplicates based on timestamp
                merged_data = pd.concat([existing_data, new_data]).drop_duplicates(
                    subset=['timestamp'], 
                    keep='last'
                ).sort_values('timestamp')
                
                # Keep only last 90 days of data
                cutoff_date = datetime.now() - timedelta(days=10)
                merged_data = merged_data[merged_data['timestamp'] > cutoff_date]
                
                # Save merged data
                merged_data.to_csv(local_file, index=False)
                logging.info(f"Successfully merged data for {os.path.basename(local_file)}")
            else:
                # If no existing file, just save the new data
                new_data.to_csv(local_file, index=False)
                logging.info(f"Created new file {os.path.basename(local_file)}")
                
        except Exception as e:
            logging.error(f"Error merging files: {e}")
            # In case of error, keep the existing file
            if os.path.exists(temp_file):
                os.remove(temp_file)

    def transfer_files(self):
        if check_and_cleanup_stale_lock():
            logging.info("Transfer already in progress. Skipping.")
            return

        create_lock_file()
        sftp = None

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=self.hostname,
                username=self.username,
                password=self.password,
                timeout=30
            )
            sftp = ssh.open_sftp()
            
            remote_files = sftp.listdir(self.remote_path)
            csv_files = [f for f in remote_files if f.endswith('.csv')]
            
            if not csv_files:
                logging.warning("No CSV files found in remote directory")
                return

            transferred_count = 0
            for file in csv_files:
                try:
                    remote_file = f"{self.remote_path}\\{file}"
                    temp_file = os.path.join(self.temp_path, f"temp_{file}")
                    local_file = os.path.join(self.local_path, file)
                    
                    # Download to temp location first
                    logging.info(f"Transferring {file}")
                    sftp.get(remote_file, temp_file)
                    
                    # Merge with existing file
                    self.merge_csv_files(temp_file, local_file)
                    
                    # Clean up temp file
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                        
                    transferred_count += 1
                    
                except Exception as e:
                    logging.error(f"Error transferring {file}: {e}")
                    if os.path.exists(temp_file):
                        os.remove(temp_file)

            logging.info(f"Transfer completed. Processed {transferred_count} files.")

        except Exception as e:
            logging.error(f"Error during transfer: {e}")
        finally:
            if sftp:
                try:
                    sftp.close()
                    logging.info("SFTP connection closed")
                except:
                    pass
            # Clean up temp directory
            for file in os.listdir(self.temp_path):
                try:
                    os.remove(os.path.join(self.temp_path, file))
                except:
                    pass
            remove_lock_file()

def main():
    setup_logging()
    logging.info("Starting file transfer")
    transfer = HistorianTransfer()
    transfer.transfer_files()

if __name__ == "__main__":
    main()