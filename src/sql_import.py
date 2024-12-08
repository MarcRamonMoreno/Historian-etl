import pyodbc
import pandas as pd
import os
import glob
import logging
from datetime import datetime
import time
from logging.handlers import RotatingFileHandler

def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logger = logging.getLogger('SQLImporter')
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler('logs/sql_import.log', maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

class SQLImporter:
    def __init__(self):
        self.logger = setup_logging()
        self.conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=100.97.52.112;"
            "DATABASE=HistorianData;"
            "UID=mpp;"
            "PWD=MPP_DataBase_2024##;"
            "TrustServerCertificate=yes"
        )
        self.csv_dir = "/home/mpp/historian_export/historian_exports"
        self.batch_size = 1000
        self.imported_tags = set()

    def get_full_tag_name(self, filename):
        """Extract full tag name from filename."""
        base_name = os.path.basename(filename)
        # Remove only the .F_CV.csv extension
        return base_name[:-9] if base_name.endswith('.F_CV.csv') else base_name
    
    def get_latest_timestamp(self, cursor, tag_name):
        """Get the latest timestamp for a given tag from the database."""
        query = "SELECT MAX(Timestamp) FROM TagData WHERE TagName = ?"
        cursor.execute(query, (tag_name,))
        result = cursor.fetchone()[0]
        return result or datetime.min
    
    def is_tag_imported(self, tag_name):
        """Check if tag has already been imported."""
        return tag_name in self.imported_tags

    def add_imported_tag(self, tag_name):
        """Mark tag as imported."""
        self.imported_tags.add(tag_name)

    def import_file(self, file_path, conn, file_number, total_files):
        try:
            tag_name = self.get_full_tag_name(file_path)
            
            # Skip if this tag has already been imported
            if self.is_tag_imported(tag_name):
                self.logger.info(f"Skipping duplicate tag file: {tag_name}")
                return True
            
            start_time = time.time()
            df = pd.read_csv(file_path)
            cursor = conn.cursor()
            
            # Convert timestamp column
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Get latest timestamp for this tag
            latest_timestamp = self.get_latest_timestamp(cursor, tag_name)
            
            # Filter for new records only (after the latest timestamp in database)
            df = df[df['timestamp'] > latest_timestamp]
            
            if len(df) == 0:
                self.logger.info(f"No new data found for {tag_name}")
                return True

            total_rows = len(df)
            self.logger.info(f"Found {total_rows} new records for {tag_name}")
            
            # Direct insert of records
            insert_sql = "INSERT INTO TagData (TagName, Timestamp, Value) VALUES (?, ?, ?)"
            cursor.fast_executemany = True
            
            for i in range(0, len(df), self.batch_size):
                batch = df.iloc[i:i + self.batch_size]
                values = [(tag_name, row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'), float(row['value']))
                         for _, row in batch.iterrows()]
                cursor.executemany(insert_sql, values)
                conn.commit()
                
                rows_processed = i + len(batch)
                if rows_processed % self.batch_size == 0:
                    speed = rows_processed / (time.time() - start_time)
                    self.logger.info(f"Progress: {rows_processed}/{total_rows} rows - Speed: {speed:.1f} rows/sec")
            
            # Mark this tag as imported
            self.add_imported_tag(tag_name)
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"Completed importing {total_rows} records in {elapsed_time:.1f} seconds")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing {file_path}: {e}")
            return False

    def import_all(self):
        try:
            start_time = time.time()
            self.logger.info("Starting import process")
            conn = pyodbc.connect(self.conn_str)
            
            # Get all CSV files
            files = glob.glob(os.path.join(self.csv_dir, "*.csv"))
            total_files = len(files)
            self.logger.info(f"Found {total_files} CSV files to process")
            
            successful_imports = 0
            for i, file in enumerate(files, 1):
                success = self.import_file(file, conn, i, total_files)
                if success:
                    successful_imports += 1
                elapsed = time.time() - start_time
                self.logger.info(f"Overall Progress: {i}/{total_files} files ({(i/total_files)*100:.1f}%) - Elapsed time: {elapsed/60:.1f} minutes")
            
            total_time = time.time() - start_time
            self.logger.info(f"Import completed: {successful_imports}/{total_files} files imported successfully in {total_time/60:.1f} minutes")
            conn.close()
        except Exception as e:
            self.logger.error(f"Error in import_all: {e}")

def main():
    importer = SQLImporter()
    importer.import_all()

if __name__ == "__main__":
    main()