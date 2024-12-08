import schedule
import time
import subprocess
import logging
from datetime import datetime
import os
from logging.handlers import RotatingFileHandler

def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logger = logging.getLogger('Scheduler')
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler = RotatingFileHandler('logs/scheduler.log', maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def run_scripts(logger):
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Starting scheduled execution at {current_time}")
        
        logger.info("Starting SFTP transfer...")
        result_sftp = subprocess.run(["python3", "sftp_script.py"], capture_output=True, text=True)
        logger.info(f"SFTP Output: {result_sftp.stdout}")
        if result_sftp.stderr:
            logger.error(f"SFTP Errors: {result_sftp.stderr}")
            
        logger.info("Starting SQL import...")
        result_sql = subprocess.run(["python3", "sql_import.py"], capture_output=True, text=True)
        logger.info(f"SQL Import Output: {result_sql.stdout}")
        if result_sql.stderr:
            logger.error(f"SQL Import Errors: {result_sql.stderr}")
            
        logger.info("Both scripts completed successfully")
    except Exception as e:
        logger.error(f"Error in run_scripts: {e}")

def main():
    logger = setup_logging()
    logger.info("Scheduler started")
    
    # Schedule the job to run at 16:05
    schedule.every().day.at("02:00").do(run_scripts, logger)
    
    # Check if current time is close to 16:05
    current_hour = datetime.now().hour
    current_minute = datetime.now().minute
    if current_hour == 2 and current_minute == 0:
        logger.info("Current time matches scheduled time, running immediately...")
        run_scripts(logger)
    
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()