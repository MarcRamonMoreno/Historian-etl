# Historian Data Transfer System

An automated ETL pipeline for transferring and processing industrial historian data. This system securely transfers time-series data from a Windows-based historian server to a SQL Server database, featuring automated scheduling, data validation, and robust error handling.

## Features

- Automated SFTP file transfer from Windows historian server
- Efficient SQL Server batch importing
- Intelligent data merging with duplicate prevention
- Daily scheduled execution with error recovery
- Comprehensive logging system
- Docker containerization

## System Architecture

```
Windows Historian Server
        ↓ SFTP
   Transfer Script
        ↓
   Local Storage
        ↓
   SQL Import Script
        ↓
   SQL Server Database
```

## Prerequisites

- Docker and Docker Compose
- SQL Server with ODBC Driver 18
- Python 3.x (for development)
- Network access to historian server
- Sufficient storage for data files

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/historian-transfer-system.git
cd historian-transfer-system
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configurations
```

3. Create necessary directories:
```bash
mkdir -p logs historian_exports/temp
```

4. Initialize the SQL Server database:
```sql
-- Run the following SQL script
```

5. Start the system:
```bash
docker-compose up -d
```

:connect localhost -U SA -P YourPassword -C TrustServerCertificate=yes

CREATE DATABASE HistorianData;
GO

USE HistorianData;
GO

-- Create tables for historian data
CREATE TABLE TagData (
    ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TagName NVARCHAR(255),
    Timestamp DATETIME2,
    Value FLOAT,
    ImportDate DATETIME2 DEFAULT GETDATE()
);
GO

-- Create index for better query performance
CREATE INDEX IX_TagData_TagName_Timestamp ON TagData(TagName, Timestamp);
GO

-- Create user and grant permissions
CREATE LOGIN historian_user WITH PASSWORD = 'StrongPassword123!';
GO

CREATE USER historian_user FOR LOGIN historian_user;
GO

ALTER ROLE db_owner ADD MEMBER historian_user;
GO
```

## Components

### SFTP Transfer Script (sftp_script.py)
- Securely transfers files from Windows historian server
- Implements file locking mechanism
- Handles data merging and deduplication
- Maintains data retention policy

### SQL Import Script (sql_import.py)
- Efficient batch importing to SQL Server
- Handles large datasets with memory management
- Implements retry logic and error handling
- Prevents duplicate imports

### Scheduler (scheduler.py)
- Manages daily execution of transfer and import scripts
- Implements logging and monitoring
- Handles execution errors and retries
- Configurable schedule times

## Configuration

Key configuration files:
- `.env`: Environment variables and credentials
- `docker-compose.yml`: Container configuration
- `requirements.txt`: Python dependencies

## Logging

The system maintains rotating logs for:
- File transfers
- SQL imports
- Scheduler execution
- Error tracking

Logs are stored in the `logs` directory with a 5-file rotation policy.

## Development

For local development:

1. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run individual scripts:
```bash
python sftp_script.py
python sql_import.py
python scheduler.py
```

## Maintenance

Regular maintenance tasks:
- Monitor log files for errors
- Verify data integrity
- Check disk space usage
- Update security credentials

## Security Notes

- Uses SFTP for secure file transfer
- Implements file locking to prevent concurrent access
- Credentials stored in environment variables
- Network access controlled via firewall rules

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.