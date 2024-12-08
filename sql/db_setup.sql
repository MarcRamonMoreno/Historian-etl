-- Enable advanced options for configuration
sp_configure 'show advanced options', 1;
GO
RECONFIGURE;
GO

-- Enable contained database authentication
sp_configure 'contained database authentication', 1;
GO
RECONFIGURE;
GO

-- Set variables for configuration
:connect localhost -U SA -P YourPassword -C TrustServerCertificate=yes

-- Create the database
CREATE DATABASE HistorianData
CONTAINMENT = NONE
COLLATE Latin1_General_CI_AS;
GO

-- Use the database
USE HistorianData;
GO

-- Create TagData table
CREATE TABLE TagData (
    ID BIGINT IDENTITY(1,1) PRIMARY KEY,
    TagName NVARCHAR(255) NOT NULL,
    Timestamp DATETIME2(3) NOT NULL,  -- 3 decimal places for milliseconds
    Value FLOAT NOT NULL,
    ImportDate DATETIME2(3) DEFAULT GETDATE(),
    Status TINYINT DEFAULT 0,  -- 0: Normal, 1: Suspect, 2: Error
    Quality NVARCHAR(50)      -- Additional quality information
);
GO

-- Create indices for better performance
CREATE NONCLUSTERED INDEX IX_TagData_TagName_Timestamp 
ON TagData(TagName, Timestamp)
INCLUDE (Value, Status, Quality);
GO

CREATE NONCLUSTERED INDEX IX_TagData_ImportDate
ON TagData(ImportDate);
GO

-- Create statistics for query optimization
CREATE STATISTICS ST_TagData_TagName_Timestamp 
ON TagData(TagName, Timestamp);
GO

-- Create user-defined functions for data analysis
CREATE FUNCTION dbo.GetTagAverage
(
    @TagName NVARCHAR(255),
    @StartTime DATETIME2,
    @EndTime DATETIME2
)
RETURNS FLOAT
AS
BEGIN
    DECLARE @Result FLOAT;
    
    SELECT @Result = AVG(Value)
    FROM TagData
    WHERE TagName = @TagName
    AND Timestamp BETWEEN @StartTime AND @EndTime
    AND Status = 0;  -- Only consider normal values
    
    RETURN @Result;
END;
GO

-- Create stored procedures for common operations
CREATE PROCEDURE dbo.GetTagDataInRange
    @TagName NVARCHAR(255),
    @StartTime DATETIME2,
    @EndTime DATETIME2,
    @Interval INT = 60  -- Default interval in seconds
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        TagName,
        DATEADD(SECOND, 
                DATEDIFF(SECOND, '2000-01-01', Timestamp) / @Interval * @Interval,
                '2000-01-01') as IntervalStart,
        AVG(Value) as AverageValue,
        MIN(Value) as MinValue,
        MAX(Value) as MaxValue,
        COUNT(*) as SampleCount
    FROM TagData
    WHERE TagName = @TagName
    AND Timestamp BETWEEN @StartTime AND @EndTime
    AND Status = 0
    GROUP BY 
        TagName,
        DATEADD(SECOND, 
                DATEDIFF(SECOND, '2000-01-01', Timestamp) / @Interval * @Interval,
                '2000-01-01')
    ORDER BY IntervalStart;
END;
GO