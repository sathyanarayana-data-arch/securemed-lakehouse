-- Create Database for SecureMed Analytics
CREATE DATABASE IF NOT EXISTS securemed_db;

-- Create Partition-Projected Table for Silver Patients
CREATE EXTERNAL TABLE IF NOT EXISTS securemed_db.silver_patients (
    patient_id string,
    ssn_last4 string,
    date_of_birth string,
    gender string,
    city string,
    updated_at string,
    full_name_masked string
)
PARTITIONED BY (
    year string,
    month string,
    day string
)
STORED AS PARQUET
LOCATION 's3://securemed-lakehouse-prod/silver/patients/'
TBLPROPERTIES (
    "parquet.compression"="SNAPPY",
    "projection.enabled" = "true",
    "projection.year.type" = "integer",
    "projection.year.range" = "2025,2030",
    "projection.month.type" = "integer",
    "projection.month.range" = "1,12",
    "projection.day.type" = "integer",
    "projection.day.range" = "1,31",
    "storage.location.template" = "s3://securemed-lakehouse-prod/silver/patients/year=${year}/month=${month}/day=${day}/"
);

-- Analytical Query: Patient Distribution by City & Gender via Athena
SELECT
    city,
    gender,
    COUNT(patient_id) AS total_patients
FROM securemed_db.silver_patients
WHERE year = '2026' AND month = '11'
GROUP BY city, gender
ORDER BY total_patients DESC;