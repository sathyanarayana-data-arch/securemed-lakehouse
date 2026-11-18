-- 1. Operational Source Tables (Healthcare Domain)
CREATE SCHEMA IF NOT EXISTS clinical;

CREATE TABLE IF NOT EXISTS clinical.patients (
    patient_id UUID PRIMARY KEY,
    full_name VARCHAR(150) NOT NULL,
    ssn_last4 CHAR(4) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(20),
    city VARCHAR(100),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clinical.encounters (
    encounter_id UUID PRIMARY KEY,
    patient_id UUID REFERENCES clinical.patients(patient_id),
    department VARCHAR(100) NOT NULL,
    diagnosis_code VARCHAR(20) NOT NULL,
    claim_amount NUMERIC(10, 2) NOT NULL,
    attending_physician VARCHAR(150),
    encounter_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Metadata, Lineage & Incremental Watermark Tables
CREATE SCHEMA IF NOT EXISTS etl_meta;

CREATE TABLE IF NOT EXISTS etl_meta.pipeline_watermarks (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    last_watermark_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_meta.pipeline_audit_log (
    run_id UUID PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    stage VARCHAR(50) NOT NULL,
    source_row_count INT NOT NULL,
    target_row_count INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    target_s3_uri TEXT,
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Initialize Default Watermarks
INSERT INTO etl_meta.pipeline_watermarks (pipeline_name, source_table, last_watermark_ts)
VALUES 
    ('ingest_patients', 'clinical.patients', '1970-01-01 00:00:00+00'),
    ('ingest_encounters', 'clinical.encounters', '1970-01-01 00:00:00+00')
ON CONFLICT (pipeline_name) DO NOTHING;

-- Seed Realistic Synthetic Records
INSERT INTO clinical.patients (patient_id, full_name, ssn_last4, date_of_birth, gender, city, updated_at)
VALUES
    ('11111111-1111-1111-1111-111111111111', 'Aarav Sharma', '4821', '1985-04-12', 'M', 'Bengaluru', '2026-11-15 09:00:00+00'),
    ('22222222-2222-2222-2222-222222222222', 'Meera Nair', '9034', '1992-08-23', 'F', 'Mumbai', '2026-11-16 11:30:00+00'),
    ('33333333-3333-3333-3333-333333333333', 'Rohan Verma', '7712', '1978-12-05', 'M', 'Delhi', '2026-11-17 14:15:00+00')
ON CONFLICT (patient_id) DO NOTHING;

INSERT INTO clinical.encounters (encounter_id, patient_id, department, diagnosis_code, claim_amount, attending_physician, encounter_ts, updated_at)
VALUES
    ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '11111111-1111-1111-1111-111111111111', 'Cardiology', 'I10', 1450.00, 'Dr. Kavita Rao', '2026-11-15 10:15:00+00', '2026-11-15 10:30:00+00'),
    ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', '22222222-2222-2222-2222-222222222222', 'Orthopedics', 'M54.5', 820.50, 'Dr. Vikram Seth', '2026-11-16 12:00:00+00', '2026-11-16 12:20:00+00'),
    ('cccccccc-cccc-cccc-cccc-cccccccccccc', '33333333-3333-3333-3333-333333333333', 'Neurology', 'G43.9', 2100.75, 'Dr. Ananya Sen', '2026-11-17 15:00:00+00', '2026-11-17 15:45:00+00')
ON CONFLICT (encounter_id) DO NOTHING;
