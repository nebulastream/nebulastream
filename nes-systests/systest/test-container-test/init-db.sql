-- Wait a moment for SQL Server to be fully ready
WAITFOR DELAY '00:00:05';

-- Create the v_Labor_null table
CREATE TABLE dbo.v_Labor_null
(
    LabVal_ID         INT,
    mlife_FallNr      INT,
    Zeitpunkt         SMALLDATETIME,
    GruppenNr         INT,
    Gruppe            INT,
    Bez               NVARCHAR(50),
    Wert              VARCHAR(256),
    Einheit           VARCHAR(256),
    Wertebereich      VARCHAR(256),
    SampleNr          VARCHAR(256),
    TSFail            SMALLDATETIME,
    TSLog             SMALLDATETIME,
    Lab_ID            INT
);

-- Create the v_Vitalwerte_unvalidiert table
CREATE TABLE dbo.v_Vitalwerte_unvalidiert
(
    mlife_ANR    INT,
    mlife_FallNr SMALLINT,
    Herzfrequenz REAL,
    BPdias       REAL,
    BPM          VARCHAR(8),
    BPsys        CHAR,
    Temp         REAL,
    ZVD          BIT,
    dPAP         REAL,
    mPAP         REAL,
    sPAP         REAL,
    dNiB         REAL,
    mNiB         REAL,
    sNiB         REAL,
    SPO2         REAL,
    PulsRate     REAL,
    LAP          REAL,
    ZentTemp     TINYINT,
    BIS          BIGINT,
    HZV          FLOAT
);

-- Insert dummy data into v_Labor_null
INSERT INTO dbo.v_Labor_null (LabVal_ID, mlife_FallNr, Zeitpunkt, GruppenNr, Gruppe, Bez, Wert, Einheit, Wertebereich, SampleNr, TSFail, TSLog, Lab_ID)
VALUES
    (1, 1001, '2024-01-15 08:30', 1, 5, 'Hämoglobin', '14.5', 'g/dL', '12-16', 'S001', '2024-01-15 08:00', '2024-01-15 09:00', 101),
    (2, 1001, '2024-01-15 08:30', 1, 5, 'Leukozyten', '7.2', '10^9/L', '4-10', 'S001', '2024-01-15 08:00', '2024-01-15 09:00', 101),
    (3, 1002, '2024-01-15 09:15', 2, 5, 'Glucose', '95', 'mg/dL', '70-110', 'S002', '2024-01-15 08:45', '2024-01-15 09:30', 102),
    (4, 1002, '2024-01-15 09:15', 2, 5, 'Kreatinin', '?.9', 'mg/dL', '0.6-1.2', 'S002', '2024-01-15 08:45', '2024-01-15 09:30', 102),
    (5, 1003, '2024-01-15 10:00', 1, 5, 'Thrombozyten', 'K', '10^9/L', '150-400', 'S003', '2024-01-15 09:30', '2024-01-15 10:15', 103);

-- Insert dummy data into v_Vitalwerte_unvalidiert
INSERT INTO dbo.v_Vitalwerte_unvalidiert (mlife_ANR, mlife_FallNr, Herzfrequenz, BPdias, BPM, BPsys, Temp, ZVD, dPAP, mPAP, sPAP, dNiB, mNiB, sNiB, SPO2, PulsRate, LAP, ZentTemp, BIS, HZV)
VALUES
    (2001, 1001, 72.5, 75.0, '120/75', '1', 36.8, 0, 15.0, 25.0, 35.0, 70.0, 85.0, 115.0, 98.5, 72.0, 8.0, 37, 45, 5.2),
    (2002, 1002, 68.0, 70.0, '115/70', '1', 37.1, 1, 14.0, 24.0, 34.0, 68.0, 82.0, 112.0, 99.0, 68.0, 7.5, 37, 48, 5.5),
    (2003, 1003, 80.5, 80.0, '130/80', '1', 36.5, 0, 16.0, 26.0, 36.0, 75.0, 90.0, 125.0, 97.0, 80.0, 9.0, 36, 42, 4.8);

PRINT 'Database initialization completed successfully!';

