import psycopg2
import pandas as pd


def create_database():
    # connect to the database
    # "host=127.0.0.1 dbname=studentdb user=student password=student"
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=myfirstdb user=postgres password=2210"
    )
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    # create sparkify database with utf-8 encoding
    cur.execute("DROP database if exists medical")
    cur.execute("create database medical")

    # close connection to default database
    conn.close()

    # connect to sparkfy database
    conn = psycopg2.connect("host=127.0.0.1 dbname=medical user=postgres password=2210")
    cur = conn.cursor()

    return conn, cur


# Bringing the date from 3 csv files, to create their respective tables
# FACT TABLE
# "FactTablePK", "dimPatientPK","dimPhysicianPK",  "GrossCharge", "AR"
fact_table = pd.read_csv("create-tables\data\FactTable.csv")
fact_table_clean = fact_table[
    ["FactTablePK", "dimPatientPK", "dimPhysicianPK", "GrossCharge", "AR"]
]
# PHYSICIAN TABLE
# dimPhysicianPK, ProviderNpi, ProviderName, ProviderSpecialty, ProviderFTE
physician_table_clean = pd.read_csv("create-tables\data\DimPhyscian.csv")
# PATIENT TABLE
# 'dimPatientPK','PatientNumber','FirstName','LastName','Email','PatientGender','PatientAge','City'
patient_table = pd.read_csv("create-tables\data\Dimpatient.csv")
patient_table_clean = patient_table[
    [
        "dimPatientPK",
        "PatientNumber",
        "FirstName",
        "LastName",
        "Email",
        "PatientGender",
        "PatientAge",
        "City",
    ]
]
# DB connection
conn, cur = create_database()

# set automatic commit  to be true so that each action is committed without having to call conn.commit() after each command
# conn.set_session(autocommit=True)

## defining tables
fact_table_create = """ create table if not exists fact(
                    FactTablePK int primary key,
                    dimPatientPK int,
                    dimPhysicianPK int,  
                    GrossCharge int, 
                    AR int
)"""
cur.execute(fact_table_create)
conn.commit()

patient_table_create = """ create table if not exists patient(
                    dimPatientPK int primary key,
                    PatientNumber int,
                    FirstName varchar,
                    LastName varchar,
                    Email varchar,
                    PatientGender varchar,
                    PatientAge int,
                    City varchar
)"""
cur.execute(patient_table_create)
conn.commit()

physician_table_create = """ create table if not exists physician(
                    dimPhysicianPK int primary key, 
                    ProviderNpi int, 
                    ProviderName varchar, 
                    ProviderSpecialty varchar, 
                    ProviderFTE numeric
)"""
cur.execute(physician_table_create)
conn.commit()

# inserting data
fact_table_insert = """insert into fact(
    FactTablePK,
    dimPatientPK,
    dimPhysicianPK,  
    GrossCharge, 
    AR)
values (%s,%s,%s,%s,%s)
"""
for i, row in fact_table_clean.iterrows():
    cur.execute(fact_table_insert, list(row))
conn.commit()

patient_table_insert = """insert into patient(
    dimPatientPK,
    PatientNumber,
    FirstName,
    LastName,
    Email,
    PatientGender,
    PatientAge,
    City)
values (%s,%s,%s,%s,%s,%s,%s,%s)
"""
for i, row in patient_table_clean.iterrows():
    cur.execute(patient_table_insert, list(row))
conn.commit()


physician_table_insert = """insert into physician(
    dimPhysicianPK, 
    ProviderNpi, 
    ProviderName, 
    ProviderSpecialty, 
    ProviderFTE)
values (%s,%s,%s,%s,%s)
"""
for i, row in physician_table_clean.iterrows():
    cur.execute(physician_table_insert, list(row))
conn.commit()

cur.close()
conn.close()
