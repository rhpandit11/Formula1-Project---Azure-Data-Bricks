-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlpj/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formula1dlpj/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create tables for JSON Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create constructor table
-- MAGIC . Single Line JSON
-- MAGIC . Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING)
  USING JSON
  OPTIONS(path "/mnt/formula1dlpj/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create drivers table
-- MAGIC . Single Line JSON
-- MAGIC . complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING)
  USING JSON
  OPTIONS(path "/mnt/formula1dlpj/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create results table
-- MAGIC   Single Line JSON
-- MAGIC   Simple structute

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT, grid INT,
  position INT,
  positionText STRING,
  PositionOrder INT,
  points INT,
  laps INT,
  time STRING,
  miliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING)
USING json
OPTIONS(path "/mnt/formula1dlpj/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create pit stops table
-- MAGIC 1.Multi Line JSON
-- MAGIC 2.SImple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING)
USING JSON
OPTIONS(path "/mnt/formula1dlpj/raw/pit_stops.json", multiLine true)  

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Lap Times Table
-- MAGIC  1. CSV File
-- MAGIC  2. Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1dlpj/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Qualifying Table
-- MAGIC   1. Json File
-- MAGIC   2. MultiLine JSON
-- MAGIC   3. Multiple Files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId INT,
  driverId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyID INT,
  raceId INT)
USING json
OPTIONS (path "/mnt/formula1dlpj/raw/qualifying", multiline true)

-- COMMAND ----------

select * from f1_raw.qualifying