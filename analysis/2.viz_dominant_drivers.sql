-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style = "color:Black;text-align:center;font-family:Ariel">Report on Dominant Fromula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points,
        RANK() OVER(ORDER BY avg(calculated_points) DESC) driver_rank
 from f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY total_points DESC

-- COMMAND ----------

SELECT race_year, driver_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
 from f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY  race_year, total_points DESC

-- COMMAND ----------

SELECT race_year, driver_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
 from f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY  race_year, total_points DESC

-- COMMAND ----------

SELECT race_year, driver_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
 from f1_presentation.calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers where driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY  race_year, total_points DESC