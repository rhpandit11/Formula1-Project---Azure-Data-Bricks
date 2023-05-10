-- Databricks notebook source
SELECT team_name, 
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1) >= 100
ORDER BY total_points DESC

-- COMMAND ----------

SELECT team_name, 
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING count(1) >= 100
ORDER BY total_points DESC

-- COMMAND ----------

SELECT team_name, 
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
from f1_presentation.calculated_race_results
where race_year BETWEEN 2001 AND 2011
GROUP BY team_name
HAVING count(1) >= 100
ORDER BY total_points DESC