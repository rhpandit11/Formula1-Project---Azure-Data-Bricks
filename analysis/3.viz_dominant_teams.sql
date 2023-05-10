-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points,
        RANK() OVER(ORDER BY avg(calculated_points) DESC) team_rank
 from f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1) >= 100
ORDER BY total_points DESC

-- COMMAND ----------

SELECT race_year, team_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
 from f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams where team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY  race_year, total_points DESC

-- COMMAND ----------

SELECT race_year, team_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
 from f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams where team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY  race_year, total_points DESC

-- COMMAND ----------

SELECT race_year, team_name,
       count(1) AS total_races,
        sum(calculated_points) AS total_points,
        avg(calculated_points) AS avg_points
 from f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams where team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY  race_year, total_points DESC