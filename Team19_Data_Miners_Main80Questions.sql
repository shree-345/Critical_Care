--1.Write a query to count the number of columns in the nursing chart table.
SELECT COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'nursingchart';

--2.Using a recursive query, show a list of patients that were transferred to various departments after they were admitted, 
--3.List all patients who had a systolic blood pressure higher than the median value in the ICU
WITH Median_BP AS (
    SELECT 
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY invasive_sbp) AS median_bp
    FROM 
        nursingchart
    JOIN
        baseline ON nursingchart.inp_no = baseline.inp_no
    WHERE
        admitdept = 'ICU'
)
SELECT 
    nursingchart.inp_no,
    nursingchart.invasive_sbp
FROM 
    nursingchart
JOIN 
    baseline ON nursingchart.inp_no = baseline.inp_no
WHERE 
    admitdept = 'ICU'
    AND nursingchart.invasive_sbp > (SELECT median_bp FROM Median_BP);

--4.Create a function to fetch the details of the last recorded drug for a patient.
CREATE OR REPLACE FUNCTION get_last_recorded_patient_drug(p_patient_id BIGINT)
RETURNS TABLE (
    patient_id BIGINT,
    drug_name text,
    administered_time TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        drugs.patient_id,
        drugname AS drug_name,
        drug_time AS administered_time
    FROM
        drugs
    JOIN
        baseline ON drugs.patient_id = baseline.patient_id
    WHERE
        drugs.patient_id =p_patient_id
    ORDER BY
        drug_time DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

select * from get_last_recorded_patient_drug(6039999)

--5.List the 5 most recent transfers.
SELECT *
FROM transfer
ORDER BY stoptime DESC
LIMIT 5;


--6.Use a window function to calculate the rolling average of heart rate for each patient.
SELECT 
inp_no,
    heart_rate,
    AVG(heart_rate) OVER (PARTITION BY inp_no ORDER BY inp_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rolling_avg_4
FROM 
    nursingchart;

--7.List patients who were transferred back into surgery after they were discharged.
SELECT DISTINCT baseline.patient_id, baseline.inp_no,admitdept
FROM baseline
JOIN transfer ON baseline.admitdept = transfer.transferdept
JOIN icd  ON transfer.patient_id = icd.patient_id
WHERE status_discharge IS NOT NULL
  AND admitdept = 'Surgery'
  AND icu_discharge_time < starttime
ORDER BY admitdept;

--8.Find the average age in each department by gender.
SELECT admitdept,sex,
AVG(age)
FROM baseline
GROUP BY baseline.admitdept,baseline.sex;


--9.Show all patients whose blood sugar is in the 99th percentile, and the time when it was recorded.
WITH Percentile AS (SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY blood_sugar) AS percentile_99
 FROM nursingchart)
SELECT inp_no,blood_sugar,charttime
FROM nursingchart 
JOIN Percentile 
ON blood_sugar >= percentile_99
ORDER BY inp_no,charttime;

--10.Show the last 6 letters of disease names.
SELECT RIGHT(icd_desc, 6)AS
last_6_letters
FROM icd;

--11.Show the most commonly administered drug for each department, and the number of times it was administered. 
WITH drug_counts AS (
    SELECT
        admitdept,
        drugname,
        COUNT(*) AS drug_count
    FROM baseline
    JOIN drugs  ON baseline.patient_id = drugs.patient_id
    GROUP BY admitdept, drugname),

 ranked_drugs AS (
   SELECT
        admitdept,
        drugname,
        drug_count,
        RANK() OVER (PARTITION BY admitdept ORDER BY drug_count DESC) AS rank
    FROM drug_counts)
SELECT
    admitdept,
     drugname,
    drug_count
FROM ranked_drugs 
WHERE rank = 1
ORDER BY admitdept;

--12.Show the position of the letter y in disease name if it exists.
SELECT POSITION ('y' IN icd_desc)
FROM icd;

--13.Using windows function rank and display the 3 oldest patients admitted into each department.
WITH RankedPatients AS (
    SELECT 
        patient_id,
        admitdept,
        RANK() OVER (PARTITION BY admitdept ORDER BY age DESC) AS age_rank
    FROM baseline
)
SELECT 
    patient_id,
    admitdept,
    age_rank
FROM RankedPatients
WHERE age_rank <= 3
ORDER BY admitdept, age_rank;


--14.Show the number of patients that were discharged in 2020.
SELECT COUNT(*) AS number_patients_discharged_in_2020
FROM baseline
WHERE EXTRACT (YEAR FROM icu_discharge_time)= 2020

--15.Show the total ICU stay in days for each patient who was transferred at least once.
SELECT 
    Patient_id,
    SUM(
        DATE("stoptime") - DATE("starttime")
    ) AS total_icu_stay_days
FROM 
    transfer
GROUP BY 
    Patient_id
HAVING 
    COUNT(*) > 0; 


--16.Find the average, minimum, and maximum systolic blood pressure for patients in each department.

SELECT 
    AVG(nursingchart.invasive_sbp) AS avg_sbp,
    MIN(nursingchart.invasive_sbp) AS min_sbp,
    MAX(nursingchart.invasive_sbp) AS max_sbp, 
    baseline.admitdept
FROM 
    baseline 
JOIN 
    nursingchart ON baseline.inp_no = nursingchart.inp_no
GROUP BY 
    baseline.admitdept;

--Q17. Write a stored procedure to calculate the total number of patients per department and return the results as a table.

CREATE OR REPLACE PROCEDURE calculate_patient_count_per_department()
LANGUAGE plpgsql
AS $$
BEGIN
   DROP TABLE IF EXISTS temp_patient_count_per_department;
    CREATE TEMP TABLE temp_patient_count_per_department (
        department TEXT, total_patients BIGINT
    );

    INSERT INTO temp_patient_count_per_department
    SELECT admitdept AS department,
        COUNT(DISTINCT patient_id) AS total_patients
    FROM baseline
    GROUP BY admitdept;
    -- raise a notice
    RAISE NOTICE 'Patient count per department calculated successfully.';
END;
$$;
CALL calculate_patient_count_per_department();
SELECT * FROM temp_patient_count_per_department;

--Q18. Show the top 3 patients who went into surgery the most number of times

SELECT patient_id,COUNT(DISTINCT DATE(starttime)) AS unique_surgery_count
FROM transfer
WHERE transferdept = 'Surgery'
GROUP BY patient_id
ORDER BY unique_surgery_count DESC
LIMIT 3;

--Q19. Show patients whose critical-care pain observation tool score is 0.

SELECT DISTINCT inp_no, cpot_pain_score 
FROM nursingchart
WHERE cpot_pain_score = '0';

--Q20. "Use windows functions to find BP measurements for 3 consecutive days. 
--List all patients who experienced a drop in blood pressure measurements for 3 continuous days."

CREATE INDEX idx_baseline_inp_no ON baseline(inp_no);
CREATE INDEX idx_baseline_patient_id ON baseline(patient_id);
CREATE INDEX idx_outcome_patient_id ON outcome(patient_id);

WITH ranked_bp AS (
    SELECT inp_no,
        blood_pressure_high AS systolic_bp,
        blood_pressure_low AS diastolic_bp,
        DATE(charttime) AS measurement_date,
        ROW_NUMBER() OVER (PARTITION BY inp_no ORDER BY DATE(charttime)) AS row_num
    FROM nursingchart  -- Replace with your table name
    WHERE blood_pressure_high IS NOT NULL AND blood_pressure_low IS NOT NULL
),
bp_trends AS (
    SELECT r1.inp_no,
        r1.measurement_date AS day1_date,
        r1.systolic_bp AS day1_systolic,
        r1.diastolic_bp AS day1_diastolic,
        r2.measurement_date AS day2_date,
        r2.systolic_bp AS day2_systolic,
        r2.diastolic_bp AS day2_diastolic,
        r3.measurement_date AS day3_date,
        r3.systolic_bp AS day3_systolic,
        r3.diastolic_bp AS day3_diastolic
    FROM ranked_bp r1
    JOIN ranked_bp r2 ON r1.inp_no = r2.inp_no AND r2.row_num = r1.row_num + 1
    JOIN ranked_bp r3 ON r1.inp_no = r3.inp_no AND r3.row_num = r1.row_num + 2
),
drops AS (
    SELECT inp_no,
        day1_date, day1_systolic, day1_diastolic,
        day2_date, day2_systolic, day2_diastolic,
        day3_date, day3_systolic, day3_diastolic
    FROM bp_trends
    WHERE 
        day1_systolic > day2_systolic AND day2_systolic > day3_systolic
        AND day1_diastolic > day2_diastolic AND day2_diastolic > day3_diastolic
)
SELECT inp_no,
    day1_date, day1_systolic, day1_diastolic,
    day2_date, day2_systolic, day2_diastolic,
    day3_date, day3_systolic, day3_diastolic
FROM drops;

--Q21. How was general health of patients who had a breathing rate > 20?

SELECT oc.sf36_generalhealth AS general_health, COUNT(*) AS num_patients
FROM nursingchart n
JOIN baseline b ON n.inp_no = b.inp_no
JOIN outcome oc ON b.patient_id = oc.patient_id
WHERE n.breathing > 20
GROUP BY oc.sf36_generalhealth;
	
-- Q22. List patients with heart_rate more than two standard deviations from the average.
WITH heart_rate_stats AS (
    SELECT AVG(heart_rate) AS avg_heart_rate,
        STDDEV(heart_rate) AS stddev_heart_rate
    FROM nursingchart
    WHERE heart_rate IS NOT NULL
)
SELECT n.inp_no,
    n.heart_rate,
    stats.stddev_heart_rate AS Twostandarddeviations
FROM nursingchart n
CROSS JOIN heart_rate_stats stats
WHERE n.heart_rate > stats.avg_heart_rate + 2 * stats.stddev_heart_rate
    OR n.heart_rate < stats.avg_heart_rate - 2 * stats.stddev_heart_rate;

-- Q23. Create a trigger to raise notice and prevent deletion of a record from baseline table.

CREATE OR REPLACE FUNCTION prevent_baseline_deletion()
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'Deletion of records from the baseline table is not allowed.';
   
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER no_delete_on_baseline
BEFORE DELETE ON baseline
FOR EACH ROW
EXECUTE FUNCTION prevent_baseline_deletion();

DELETE FROM baseline WHERE patient_id = 6291268;


--Q24. Use a CTE to get all patients with temperature readings above 38.
WITH high_temp_patients AS (
    SELECT inp_no, temperature
    FROM nursingchart
    WHERE temperature > 38)
SELECT ht.inp_no, b.patient_id, ht.temperature
FROM high_temp_patients ht
JOIN baseline b ON ht.inp_no = b.inp_no;
	

-- Q25. Develop a stored procedure to insert a new patient into the patients table and return the new patient ID.
DROP FUNCTION IF EXISTS insert_new_patient(bigint, integer, varchar);

CREATE OR REPLACE FUNCTION insert_new_patient(
    p_patient_id BIGINT,
    p_age INT,
    p_sex VARCHAR) 
RETURNS BIGINT AS $$
DECLARE new_patient_id BIGINT;
BEGIN
    -- Insert the new patient record into the baseline table
    INSERT INTO baseline (patient_id, age, sex)
    VALUES (p_patient_id, p_age, p_sex)
    RETURNING patient_id INTO new_patient_id;
    -- Return the new patient ID
    RETURN new_patient_id;
END;
$$ LANGUAGE plpgsql;

SELECT insert_new_patient(6130246, 30, 'Male');

--Q26. Find a correlation between blood sugar levels and discharge time from ICU.

SELECT CORR(nc.blood_sugar, EXTRACT(EPOCH FROM b.icu_discharge_time)) AS correlation
FROM nursingchart nc
JOIN baseline b ON nc.inp_no = b.inp_no
WHERE nc.blood_sugar IS NOT NULL
  AND b.icu_discharge_time IS NOT NULL;
  
--Q.27 Divide the patients into 3 age groups.

SELECT patient_id, age,
    CASE
        WHEN age <= 40 THEN 'Young Adults (0-40)'
        WHEN age BETWEEN 41 AND 80 THEN 'Middle-Aged (41-80)'
        WHEN age > 80 THEN 'Older Adults (81-120)'
        ELSE 'Unknown'
    END AS age_group
FROM baseline

--Q28. Show the hour(as a time slot like 9 AM - 10 AM) when least discharges happen

SELECT 
    CONCAT(EXTRACT(HOUR FROM icu_discharge_time), '-', EXTRACT(HOUR FROM icu_discharge_time) + 1) AS time_slot,
    COUNT(*) AS discharge_count
FROM baseline
WHERE icu_discharge_time IS NOT NULL
GROUP BY EXTRACT(HOUR FROM icu_discharge_time)
ORDER BY discharge_count ASC
LIMIT 1;

--Q29. Display 3 random patients who had UTI.

SELECT patient_id, infectionsite
FROM (
    SELECT DISTINCT patient_id, infectionsite
    FROM baseline
    WHERE infectionsite ILIKE '%UTI%'
) subquery
ORDER BY RANDOM()
LIMIT 3;


--Q30. List the average length of stay for patients diagnosed with soft tissue infections.

WITH patient_stay AS (
    -- length of stay for each patient
    SELECT b.patient_id,
        MIN(t.starttime) AS admission_time,
        MAX(t.stoptime) AS discharge_time,
        AGE(MAX(t.stoptime), MIN(t.starttime)) AS length_of_stay
    FROM baseline b
    JOIN transfer t ON b.patient_id = t.patient_id
    WHERE b.infectionsite = 'Soft Tissue'
    GROUP BY b.patient_id
),
average_length_of_stay AS (
    -- average length of stay
    SELECT AVG(EXTRACT(EPOCH FROM length_of_stay) / 3600) AS avg_length_of_stay_hours
    FROM patient_stay
)
SELECT avg_length_of_stay_hours
FROM average_length_of_stay;

--Q31. Create a table called Patient1952 to store all patients born in 1952 with their age and sex info.

Drop table Patient1952;
CREATE TABLE Patient1952 (
    patient_id SERIAL PRIMARY KEY, 
    inp_no INT, age INT, sex VARCHAR(10), icu_discharge_time DATE        
);

INSERT INTO Patient1952 (inp_no, age, sex, icu_discharge_time)
SELECT 
    b.inp_no, 
    EXTRACT(YEAR FROM b.icu_discharge_time) - 1952 AS calculated_age, 
    b.sex, b.icu_discharge_time
FROM baseline AS b
WHERE EXTRACT(YEAR FROM b.icu_discharge_time) - 1952 = b.age
    AND b.age IS NOT NULL; -- Ensure age column is not null
SELECT * FROM Patient1952

-- Q32. Give the highest temperature, and highest heart rate recorded of all the patients in surgery for each day

WITH SurgeryPatients AS (
    SELECT nc.inp_no, nc.temperature, nc.heart_rate, DATE(nc.charttime) AS record_date
    FROM nursingchart nc
    INNER JOIN transfer t
    ON nc.inp_no = t.inp_no
    WHERE t.transferdept = 'Surgery'
),
DailyMaxValues AS (
    SELECT record_date, MAX(temperature) AS highest_temperature, MAX(heart_rate) AS highest_heart_rate
    FROM SurgeryPatients
    GROUP BY record_date
)
SELECT record_date, highest_temperature, highest_heart_rate
FROM DailyMaxValues
ORDER BY record_date;

--33.List all patients whose heart rate increased by over 30% from the previous reading and the time when it happened.
--	List all occurences of heart rate increase. Use Windows functions to achieve this.

SELECT * FROM
	(SELECT 
	patient_id,
	heart_rate,
	charttime,
	LAG(heart_rate) OVER (PARTITION BY patient_id ORDER BY charttime) AS previous_heart_rate,
    CASE 
       WHEN LAG(heart_rate) OVER (PARTITION BY patient_id ORDER BY charttime) = 0 
            OR LAG(heart_rate) OVER (PARTITION BY patient_id ORDER BY charttime) IS NULL THEN NULL
       ELSE ((heart_rate - LAG(heart_rate) OVER (PARTITION BY patient_id ORDER BY charttime)) * 100.0 / 
            LAG(heart_rate) OVER (PARTITION BY patient_id ORDER BY charttime))
    END AS percent_increase
	FROM 
	nursingchart n, baseline b
	WHERE
	n.inp_no = b.inp_no) as temp_table
WHERE	
percent_increase>30;

--34.List patients who had milk and soft food but produced no urine.

SELECT DISTINCT patient_id
FROM 
baseline
WHERE inp_no IN
(SELECT DISTINCT n.inp_no
--,milk,soft_food,urine_volume
FROM
nursingchart n
WHERE
milk IS NOT NULL
AND
soft_food IS NOT NULL
AND (urine_volume IS NULL OR urine_volume = 0));

--35.Using crosstab, show number of times each patient was transferred to each department.

CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT * FROM CROSSTAB('SELECT patient_id,transferdept,count(*) FROM transfer 
GROUP BY patient_id,transferdept
ORDER BY patient_id,transferdept')
AS patient_transfer_details(patient_id bigint,ICU bigint,Medical_Specialties bigint,Surgery bigint);

--36."Produce a list of 100 normally distributed age values. Set the mean as the 3rd lowest age in the table, and assume the 
--standard deviation from the mean is 3."

CREATE EXTENSION IF NOT EXISTS tablefunc;

WITH mean_age AS (
    SELECT age AS mean
    FROM baseline
    ORDER BY age ASC
    LIMIT 1 OFFSET 2 ),
	normally_distributed_age AS(
	SELECT 
        ROUND(normal_rand(1, mean, 3)) AS age -- StdDev = 3
    FROM 
        generate_series(1, 100), -- Generate 100 values
        mean_age
	)
SELECT age
FROM normally_distributed_age
WHERE age > 0; -- Excluding negative ages
 
 
--Other way

WITH mean_age AS (
    SELECT age AS mean
    FROM baseline
    ORDER BY age ASC
    LIMIT 1 OFFSET 2 ),
	normally_distributed_age AS(
	SELECT 
       generate_series AS cnt,
       ROUND(mean + 3 * SQRT(-2 * LN(random())) * COS(2 * PI() * random())) AS age -- StdDev = 3
    FROM 
        generate_series(1, 100), -- Generate 100 values
        mean_age
	)
SELECT age
FROM normally_distributed_age
WHERE age > 0; -- Excluding negative ages

--37.Display the patients who engage in vigorous physical activity and have no body pain.

SELECT * FROM outcome 
WHERE
sf36_activitylimit_vigorousactivity IS NOT NULL
AND
sf36_pain_bodypainpast4wk ILIKE '%no%'
AND
sf36_pain_bodypainpast4wkinterhousework ILIKE '%no%';


--38.Create a view on outcome table to show patients with poor health.

CREATE OR REPLACE VIEW outcome_tab_view AS
SELECT * FROM outcome WHERE sf36_generalhealth = '5_Poor';

Validation:
SELECT * FROM outcome_tab_view;

--39.Create a procedure to check if a disease code exists.

CREATE OR REPLACE PROCEDURE check_disease_code_exists(icdCode varchar) AS
$$
DECLARE
is_present_disease_code boolean;
BEGIN

SELECT EXISTS( SELECT * FROM 
			  icd 
			  WHERE 
			  icd_code = icdCode) 
			  INTO is_present_disease_code;
			  
IF is_present_disease_code THEN
	RAISE NOTICE ' % Disease code is present', icdCode;
ELSE
	RAISE EXCEPTION ' % Disease code is not present',icdCode;
END IF;
END;
$$ LANGUAGE plpgsql;

CALL check_disease_code_exists('G93.812');
CALL check_disease_code_exists('111');

--40.Which drug was most administered among patients who have never been intubated?

SELECT drugname,COUNT(drugname) cnt FROM drugs WHERE patient_id IN
(SELECT DISTINCT b.patient_id 
FROM 
nursingchart n,baseline b
WHERE
n.inp_no=b.inp_no
AND
n.endotracheal_intubation IS NULL)
GROUP BY drugname
ORDER BY cnt DESC
LIMIT 1;


--41.Add a column birthyear to baseline column based on age.

ALTER TABLE baseline ADD COLUMN birthyear int;
UPDATE baseline SET birthyear = EXTRACT(year from current_date) - age;

validation:
SELECT * FROM baseline;

42.Use regular expression to find disease names that end in 'itis'.

SELECT icd_desc AS disease_name FROM icd WHERE icd_desc ~ 'itis$';
or
SELECT icd_desc AS disease_name FROM icd WHERE REGEXP_LIKE(icd_desc, 'itis$');

--43.Write a stored procedure to generate a summary report for a patient ID specified by user, including blood sugar, temperature, heart rate and drug administration.

CREATE OR REPLACE PROCEDURE patient_summary_report(patientid bigint) AS
$$
DECLARE
rec RECORD;
BEGIN

RAISE NOTICE 'Summary Report for Patient ID: %', patientid;

RAISE NOTICE 'Blood Sugar for Patient ID: %', patientid;
FOR rec IN
	select blood_sugar,charttime
	from
	nursingchart n,baseline b
	where
	n.inp_no = b.inp_no
	and 
	patient_id = patientid
	and
	blood_sugar is not null
LOOP
    RAISE NOTICE 'Blood Sugar: % ,Chart Time: % ', rec.blood_sugar,rec.charttime;
END LOOP;

RAISE NOTICE 'Temperature for Patient ID: %', patientid;
FOR rec IN
	select temperature,charttime
	from
	nursingchart n,baseline b
	where
	n.inp_no = b.inp_no
	and 
	patient_id = patientid
	and
	temperature is not null
LOOP
    RAISE NOTICE 'Temperature: % ,Chart Time: % ', rec.temperature,rec.charttime;
END LOOP;

RAISE NOTICE 'Heart_rate for Patient ID: %', patientid;
FOR rec IN
	select heart_rate,charttime
	from
	nursingchart n,baseline b
	where
	n.inp_no = b.inp_no
	and 
	patient_id = patientid
	and
	heart_rate is not null
LOOP
    RAISE NOTICE 'Heart Rate: % ,Chart Time: % ', rec.heart_rate,rec.charttime;
END LOOP;

RAISE NOTICE 'Drug administered for Patient ID: %', patientid;
FOR rec IN
	select patient_id,drugname,drug_time from drugs where patient_id = patientid
LOOP
    RAISE NOTICE 'Drugname: % , Time: % ', rec.drugname,rec.drug_time;
END LOOP;
			  
END;
$$ LANGUAGE plpgsql;

CALL patient_summary_report(5406548);

--or

CREATE OR REPLACE PROCEDURE patient_summary_report(patientid bigint) AS
$$
DECLARE
rec RECORD;
BEGIN

RAISE NOTICE 'Summary Report for Patient ID: %', patientid;

RAISE NOTICE 'Blood Sugar,temperature,heart rate,drug administered for Patient ID: %', patientid;
FOR rec IN
	select blood_sugar,temperature,heart_rate,charttime,drugname,drug_time
	from
	nursingchart n,baseline b,drugs d
	where
	n.inp_no = b.inp_no
	and 
	b.patient_id = d.patient_id
	and 
	b.patient_id = patientid
LOOP
    RAISE NOTICE 'Blood Sugar: % ,Temperature: % ,Heart Rate: % ,Chart Time: %,drugname: %,drug_time: % ', rec.blood_sugar,rec.temperature,rec.heart_rate,rec.charttime,rec.drugname,rec.drug_time;
END LOOP;
			  
END;
$$ LANGUAGE plpgsql;

CALL patient_summary_report(5406548);


--44.Create an index on any column in outcome table and also write a query to delete that index.

CREATE INDEX outcome_patientid_index on outcome(patient_id);

SELECT * FROM pg_indexes WHERE tablename = 'outcome';

DROP INDEX IF EXISTS outcome_patientid_index;


--45.Display the sf36_generalhealth of all patients whose blood sugar has a standard deviation of more than 2 from the average.

SELECT o.patient_id,o.sf36_generalhealth  
FROM outcome o,baseline b,nursingchart n
WHERE
o.patient_id = b.patient_id
AND
b.inp_no = n.inp_no
AND
ABS(blood_sugar - (SELECT AVG(blood_sugar) FROM nursingchart )) > 2 * (SELECT STDDEV(blood_sugar) FROM nursingchart);

--46.Show the average time spent across different departments among alive patients, and among dead patients.

SELECT 
    discharge_dept,
    TO_TIMESTAMP(AVG(CASE WHEN follow_vital = 'Alive' THEN COALESCE(EXTRACT(EPOCH FROM follow_date),0) END)) AS avg_time_stamp_alive_patient,
    TO_TIMESTAMP(AVG(CASE WHEN follow_vital = 'Death' THEN COALESCE(EXTRACT(EPOCH FROM follow_date),0) END)) AS avg_time_stamp_death_patient
FROM 
    outcome
GROUP BY 
    discharge_dept;


--47.Write a query to list all the users in the database.

SELECT * FROM pg_catalog.pg_user;

--48.For each patient, find their maximum blood oxygen saturation while they were in the ICU , and display if it is above or below the average value among all patients.

WITH 
max_blood_oxygen_saturation AS 
 (SELECT MAX(blood_oxygen_saturation) maximum_saturation,b.inp_no,b.patient_id FROM nursingchart n,baseline b 
 	WHERE n.inp_no=b.inp_no AND admitdept='ICU' GROUP BY b.inp_no,b.patient_id),
avg_blood_oxygen_saturation AS
 (SELECT AVG(maximum_saturation) avarage_saturation FROM max_blood_oxygen_saturation)
SELECT m.patient_id,
	   m.maximum_saturation,
	   CASE WHEN m.maximum_saturation > a.avarage_saturation THEN 'Above Avarage'
	   		WHEN m.maximum_saturation < a.avarage_saturation THEN 'Below Avarage'
	   END compare
FROM
max_blood_oxygen_saturation m,avg_blood_oxygen_saturation a;

--49. For each department, find the percentage of alive patients whose general health was poor after discharge
	select round(Count(b.patient_id)*100/sum(count(b.*)) over ()) as no_of_patients, b.admitdept as department,o.follow_vital,o.sf36_generalhealth as Health
	from baseline as b
	join 
	outcome as o			
	on 
	b.patient_id=o.patient_id
	where o.follow_vital ='Alive' and o.sf36_generalhealth='Poor'
	group by b.admitdept, o.follow_vital, o.sf36_generalhealth
	
select count(b.patient_id), o.follow_vital,o.sf36_generalhealth from baseline as b
	join 
	outcome as o
	on 
	b.patient_id=o.patient_id
	where o.follow_vital ='Alive' and o.sf36_generalhealth='Poor'
	group by b.admitdept, o.follow_vital, o.sf36_generalhealth
	
	/*count(b.patient_id)*100.0/count(b.*) as no_of_patients*/

--50.Write a function that takes a date and returns the average temperature recorded for that day.

/*select distinct charttime from nursingchart
group by charttime

select avg(temperature) as temp_sumfrom nursingchart
 where  charttime ::date='2019-1-02' and temperature is not null;

select avg(temperature) as temp_sum from nursingchart
 where date(charttime)='2019-1-02' and temperature is not null;*/

create or replace function AvgTemperature(enterdate Date)
returns decimal
as $$
declare
avg_temp Decimal;
begin
select avg(temperature) into avg_temp from nursingchart where charttime ::Date=enterdate;
return avg_temp;
end;
$$ language plpgsql;

select  AvgTemperature('2019-1-02')

--51. Show the time spent in ICU for each patient that transferred to ICU from surgery.

select admitdept,icu_discharge_timefrom baseline  from baseline as b  CHECK THIS
where admitdept='ICU'

select discharge_dept,follow_date from outcome as o
where discharge_dept='Surgery';

select  b.admitdept, o.discharge_dept,o.follow_date,b.icu_discharge_time from outcome as o 
join 
baseline as b 
on
o.patient_id=b.patient_id
where b.admitdept='Surgery' and o.discharge_dept='ICU'

--52. List all the drugs that were administered between 4 and 5 AM.
select drugname, drug_time::time from drugs where drug_time ::time between '04:00:00' and '05:00:00';

--53. Rank each patient based on the number of times they went to ICU.   
select patient_id,count(patient_id)as patient_count,dense_rank() over(order by count(patient_id) desc) as dense_rank,transferdept from transfer 
where transferdept='ICU'
group by patient_id, transferdept;				

select * from icd;

--54.Create a function to calculate the percentage of patients admitted into each department.
create or replace function Patience_in_departmets()
returns table(
Percentage_of_patients decimal,
admitdept text
)
as $$
begin
return query 
	select round((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER ()) as Percentage_of_patients, baseline.admitdept as departments
from baseline
group by baseline.admitdept;
end;
$$ language plpgsql;

select * from Patience_in_departmets()


drop function Patience_in_departmets();

select Patience_in_departmets(), admitdept from baseline
group by admitdept;

select count(*) from baseline;
select patient_id from baseline;

--55. Calculate the variance and standard deviation of oxygen flow readings across different admin departments.
update nursingchart
set oxygen_flow = 0
where oxygen_flow is null;											//Change the oxygen_flow column values
rollback

select oxygen_flow from nursingchart
alter table nursingchart
alter column oxygen_flow type integer using oxygen_flow::integer;

select variance(n.oxygen_flow) as variance_oxygenflow, stddev(n.oxygen_flow), t.transferdept from nursingchart as n
join transfer as t
on
t.inp_no= n.inp_no
group by t.transferdept

select oxygen_flow from nursingchart;
--56.. Use a nested query to calculate the max blood sugar among patients whose average is below 120
select inp_no, max(blood_sugar)as max_blood_sugar,(select inp_no , avg(blood_sugar) from nursingchart
group by inp_no
having avg(blood_sugar)< 120) from nursingchart
group by inp_no


select inp_no, avg(blood_sugar) as average_blood from nursingchart
group by inp_no, blood_sugar
having avg(blood_sugar)<120

--57.List all transfers that started due to a change in disease.
select * from transfer
where stopreason='Disease change'

--58. Show the number of drugs administered to every patient aged 65 or older.
select b.patient_id,b.age, count(d.drugname) as count_of_drugs from baseline as b
join
drugs as d
on
b.patient_id = d.patient_id
where b.age >65
group by b.patient_id, b.age

--59. Find the patients’ report feeling happy all the time.
select substring(sf36_emotional_happyperson,3) from outcome
update outcome
set sf36_emotional_happyperson=substring(sf36_emotional_happyperson,3)

select sf36_emotional_happyperson from outcome
select distinct(sf36_emotional_happyperson) from outcome

select patient_id,sf36_emotional_happyperson as Happy_person from outcome 
where sf36_emotional_happyperson ='All of the time'

--60. List the patients that were discharged in December of any year.
select patient_id, icu_discharge_time from baseline
where extract(month  from icu_discharge_time)=12    2019-12-19

--61. List the last 100 patients that were discharged.
select icu_discharge_time,patient_id from baseline
order by icu_discharge_time desc

--62. Create a role that cannot create other roles and expires on 12/31/24.
create role rolez
nocreaterole					
valid until '2024-12-31';
select rolname, rolvaliduntil
from pg_roles
where rolname='rolez'

--63. Find instances where a patient was transferred into the same department twice within a day
SELECT t1.patient_id,t1.transferdept, 
t1.starttime AS starttime1, 
t1.stoptime AS stoptime1,
t2.starttime AS starttime2,
t2.stoptime AS stoptime2 FROM transfer t1  
JOIN transfer t2 
ON t1.patient_id = t2.patient_id  
AND t1.transferdept = t2.transferdept 
AND t1.starttime::date = t2.starttime::date 
AND t1.starttime < t2.starttime 
AND t1.starttime::date = t2.starttime::date
WHERE t1.starttime::date = t2.starttime::date 
ORDER BY t1.transferdept, t1.patient_id, t1.starttime;

--64. Use nested CTEs to calculate the median temperature of patients over 60 years old while in ICU.

with patient_above_60
as
(
select patient_id, age from baseline 
where age>60
),

avg_temp
as
(
select p.patient_id,p.age,avg(n.temperature)  as avg_temp from patient_above_60 as p
join
transfer as t
on
p.patient_id=t.patient_id
join nursingchart as n
on
t.inp_no =n.inp_no
where transferdept='ICU'
group by p.patient_id, p.age
)


select * from avg_temp

--65. Show the average sodium value for each patient.
select b.patient_id,l.inp_no,round(avg(labvalue:: Numeric),2) as Avg_sodium_value from lab l
join baseline b
on b.inp_no=l.inp_no
where item like 'Sodium (NA)'
group by l.inp_no,b.patient_id
order by Avg_sodium_value asc


--66.For each department show the count of patients whose condition got worse a year after discharge.
select discharge_dept,count(patient_id) as No_of_Patients from outcome
where sf36_oneyearcomparehealthcondition like '5_Much worse now than one year ago'
group by discharge_dept


--68. Show the 9th youngest patient and if they are alive or not.
select * from 
(select b.patient_id,age,follow_vital,
dense_rank() over(order by age asc) as age_rank
from baseline b
join outcome o
on b.patient_id=o.patient_id
)
where age_rank=9

--69.Show the bar distribution of ventilator modes for Pneumonia patients. Hint: Do not consider null values.
select  breathing_pattern,count(distinct b.inp_no) as No_of_Patients from  nursingchart n
join baseline b
on b.inp_no=n.inp_no
where infectionsite like 'Pneumonia' and breathing_pattern is not null
group by breathing_pattern

--70. Create a view on baseline table with a check option on admit department
create view admission_dept_Icu as
select  patient_id,age,sex,infectionsite,admitdept from baseline
where admitdept like 'ICU'


select * from admission_dept_Icu

--71. How many patients were admitted to Surgery within 30 days of getting discharged?
Select count(distinct t1.patient_id) as patient_count
from transfer t1
join transfer t2 
  on t1.patient_id = t2.patient_id
Where t1.transferdept = 'Surgery' 
  and t2.stoptime is not  NULL
  and t1.starttime > t2.stoptime
  and t1.starttime <= t2.stoptime + INTERVAL '30 days'
  
  
--73.List the tables where column Patient_ID is present.(display column position number with respective table also)
select
    table_schema, 
    table_name, 
    column_name, 
    ordinal_position as column_pos 
from
    Information_schema.columns
where
    column_name like '%patient_id%' 
order by
    table_name;
	
--74.Find the average heart rate of patients under 40.
select n.inp_no,round(avg(heart_rate::numeric),2) as Avg_heart_rate,age from nursingchart n
join baseline b
on b.inp_no=n.inp_no
where age<40
group by n.inp_no,age
order by age

--76.Identify patients whose breathing tube has been removed.
select distinct patient_id,n.inp_no,extubation from nursingchart n
join baseline b
on b.inp_no=n.inp_no
where extubation=True

--77. Compare each diastolic blood pressure value with the previous reading. And show previous and current value.
select inp_no,invasive_diastolic_blood_pressure as Current_reading,
lag(invasive_diastolic_blood_pressure ) over (partition by inp_no order by charttime) as previous_reading,
charttime
from nursingchart
where invasive_diastolic_blood_pressure is not null


--78.List patients who have more than 500 entries in the nursing chart.
select patient_id,count(b.patient_id) as No_of_Entries from baseline b
join nursingchart n
on b.inp_no=n.inp_no
group by patient_id
having count(patient_id)>500
order by count(patient_id)


--79.Display month name and the number of patients discharged from the ICU in that month.
select extract(year from icu_discharge_time) as Year,To_char(icu_discharge_time,'month') as Month,count(patient_id) as No_of_Patients from baseline
group by extract(year from icu_discharge_time),To_char(icu_discharge_time,'month')
order by extract(year from icu_discharge_time) desc,To_char(icu_discharge_time,'month') 


--80. Write a function that calculates the percentage of people who had moderate body pain after 4 weeks?
Create or replace function calculate_moderate_pain_percentage()
returns numeric AS $$
begin
    return(
        with cte as (
            select count(patient_id) as moderate_pain_count
            from outcome
            where sf36_pain_bodypainpast4wk like '4_Moderate'
        ),
        total_patients as (
            select count(patient_id) as total_pp
            from outcome
            where sf36_pain_bodypainpast4wk is not  NULL
        )
        select ROUND((cte.moderate_pain_count * 100.0 / total_patients.total_pp), 2)
        from cte, total_patients
    );
end;
$$ language plpgsql;



select calculate_moderate_pain_percentage()


