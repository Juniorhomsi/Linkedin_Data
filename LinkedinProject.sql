CREATE DATABASE IF NOT EXISTS LINKEDIN;

--  Créer le schéma BRONZE + le Stage S3
CREATE SCHEMA IF NOT EXISTS LINKEDIN.BRONZE;

CREATE OR REPLACE STAGE LINKEDIN.BRONZE.linkedin_stage
  URL = 's3://snowflake-lab-bucket/';

-- Afficher Les fichiers disponibles dans le bucket S3 public
LIST @LINKEDIN.BRONZE.linkedin_stage;

-- Creation des files formats
CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.csv_format
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE FILE FORMAT LINKEDIN.BRONZE.json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- Configuration d'un warehouse
CREATE OR REPLACE WAREHOUSE LINKEDIN_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse pour le projet LinkedIn';
    
-- Creation des tables et copy des données depuis le s3 vers la tables 


-- TABLE : JOB_POSTINGS
-- Création de la table
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_POSTINGS (
    job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary STRING,
    med_salary STRING,
    min_salary STRING,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    original_listed_time STRING,
    remote_allowed STRING,
    views STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

-- Chargement des données depuis le stage
COPY INTO LINKEDIN.BRONZE.JOB_POSTINGS
FROM @LINKEDIN.BRONZE.linkedin_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Vérification
SELECT * FROM LINKEDIN.BRONZE.JOB_POSTINGS LIMIT 10;

-- TABLE : BENEFICTS
-- Création de la table
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.BENEFITS (
    job_id STRING,
    inferred STRING,
    type STRING
);

-- Chargement des données
COPY INTO LINKEDIN.BRONZE.BENEFITS
FROM @LINKEDIN.BRONZE.linkedin_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Vérification
SELECT * FROM LINKEDIN.BRONZE.BENEFITS LIMIT 10;

-- TABLE : EMPLOYEE_COUNNTS
-- Création de la table
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.EMPLOYEE_COUNTS (
    company_id STRING,
    employee_count STRING,
    follower_count STRING,
    time_recorded STRING
);

-- Chargement des données
COPY INTO LINKEDIN.BRONZE.EMPLOYEE_COUNTS
FROM @LINKEDIN.BRONZE.linkedin_stage/employee_counts.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Vérification
SELECT * FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS LIMIT 10;


-- Table : JOB_SKILLS
-- Création de la table
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_SKILLS (
    job_id STRING,
    skill_abr STRING
);

-- Chargement des données
COPY INTO LINKEDIN.BRONZE.JOB_SKILLS
FROM @LINKEDIN.BRONZE.linkedin_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.csv_format');

-- Vérification
SELECT * FROM LINKEDIN.BRONZE.JOB_SKILLS LIMIT 10;


-- Table : COMPANIES
-- Pour le JSON : une seule colonne VARIANT
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANIES (
    data VARIANT
);

-- Chargement des données
COPY INTO LINKEDIN.BRONZE.COMPANIES
FROM @LINKEDIN.BRONZE.linkedin_stage/companies.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

-- Vérification
SELECT * FROM LINKEDIN.BRONZE.COMPANIES LIMIT 10;

-- Table : COMPANY_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_INDUSTRIES (
    data VARIANT
);

COPY INTO LINKEDIN.BRONZE.COMPANY_INDUSTRIES
FROM @LINKEDIN.BRONZE.linkedin_stage/company_industries.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

SELECT * FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES LIMIT 10;


-- Table : COMPANY_SPECIALITIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.COMPANY_SPECIALITIES (
    data VARIANT
);

COPY INTO LINKEDIN.BRONZE.COMPANY_SPECIALITIES
FROM @LINKEDIN.BRONZE.linkedin_stage/company_specialities.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

SELECT * FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES LIMIT 10;

-- Table : JOB_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.BRONZE.JOB_INDUSTRIES (
    data VARIANT
);

COPY INTO LINKEDIN.BRONZE.JOB_INDUSTRIES
FROM @LINKEDIN.BRONZE.linkedin_stage/job_industries.json
FILE_FORMAT = (FORMAT_NAME = 'LINKEDIN.BRONZE.json_format');

SELECT * FROM LINKEDIN.BRONZE.JOB_INDUSTRIES LIMIT 10;


-- ********** SCHEMA SILVER : ON NETTOIE LES DONNEES ***********
CREATE SCHEMA IF NOT EXISTS LINKEDIN.SILVER;

-- On crée les tables nettoyées

-- Table JOB_POSTINGS
-- On nettoie :  on ne veut pas les salaires negatifs ou alors des données avec le titre vide, ou sans job_id
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
  job_id,
  company_name,
  title,
  description,
  max_salary::FLOAT AS max_salary,
  med_salary::FLOAT AS med_salary,
  min_salary::FLOAT AS min_salary,
  pay_period,
  formatted_work_type,
  location,
  applies::INT AS applies,
  TO_TIMESTAMP(original_listed_time::BIGINT / 1000) AS original_listed_time,
  --IFF(remote_allowed::FLOAT = 1, TRUE, FALSE) AS remote_allowed,
  CASE
    WHEN remote_allowed = '1' THEN TRUE
    WHEN remote_allowed = '0' THEN FALSE
    ELSE NULL
  END AS remote_allowed,
  views::INT AS views,
  formatted_experience_level,
  sponsored::BOOLEAN AS sponsored,
  work_type,
  currency,
  compensation_type
  FROM LINKEDIN.BRONZE.JOB_POSTINGS
  WHERE job_id IS NOT NULL
    AND title IS NOT NULL
    AND company_name IS NOT NULL
    AND TRIM(title) <> ''
    AND (min_salary IS NULL OR min_salary > 0)
    AND (max_salary IS NULL OR max_salary > 0);


-- Vérification
SELECT * FROM LINKEDIN.SILVER.JOB_POSTINGS;
-- Observation 
-- Certaines valeurs de company_name correspondent à des identifiants internes fournis par la source LinkedIn. En l’absence d’une clé technique uniforme entre les fichiers, ces valeurs ont été conservées dans la couche Silver afin de ne pas perdre d’information.


--Table: BENEFITS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.BENEFITS AS
SELECT
    job_id,
    inferred::BOOLEAN AS inferred,
    type AS benefit_type
FROM LINKEDIN.BRONZE.BENEFITS
WHERE type IS NOT NULL;

SELECT * FROM LINKEDIN.SILVER.BENEFITS LIMIT 10;

-- Table SILVER.EMPLOYEE_COUNTS
CREATE OR REPLACE TABLE LINKEDIN.SILVER.EMPLOYEE_COUNTS AS
SELECT
    company_id,
    employee_count::INT AS employee_count,
    follower_count::INT AS follower_count,
    TO_TIMESTAMP(time_recorded::BIGINT / 1000) AS time_recorded
FROM LINKEDIN.BRONZE.EMPLOYEE_COUNTS
WHERE employee_count >= 0
  AND follower_count >= 0;

SELECT * FROM LINKEDIN.SILVER.EMPLOYEE_COUNTS LIMIT 10;

-- Table SILVER.JOB_SKILLS
CREATE TABLE IF NOT EXISTS LINKEDIN.SILVER.JOB_SKILLS AS
SELECT
    job_id,
    skill_abr
FROM LINKEDIN.BRONZE.JOB_SKILLS;

SELECT * FROM LINKEDIN.SILVER.JOB_SKILLS LIMIT 10;

-- On passe de JSON aux colonnes

-- Table SILVER.COMPANIES
CREATE TABLE IF NOT EXISTS LINKEDIN.SILVER.COMPANIES AS
SELECT
    data:company_id::INT AS company_id,
    data:name::STRING AS company_name,
    data:description::STRING AS description,
    data:company_size::INT AS company_size,
    data:state::STRING AS state,
    data:country::STRING AS country,
    data:city::STRING AS city,
    data:zip_code::STRING AS zip_code,
    data:address::STRING AS address,
    data:url::STRING AS url
FROM LINKEDIN.BRONZE.COMPANIES;

SELECT * FROM LINKEDIN.SILVER.COMPANIES;

SELECT count(*) FROM LINKEDIN.SILVER.COMPANIES
WHERE company_size IS NULL;
-- On constate que nous avons 589 sur 6063 n'ont precisé la taille de l'entreprise 



--Table SILVER.COMPANY_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    data:company_id::INT AS company_id,
    data:industry::STRING AS industry
FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES;

SELECT * FROM LINKEDIN.SILVER.COMPANY_INDUSTRIES LIMIT 10;


--Table SILVER.COMPANY_SPECIALITIES
CREATE TABLE IF NOT EXISTS LINKEDIN.SILVER.COMPANY_SPECIALITIES AS
SELECT
    data:company_id::INT AS company_id,
    data:speciality::STRING AS speciality
FROM LINKEDIN.BRONZE.COMPANY_SPECIALITIES;

SELECT * FROM LINKEDIN.SILVER.COMPANY_SPECIALITIES LIMIT 10;


-- Table SILVER.JOB_INDUSTRIES
CREATE TABLE IF NOT EXISTS LINKEDIN.SILVER.JOB_INDUSTRIES AS
SELECT
    data:job_id::INT AS job_id,
    data:industry_id::INT AS industry_id
FROM LINKEDIN.BRONZE.JOB_INDUSTRIES;

SELECT * FROM LINKEDIN.SILVER.JOB_INDUSTRIES LIMIT 10;

-- *******************************
-- *    Couche finale : GOLD     *
-- *******************************

-- Nous allons creer le schema gold 
-- Dans ce schema nous allons creer les tables de dimensions / faits 
CREATE SCHEMA IF NOT EXISTS LINKEDIN.GOLD;

-- Vue Dimemsion DIM_COMPANIES
-- Ici nous essayons de categoriser les companies
-- La dimension DIM_COMPANIES permet de contextualiser les offres d’emploi selon la taille et la localisation des entreprises, facilitant ainsi les analyses de répartition par catégorie d’entreprise.
CREATE OR REPLACE VIEW LINKEDIN.GOLD.DIM_COMPANIES AS
SELECT
    company_id,
    NVL(company_name, 'Unknown') AS company_name,
    description,
    CASE
        WHEN company_size = 0 THEN 'Très petite (< 10)'
        WHEN company_size = 1 THEN 'Petite (10-50)'
        WHEN company_size = 2 THEN 'Petite-Moyenne (50-200)'
        WHEN company_size = 3 THEN 'Moyenne (200-500)'
        WHEN company_size = 4 THEN 'Grande (500-1000)'
        WHEN company_size = 5 THEN 'Très grande (1000-5000)'
        WHEN company_size = 6 THEN 'Énorme (5000-10000)'
        WHEN company_size = 7 THEN 'Géant (> 10000)'
        ELSE 'Non renseigné'
    END AS company_size_label,
    country,
    city,
    url
FROM LINKEDIN.SILVER.COMPANIES;

-- Vérification
SELECT * FROM LINKEDIN.GOLD.DIM_COMPANIES;

-- Vue DIM_JOBS
CREATE OR REPLACE VIEW LINKEDIN.GOLD.DIM_JOBS AS
SELECT
    job_id,
    NVL(title, 'Unknown') AS job_title,
    formatted_work_type,
    formatted_experience_level,
    location,
    remote_allowed,
    work_type,
    NVL(pay_period, 'Not specified') AS pay_period,
    currency,
    min_salary,
    med_salary,
    max_salary
FROM LINKEDIN.SILVER.JOB_POSTINGS;

-- Vérification
SELECT * FROM LINKEDIN.GOLD.DIM_JOBS;

-- Table FACT_JOB_POSTINGS
CREATE OR REPLACE TABLE LINKEDIN.GOLD.FACT_JOB_POSTINGS AS
SELECT
    jp.job_id,
    jp.company_name,
    jp.title AS job_title,
    jp.formatted_work_type,
    jp.formatted_experience_level,
    jp.location,
    jp.remote_allowed,
    jp.min_salary,
    jp.med_salary,
    jp.max_salary,
    jp.pay_period,
    jp.applies,
    jp.views,
    jp.original_listed_time,
    jp.sponsored,
    c.company_id,
    c.company_size_label,
    c.country,
    c.city
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.GOLD.DIM_COMPANIES c
    ON jp.company_name = c.company_id
WHERE jp.title IS NOT NULL;

-- Vérification
SELECT * FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS LIMIT 10;

--Table FACT_JOB_INDUSTRIES
CREATE OR REPLACE TABLE LINKEDIN.GOLD.FACT_JOB_INDUSTRIES AS
SELECT
    ji.job_id,
    ji.industry_id,
    jp.job_title,
    jp.min_salary,
    jp.med_salary,
    jp.max_salary,
    jp.company_size_label
FROM LINKEDIN.SILVER.JOB_INDUSTRIES ji
LEFT JOIN LINKEDIN.GOLD.FACT_JOB_POSTINGS jp
    ON ji.job_id = jp.job_id;

-- Vérification
SELECT * FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES;

SELECT DISTINCT formatted_work_type 
FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS 
LIMIT 20;

-- Requête 2 :
SELECT DISTINCT company_size_label 
FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS 
LIMIT 20;

-- Verifications 
-- Voir les company_name dans JOB_POSTINGS
SELECT DISTINCT company_name 
FROM LINKEDIN.SILVER.JOB_POSTINGS 
LIMIT 10;
-- Voir les company_name dans COMPANIES
SELECT DISTINCT company_name 
FROM LINKEDIN.SILVER.COMPANIES 
LIMIT 10;

-- Requête 3 :
SELECT DISTINCT industry_id 
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES 
LIMIT 20;
-- Requête 4 :
SELECT COUNT(*) as total 
FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS;


SELECT
  industry_id,
  job_title,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES
WHERE job_title IS NOT NULL
GROUP BY industry_id, job_title
ORDER BY nb_offres DESC
LIMIT 10;

SELECT
  industry_id,
  job_title,
  AVG(med_salary) AS avg_salary
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES
WHERE med_salary IS NOT NULL
GROUP BY industry_id, job_title
ORDER BY avg_salary DESC
LIMIT 10;

SELECT
  company_size_label,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS
GROUP BY company_size_label
ORDER BY nb_offres DESC;


SELECT
  industry_id,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES
GROUP BY industry_id
ORDER BY nb_offres DESC;

SELECT
  formatted_work_type,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.DIM_JOBS
GROUP BY formatted_work_type
ORDER BY nb_offres DESC;




