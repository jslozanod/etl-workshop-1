DROP TABLE IF EXISTS fact_application CASCADE;
DROP TABLE IF EXISTS dim_candidate CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_seniority CASCADE;
DROP TABLE IF EXISTS dim_technology CASCADE;

CREATE TABLE dim_candidate (
  candidate_key SERIAL PRIMARY KEY,
  first_name TEXT NOT NULL,
  last_name  TEXT NOT NULL,
  email      TEXT NOT NULL
);

CREATE TABLE dim_country (
  country_key SERIAL PRIMARY KEY,
  country TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_date (
  date_key SERIAL PRIMARY KEY,
  application_date DATE NOT NULL UNIQUE,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL
);

CREATE TABLE dim_seniority (
  seniority_key SERIAL PRIMARY KEY,
  seniority TEXT NOT NULL UNIQUE
);

CREATE TABLE dim_technology (
  technology_key SERIAL PRIMARY KEY,
  technology TEXT NOT NULL UNIQUE
);

CREATE TABLE fact_application (
  application_key SERIAL PRIMARY KEY,

  candidate_key   INT NOT NULL REFERENCES dim_candidate(candidate_key),
  country_key     INT NOT NULL REFERENCES dim_country(country_key),
  date_key        INT NOT NULL REFERENCES dim_date(date_key),
  seniority_key   INT NOT NULL REFERENCES dim_seniority(seniority_key),
  technology_key  INT NOT NULL REFERENCES dim_technology(technology_key),

  yoe INT NOT NULL,
  code_challenge_score INT NOT NULL,
  technical_interview_score INT NOT NULL,
  is_hired BOOLEAN NOT NULL
);