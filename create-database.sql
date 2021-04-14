CREATE DATABASE Covid;
GO

USE Covid;
GO

CREATE TABLE ISO_Code (
	id_iso_code INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  code VARCHAR(10) NOT NULL
);
GO

CREATE TABLE Continent (
	id_continent INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
  name VARCHAR(15) NOT NULL
);
GO

CREATE TABLE Location (
	id_location INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
	id_iso_code INT NOT NULL,
	id_continent Int,
	name VARCHAR(40) NOT NULL,
	population INT,
	population_density FLOAT,
	median_age FLOAT,
	age_65_older FLOAT,
	age_70_older FLOAT,
	gdp_per_capita FLOAT,
	extreme_poverty FLOAT,
	cardiovasc_death_rate FLOAT,
	diabetes_prevalence FLOAT,
	female_smokers FLOAT,
	male_smokers FLOAT,
	handwahing_facilities FLOAT,
	hospital_beds_per_thousand FLOAT,
	life_expenctancy FLOAT,
	human_development_index FLOAT,
	CONSTRAINT fk_iso_code FOREIGN KEY (id_iso_code) REFERENCES ISO_Code(id_iso_code),
	CONSTRAINT fk_continent FOREIGN KEY (id_continent) REFERENCES Continent(id_continent)
);
GO

CREATE TABLE Tests_Unit (
	id_tests_unit INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
	name VARCHAR(20) NOT NULL
);
GO

CREATE TABLE DailyRecord (
	id_review INT PRIMARY KEY IDENTITY (1,1) NOT NULL,
	id_location INT NOT NULL,
	date DATETIME,
	total_cases INT,
	new_cases INT,
	new_cases_smoothed FLOAT,
	total_deaths INT,
	new_deaths INT,
	new_deaths_smoothed FLOAT,
	total_cases_per_million FLOAT,
	new_cases_per_millio FLOAT,
	new_cases_smothed_per_million FLOAT,
	total_deaths_per_million FLOAT,
	new_deaths_per_million FLOAT,
	new_deaths_smoothed_per_million FLOAT,
	reproduction_rate FLOAT,
	icu_patients INT,
	icu_patients_per_million FLOAT,
	hosp_patients INT,
	hosp_patients_per_million FLOAT,
	weekly_icu_admissions FLOAT,
	weekly_icu_admissions_per_million FLOAT,
	new_tests INT,
	total_tests INT,
	total_tests_per_thousand FLOAT,
	new_tests_per_thousand FLOAT,
	new_tests_smoothed INT,
	new_tests_smoothed_per_thousand FLOAT,
	positive_rate FLOAT,
	tests_per_case FLOAT,
	id_tests_units INT,
	total_vaccinations INT,
	people_vaccinated INT,
	people_fully_vaccinated INT,
	new_vaccinations INT,
	new_vaccinations_smoothed INT,
	total_vaccinations_per_hundred FLOAT,
	people_vaccinated_per_hundred FLOAT,
	people_fully_vaccinated_per_hundred FLOAT,
	new_vaccinations_smoothed_per_million INT,
	stringency_index FLOAT,
  CONSTRAINT fk_location FOREIGN KEY (id_location) REFERENCES Location(id_location),
	CONSTRAINT fk_tests_units FOREIGN KEY (id_tests_units) REFERENCES Tests_Unit(id_tests_unit)
);
GO

--Procedure to truncate all tables
CREATE OR ALTER PROCEDURE TruncateTables
AS
BEGIN 
	TRUNCATE TABLE ISO_Code
END
GO

--Procedure to insert into ISO_Code
CREATE OR ALTER PROCEDURE InsertISO_Code
@code VARCHAR(10)
AS
BEGIN 
	IF NOT EXISTS(SELECT * FROM ISO_Code WHERE code = @code)
	BEGIN
		INSERT INTO ISO_Code (code) VALUES (@code)
	END
END
GO

--Procedure to insert into Continent
CREATE OR ALTER PROCEDURE InsertContinent
@name VARCHAR(15)
AS
BEGIN 
	IF NOT EXISTS(SELECT * FROM Continent WHERE name = @name)
	BEGIN
		INSERT INTO Continent (name) VALUES (@name)
	END
END
GO

--Procedure to insert into Location
CREATE OR ALTER PROCEDURE InsertLocation
@id_iso_code INT,
@id_continent INT,
@name VARCHAR(40),
@population INT = NULL,
@population_density FLOAT = NULL,
@median_age FLOAT = NULL,
@age_65_older FLOAT = NULL,
@age_70_older FLOAT = NULL,
@gdp_per_capita FLOAT = NULL,
@extreme_poverty FLOAT = NULL,
@cardiovasc_death_rate FLOAT = NULL,
@diabetes_prevalence FLOAT = NULL,
@female_smokers FLOAT = NULL,
@male_smokers FLOAT = NULL,
@handwahing_facilities FLOAT = NULL,
@hospital_beds_per_thousand FLOAT = NULL,
@life_expenctancy FLOAT = NULL,
@human_development_index FLOAT = NULL
AS
BEGIN 
	INSERT INTO Location (id_iso_code,id_continent,name,population,population_density,median_age,age_65_older,age_70_older,gdp_per_capita,extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,male_smokers,handwahing_facilities,hospital_beds_per_thousand,life_expenctancy,human_development_index) 
	VALUES (@id_iso_code,@id_continent,@name,@population,@population_density,@median_age,@age_65_older,@age_70_older,@gdp_per_capita,@extreme_poverty,@cardiovasc_death_rate,@diabetes_prevalence,@female_smokers,@male_smokers,@handwahing_facilities,@hospital_beds_per_thousand,@life_expenctancy,@human_development_index)
END
GO

--Procedure to insert into Tests_Unit
CREATE OR ALTER PROCEDURE InsertTests_Unit
@name VARCHAR(20)
AS
BEGIN 
	IF NOT EXISTS(SELECT * FROM Tests_Unit WHERE name = @name)
	BEGIN
		INSERT INTO Tests_Unit (name) VALUES (@name)
	END
END
GO

--Procedure to insert into DailyRecord
CREATE OR ALTER PROCEDURE InsertDailyRecord
@id_location INT,
@date DATETIME = NULL,
@total_cases INT = NULL,
@new_cases INT = NULL,
@new_cases_smoothed FLOAT = NULL,
@total_deaths INT = NULL,
@new_deaths INT = NULL,
@new_deaths_smoothed FLOAT = NULL,
@total_cases_per_million FLOAT = NULL,
@new_cases_per_millio FLOAT = NULL,
@new_cases_smothed_per_million FLOAT = NULL,
@total_deaths_per_million FLOAT = NULL,
@new_deaths_per_million FLOAT = NULL,
@new_deaths_smoothed_per_million FLOAT = NULL,
@reproduction_rate FLOAT = NULL,
@icu_patients INT = NULL,
@icu_patients_per_million FLOAT = NULL,
@hosp_patients INT = NULL,
@hosp_patients_per_million FLOAT = NULL,
@weekly_icu_admissions FLOAT = NULL,
@weekly_icu_admissions_per_million FLOAT = NULL,
@new_tests INT = NULL,
@total_tests INT = NULL,
@total_tests_per_thousand FLOAT = NULL,
@new_tests_per_thousand FLOAT = NULL,
@new_tests_smoothed INT = NULL,
@new_tests_smoothed_per_thousand FLOAT = NULL,
@positive_rate FLOAT = NULL,
@tests_per_case FLOAT = NULL,
@id_tests_units INT = NULL,
@total_vaccinations INT = NULL,
@people_vaccinated INT = NULL,
@people_fully_vaccinated INT = NULL,
@new_vaccinations INT = NULL,
@new_vaccinations_smoothed INT = NULL,
@total_vaccinations_per_hundred FLOAT = NULL,
@people_vaccinated_per_hundred FLOAT = NULL,
@people_fully_vaccinated_per_hundred FLOAT = NULL,
@new_vaccinations_smoothed_per_million INT = NULL,
@stringency_index FLOAT = NULL
AS
BEGIN 
	INSERT INTO DailyRecord (id_location,date,total_cases,new_cases,new_cases_smoothed,total_deaths,new_deaths,new_deaths_smoothed,total_cases_per_million,new_cases_per_millio,new_cases_smothed_per_million,total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,reproduction_rate,icu_patients,icu_patients_per_million,hosp_patients,hosp_patients_per_million,weekly_icu_admissions,weekly_icu_admissions_per_million,new_tests,total_tests,total_tests_per_thousand,new_tests_per_thousand,new_tests_smoothed,new_tests_smoothed_per_thousand,positive_rate,tests_per_case,id_tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,new_vaccinations,new_vaccinations_smoothed,total_vaccinations_per_hundred,people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,new_vaccinations_smoothed_per_million,stringency_index) 
	VALUES (@id_location,@date,@total_cases,@new_cases,@new_cases_smoothed,@total_deaths,@new_deaths,@new_deaths_smoothed,@total_cases_per_million,@new_cases_per_millio,@new_cases_smothed_per_million,@total_deaths_per_million,@new_deaths_per_million,@new_deaths_smoothed_per_million,@reproduction_rate,@icu_patients,@icu_patients_per_million,@hosp_patients,@hosp_patients_per_million,@weekly_icu_admissions,@weekly_icu_admissions_per_million,@new_tests,@total_tests,@total_tests_per_thousand,@new_tests_per_thousand,@new_tests_smoothed,@new_tests_smoothed_per_thousand,@positive_rate,@tests_per_case,@id_tests_units,@total_vaccinations,@people_vaccinated,@people_fully_vaccinated,@new_vaccinations,@new_vaccinations_smoothed,@total_vaccinations_per_hundred,@people_vaccinated_per_hundred,@people_fully_vaccinated_per_hundred,@new_vaccinations_smoothed_per_million,@stringency_index)
END
GO

