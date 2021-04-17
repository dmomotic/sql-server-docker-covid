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
	population BIGINT,
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
	CONSTRAINT fk_iso_code FOREIGN KEY (id_iso_code) REFERENCES ISO_Code(id_iso_code) ON DELETE CASCADE,
	CONSTRAINT fk_continent FOREIGN KEY (id_continent) REFERENCES Continent(id_continent) ON DELETE CASCADE
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
  CONSTRAINT fk_location FOREIGN KEY (id_location) REFERENCES Location(id_location) ON DELETE CASCADE,
	CONSTRAINT fk_tests_units FOREIGN KEY (id_tests_units) REFERENCES Tests_Unit(id_tests_unit) ON DELETE CASCADE
);
GO

--Procedure to truncate all tables
CREATE OR ALTER PROCEDURE TruncateTables
AS
BEGIN
	IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DailyRecord')
	BEGIN
		DELETE FROM DailyRecord;
	END 
	IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Tests_Unit')
	BEGIN
		DELETE FROM Tests_Unit;
	END 
	IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Location')
	BEGIN
		DELETE FROM Location;
	END 
	IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Continent')
	BEGIN
		DELETE FROM Continent;
	END 
	IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ISO_Code')
	BEGIN
		DELETE FROM ISO_Code;
	END
END
GO

--Procedure to insert into ISO_Code
CREATE OR ALTER PROCEDURE InsertISO_Code
@code VARCHAR(10)
AS
BEGIN 
	IF NOT EXISTS(SELECT * FROM ISO_Code WHERE code = @code)
	BEGIN
		INSERT INTO ISO_Code (code) VALUES (@code);
		SELECT SCOPE_IDENTITY() as id;
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
		INSERT INTO Continent (name) VALUES (@name);
		SELECT SCOPE_IDENTITY() as id;
	END
END
GO

--Procedure to insert into Location
CREATE OR ALTER PROCEDURE InsertLocation
@id_iso_code INT,
@id_continent INT = 0,
@name VARCHAR(40),
@population BIGINT = 0,
@population_density FLOAT = 0,
@median_age FLOAT = 0,
@age_65_older FLOAT = 0,
@age_70_older FLOAT = 0,
@gdp_per_capita FLOAT = 0,
@extreme_poverty FLOAT = 0,
@cardiovasc_death_rate FLOAT = 0,
@diabetes_prevalence FLOAT = 0,
@female_smokers FLOAT = 0,
@male_smokers FLOAT = 0,
@handwahing_facilities FLOAT = 0,
@hospital_beds_per_thousand FLOAT = 0,
@life_expenctancy FLOAT = 0,
@human_development_index FLOAT = 0
AS
BEGIN 
	INSERT INTO Location (id_iso_code,id_continent,name,population,population_density,median_age,age_65_older,age_70_older,gdp_per_capita,extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,male_smokers,handwahing_facilities,hospital_beds_per_thousand,life_expenctancy,human_development_index) 
	VALUES (@id_iso_code,@id_continent,@name,@population,@population_density,@median_age,@age_65_older,@age_70_older,@gdp_per_capita,@extreme_poverty,@cardiovasc_death_rate,@diabetes_prevalence,@female_smokers,@male_smokers,@handwahing_facilities,@hospital_beds_per_thousand,@life_expenctancy,@human_development_index)
	SELECT id_location AS id, id_iso_code FROM Location WHERE id_iso_code = @id_iso_code AND name = @name;
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
		SELECT SCOPE_IDENTITY() as id;
	END
END
GO

--Procedure to insert into DailyRecord
CREATE OR ALTER PROCEDURE InsertDailyRecord
@id_location INT,
@date DATETIME = 0,
@total_cases INT = 0,
@new_cases INT = 0,
@new_cases_smoothed FLOAT = 0,
@total_deaths INT = 0,
@new_deaths INT = 0,
@new_deaths_smoothed FLOAT = 0,
@total_cases_per_million FLOAT = 0,
@new_cases_per_millio FLOAT = 0,
@new_cases_smothed_per_million FLOAT = 0,
@total_deaths_per_million FLOAT = 0,
@new_deaths_per_million FLOAT = 0,
@new_deaths_smoothed_per_million FLOAT = 0,
@reproduction_rate FLOAT = 0,
@icu_patients INT = 0,
@icu_patients_per_million FLOAT = 0,
@hosp_patients INT = 0,
@hosp_patients_per_million FLOAT = 0,
@weekly_icu_admissions FLOAT = 0,
@weekly_icu_admissions_per_million FLOAT = 0,
@new_tests INT = 0,
@total_tests INT = 0,
@total_tests_per_thousand FLOAT = 0,
@new_tests_per_thousand FLOAT = 0,
@new_tests_smoothed INT = 0,
@new_tests_smoothed_per_thousand FLOAT = 0,
@positive_rate FLOAT = 0,
@tests_per_case FLOAT = 0,
@id_tests_units INT = 0,
@total_vaccinations INT = 0,
@people_vaccinated INT = 0,
@people_fully_vaccinated INT = 0,
@new_vaccinations INT = 0,
@new_vaccinations_smoothed INT = 0,
@total_vaccinations_per_hundred FLOAT = 0,
@people_vaccinated_per_hundred FLOAT = 0,
@people_fully_vaccinated_per_hundred FLOAT = 0,
@new_vaccinations_smoothed_per_million INT = 0,
@stringency_index FLOAT = 0
AS
BEGIN 
	INSERT INTO DailyRecord (id_location,date,total_cases,new_cases,new_cases_smoothed,total_deaths,new_deaths,new_deaths_smoothed,total_cases_per_million,new_cases_per_millio,new_cases_smothed_per_million,total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,reproduction_rate,icu_patients,icu_patients_per_million,hosp_patients,hosp_patients_per_million,weekly_icu_admissions,weekly_icu_admissions_per_million,new_tests,total_tests,total_tests_per_thousand,new_tests_per_thousand,new_tests_smoothed,new_tests_smoothed_per_thousand,positive_rate,tests_per_case,id_tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,new_vaccinations,new_vaccinations_smoothed,total_vaccinations_per_hundred,people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,new_vaccinations_smoothed_per_million,stringency_index) 
	VALUES (@id_location,@date,@total_cases,@new_cases,@new_cases_smoothed,@total_deaths,@new_deaths,@new_deaths_smoothed,@total_cases_per_million,@new_cases_per_millio,@new_cases_smothed_per_million,@total_deaths_per_million,@new_deaths_per_million,@new_deaths_smoothed_per_million,@reproduction_rate,@icu_patients,@icu_patients_per_million,@hosp_patients,@hosp_patients_per_million,@weekly_icu_admissions,@weekly_icu_admissions_per_million,@new_tests,@total_tests,@total_tests_per_thousand,@new_tests_per_thousand,@new_tests_smoothed,@new_tests_smoothed_per_thousand,@positive_rate,@tests_per_case,@id_tests_units,@total_vaccinations,@people_vaccinated,@people_fully_vaccinated,@new_vaccinations,@new_vaccinations_smoothed,@total_vaccinations_per_hundred,@people_vaccinated_per_hundred,@people_fully_vaccinated_per_hundred,@new_vaccinations_smoothed_per_million,@stringency_index)
END
GO

