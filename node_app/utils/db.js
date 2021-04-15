const sql = require("mssql");

const isoCodes = {};
const continents = {};
const locations = {};
const testsUnit = {};
let pool;

const config = {
  user: "sa",
  password: "admin_123",
  server: "localhost", //default port 1433
  database: "Covid",
  options: {
    enableArithAbort: true,
  },
};

const connect = async () => {
  try {
    console.log("Connection opening...");
    pool = await sql.connect(config);
    console.log("Connection opened...");
  } catch (error) {
    console.log("Connection not opened... :(");
    console.log(error);
  }
};

const disconnect = async () => {
  try {
    await pool?.close();
  } catch (error) {
    console.log("Connection couldn't be closed");
    console.log(error);
  }
};

const query = async (query) => {
  if (!pool) {
    console.log("Connection is not opened");
    return;
  }

  try {
    const { recordset, rowsAffected } = await sql.query(query);
    console.log(recordset, rowsAffected);
  } catch (error) {
    console.log("Couldn't execute query :(");
    console.log(error);
  }
};

const truncateTables = async () => {
  try {
    const result = await pool?.request().execute("TruncateTables");
    console.log(result);
  } catch (error) {
    console.log("It wasn't posible to truncate all the tables");
    console.log(error);
  }
};

const insertISO_Code = async (code) => {
  try {
    code = code.toUpperCase();
    const result = await pool
      ?.request()
      .input("code", sql.VarChar(10), code)
      .execute("InsertISO_Code");
    const response = result.recordset[0]; // { id }
    const id = response?.id; // id from database
    isoCodes[code] = id; // Adding id to the object
    //console.log(`ISO_Code: ${code} -> ${id}`);
  } catch (error) {
    console.log("Couldn't insert into ISO_code table :(");
    console.log(error);
  }
};

const insertContinent = async (continent) => {
  try {
    const result = await pool
      ?.request()
      .input("name", sql.VarChar(15), continent)
      .execute("InsertContinent");
    const response = result.recordset[0]; // { id }
    const id = response?.id; // id from database
    continents[continent] = id; // Adding id to the object
    //console.log(`Continent: ${continent} -> ${id}`);
  } catch (error) {
    console.log("Couldn't insert into Continent table :(");
    console.log(error);
  }
};

const insertLocation = async (
  id_iso_code,
  id_continent,
  name,
  population,
  population_density,
  median_age,
  age_65_older,
  age_70_older,
  gdp_per_capita,
  extreme_poverty,
  cardiovasc_death_rate,
  diabetes_prevalence,
  female_smokers,
  male_smokers,
  handwahing_facilities,
  hospital_beds_per_thousand,
  life_expenctancy,
  human_development_index
) => {
  try {
    const result = await pool
      ?.request()
      .input("id_iso_code", sql.Int, id_iso_code)
      .input("id_continent", sql.Int, id_continent)
      .input("name", sql.VarChar(40), name)
      .input("population", sql.BigInt, population)
      .input("population_density", sql.Float, population_density)
      .input("median_age", sql.Float, median_age)
      .input("age_65_older", sql.Float, age_65_older)
      .input("age_70_older", sql.Float, age_70_older)
      .input("gdp_per_capita", sql.Float, gdp_per_capita)
      .input("extreme_poverty", sql.Float, extreme_poverty)
      .input("cardiovasc_death_rate", sql.Float, cardiovasc_death_rate)
      .input("diabetes_prevalence", sql.Float, diabetes_prevalence)
      .input("female_smokers", sql.Float, female_smokers)
      .input("male_smokers", sql.Float, male_smokers)
      .input("handwahing_facilities", sql.Float, handwahing_facilities)
      .input(
        "hospital_beds_per_thousand",
        sql.Float,
        hospital_beds_per_thousand
      )
      .input("life_expenctancy", sql.Float, life_expenctancy)
      .input("human_development_index", sql.Float, human_development_index)
      .execute("InsertLocation");
    const response = result.recordset[0]; // { id, id_iso_code }
    const id = response?.id; // id from database
    locations[name] = id; // Adding id to the object
    //console.log(`Location: ${name} -> ${id}`);
  } catch (error) {
    console.log("Couldn't insert into Location table :(", name, population);
    console.log(error);
  }
};

const insertTests_Unit = async (name) => {
  try {
    const result = await pool
      ?.request()
      .input("name", sql.VarChar(20), name)
      .execute("InsertTests_Unit");
    const response = result.recordset[0]; // { id }
    const id = response?.id; // id from database
    testsUnit[name] = id; // Adding id to the object
    //console.log(`Continent: ${continent} -> ${id}`);
  } catch (error) {
    console.log("Couldn't insert into Tests_Unit table :(");
    console.log(error);
  }
};

const insertDailyRecord = async (record) => {
  try {
    await pool
      ?.request()
      .input("id_location", sql.Int, record.id_location)
      .input("date", sql.Date(), record.date)
      .input("total_cases", sql.Int, record.total_cases)
      .input("new_cases", sql.Int, record.new_cases)
      .input("new_cases_smoothed", sql.Float, record.new_cases_smoothed)
      .input("total_deaths", sql.Int, record.total_deaths)
      .input("new_deaths", sql.Int, record.new_deaths)
      .input("new_deaths_smoothed", sql.Float, record.new_deaths_smoothed)
      .input(
        "total_cases_per_million",
        sql.Float,
        record.total_cases_per_million
      )
      .input("new_cases_per_millio", sql.Float, record.new_cases_per_millio)
      .input(
        "new_cases_smothed_per_million",
        sql.Float,
        record.new_cases_smothed_per_million
      )
      .input(
        "total_deaths_per_million",
        sql.Float,
        record.total_deaths_per_million
      )
      .input("new_deaths_per_million", sql.Float, record.new_deaths_per_million)
      .input(
        "new_deaths_smoothed_per_million",
        sql.Float,
        record.new_deaths_smoothed_per_million
      )
      .input("reproduction_rate", sql.Float, record.reproduction_rate)
      .input("icu_patients", sql.Int, record.icu_patients)
      .input(
        "icu_patients_per_million",
        sql.Float,
        record.icu_patients_per_million
      )
      .input("hosp_patients", sql.Int, record.hosp_patients)
      .input(
        "hosp_patients_per_million",
        sql.Float,
        record.hosp_patients_per_million
      )
      .input("weekly_icu_admissions", sql.Float, record.weekly_icu_admissions)
      .input(
        "weekly_icu_admissions_per_million",
        sql.Float,
        record.weekly_icu_admissions_per_million
      )
      .input("new_tests", sql.Int, record.new_tests)
      .input("total_tests", sql.Int, record.total_tests)
      .input(
        "total_tests_per_thousand",
        sql.Float,
        record.total_tests_per_thousand
      )
      .input("new_tests_per_thousand", sql.Float, record.new_tests_per_thousand)
      .input("new_tests_smoothed", sql.Int, record.new_tests_smoothed)
      .input(
        "new_tests_smoothed_per_thousand",
        sql.Float,
        record.new_tests_smoothed_per_thousand
      )
      .input("positive_rate", sql.Float, record.positive_rate)
      .input("tests_per_case", sql.Float, record.tests_per_case)
      .input("id_tests_units", sql.Int, record.id_tests_units)
      .input("total_vaccinations", sql.Int, record.total_vaccinations)
      .input("people_vaccinated", sql.Int, record.people_vaccinated)
      .input("people_fully_vaccinated", sql.Int, record.people_fully_vaccinated)
      .input("new_vaccinations", sql.Int, record.new_vaccinations)
      .input(
        "new_vaccinations_smoothed",
        sql.Int,
        record.new_vaccinations_smoothed
      )
      .input(
        "total_vaccinations_per_hundred",
        sql.Float,
        record.total_vaccinations_per_hundred
      )
      .input(
        "people_vaccinated_per_hundred",
        sql.Float,
        record.people_vaccinated_per_hundred
      )
      .input(
        "people_fully_vaccinated_per_hundred",
        sql.Float,
        record.people_fully_vaccinated_per_hundred
      )
      .input(
        "new_vaccinations_smoothed_per_million",
        sql.Int,
        record.new_vaccinations_smoothed_per_million
      )
      .input("stringency_index", sql.Float, record.stringency_index)
      .execute("InsertDailyRecord");
  } catch (error) {
    console.log("Couldn't insert into DailyRecord table :(");
    console.log(error);
  }
};

const hasContinent = (name) => name in continents;
const hasIsoCode = (code) => code in isoCodes;
const hasLocation = (name) => name in locations;
const hasTestUnit = (name) => name in testsUnit;

const getContinentId = (name) => continents[name];
const getIsoCodetId = (name) => isoCodes[name];
const getLocationId = (name) => locations[name];
const getTestUnitId = (name) => testsUnit[name];

module.exports = {
  connect,
  disconnect,
  query,
  truncateTables,
  insertISO_Code,
  hasContinent,
  hasIsoCode,
  hasLocation,
  hasTestUnit,
  insertContinent,
  insertLocation,
  insertTests_Unit,
  insertDailyRecord,
  getContinentId,
  getIsoCodetId,
  getLocationId,
  getTestUnitId,
};
