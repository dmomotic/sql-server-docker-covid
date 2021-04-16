const db = require("./utils/db");
const reader = require("./utils/reader");

(async () => {
  await db.connect();
  //Truncating tables
  await db.truncateTables();
  //Reading JSON file
  reader.readDataFromCovidJsonFile();

  //Inserting into ISO_Code table
  const iso_codes = reader.getArrayOfIsoCodes();
  for (const code of iso_codes) {
    await db.insertISO_Code(code);
    const id_iso_code = db.getIsoCodetId(code);

    const objectFromIsoCode = reader.getObjectFromIsoCode(code);
    if (objectFromIsoCode == null) continue;

    //Inserting into Continent table
    let id_continent;
    if ("continent" in objectFromIsoCode) {
      const continent = objectFromIsoCode.continent;
      if (!db.hasContinent(continent)) {
        await db.insertContinent(continent);
      }
      id_continent = db.getContinentId(continent);
    }

    //Inserting into Location table
    const name  = objectFromIsoCode.location;
    if (!db.hasLocation(name)) {
      const {
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
        human_development_index,
      } = objectFromIsoCode;

      await db.insertLocation(
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
      );
    }

    //Inserting into DailyRecord and Tests_Unit
    const id_location = db.getLocationId(name);
    if ("data" in objectFromIsoCode) {
      for (const record of objectFromIsoCode.data) {

        //Inserting into Tests_Unit
        if ("tests_units" in record && !db.hasTestUnit(record.tests_units)) {
          await db.insertTests_Unit(record.tests_units);
        }

        const id_tests_units = db.getTestUnitId(record.tests_units);
        Object.assign(record, { id_location, id_tests_units });

        //Inserting into DailyRecord
        db.insertDailyRecord(record);
      }
    }
  }
})();

process.on("exit", async (code) => {
  await db.disconnect();
});