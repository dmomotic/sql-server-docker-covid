const fs = require("fs");
const path = require("path");

let data;

//Read the file just once
const readDataFromCovidJsonFile = () => {
  const file = fs.readFileSync(
    path.resolve(__dirname, "../data/owid-covid-data.json")
  );
  data = JSON.parse(file);
}

//Returns an object with all the data readed
const getDataFromCovidJsonFile = () => data;

//Returns array of keys (iso_code)
const getArrayOfIsoCodes = () => {
  const keys = Object.keys(data);
  return keys.filter(key => !key.includes('OWID'));
};

const getObjectFromIsoCode = (code) => {
  return data != null ? data[code] : null;
}

module.exports = {
  readDataFromCovidJsonFile,
  getDataFromCovidJsonFile,
  getArrayOfIsoCodes,
  getObjectFromIsoCode,
};

