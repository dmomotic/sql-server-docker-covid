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
const getArrayOfIsoCodes = () => Object.keys(data);

module.exports = {
  readDataFromCovidJsonFile,
  getDataFromCovidJsonFile,
  getArrayOfIsoCodes
};

