const db = require('./utils/db');
// const reader = require('./utils/reader');

(async () => {
  await db.connect();
  
  // await db.insertISO_Code("ABCD");
  // await db.truncateTables();
  // reader.readDataFromCovidJsonFile();
 
  // console.log(reader.getArrayOfIsoCodes());
  await db.disconnect();
})();