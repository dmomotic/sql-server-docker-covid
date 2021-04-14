const sql = require("mssql");

const config = {
  user: "sa",
  password: "admin_123",
  server: "localhost", //default port 1433
  database: "Covid",
  options: {
    enableArithAbort: true,
  },
};

let pool;

const connect = async () => {
  try {
    console.log("Connection opening...");
    pool = await sql.connect(config);
    console.log("Connection opened...");
  } catch (error) {
    console.log('Connection not opened... :(');
    console.log(error);
  }
}

const disconnect = async () => {
  try {
    await pool?.close();
  } catch (error) {
    console.log("Connection couldn't be closed");
    console.log(error);
  }
}

const query = async (query) => {
  if (!pool) {
    console.log('Connection is not opened');
    return;
  }

  try {
    const { recordset, rowsAffected } = await sql.query(query);
    console.log(recordset, rowsAffected);
  } catch (error) {
    console.log("Couldn't execute query :(");
    console.log(error);
  }
}

const truncateTables = async () => {
  try {
    const result = await pool?.request().execute("TruncateTables");
    console.log(result);
  } catch (error) {
    console.log("It wasn't posible to truncate all the tables");
    console.log(error);
  }
}

const insertISO_Code = async (code) => {
  try {
    const result = await pool?.request().input('code', sql.VarChar(10), code).execute('InsertISO_Code');
    console.log(result);
  } catch (error) {
    console.log("Couldn't insert into ISO_code table :(");
    console.log(error);
  }
}

module.exports = {
  connect,
  disconnect,
  query,
  truncateTables,
  insertISO_Code,
};
