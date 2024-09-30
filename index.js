const { MongoClient } = require("mongodb");
const duckdb = require('duckdb');

const uri = "mongodb+srv://DP_mongopython:yV0qDTUeZOebABy9@cluster0.2xxit.mongodb.net/";
const client = new MongoClient(uri);

const db = new duckdb.Database(':memory:', {
    "access_mode": "READ_WRITE",
    "max_memory": "2048MB",
    "threads": "4"
}, (err) => {
  if (err) {
    console.error(err);
  }
});

//Importamos el CSV de Employees

db.all("CREATE TABLE Employees AS FROM read_csv('files/Employees.csv', header = true);", function(err, res) {
  if (err) {
    console.log('Error al importar Employees');
    console.warn(err);
    return;
  }else{
    console.log("Tabla Employees creada correctamente")
    console.log(res.length);
  }
});

//Importamos el CSV de Salarios

db.all("CREATE TABLE Salary AS SELECT job, max(salary) salary FROM read_csv('files/Salary.csv', header = true) GROUP BY job;", function(err, res) {
  if (err) {
    console.log('Error al importar Salary');
    console.warn(err);
    return;
  }else{
    console.log("Tabla Salary creada correctamente")
    console.log(res.length);
  }
});

//Cruzamos las dos tablas

db.all("SELECT A.id, A.firstname, A.email, A.birth, A.job, A.vehicle, A.model, A.phone, B.salary FROM Employees A LEFT JOIN Salary B ON A.job = B.job", function(err, res) {
  if (err) {
      console.log('Error al cruzar');
      console.warn(err);
      return;
  }else{
    console.log(res.length)
    try {
      console.log('Entramos a cargar los datos');
      const database = client.db('test_python');
      const coll = database.collection('test_duckdb');
      coll.deleteMany();
      coll.insertMany(res);
      console.log('Terminó la carga');
      }
    finally {
    // Ensures that the client will close when you finish/error
    console.log('Cerramos conexión');
    client.close();
    }
  }
});

console.log("Vamos a cerrar la BBDD");
db.close();