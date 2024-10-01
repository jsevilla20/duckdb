const { MongoClient } = require("mongodb");
const duckdb = require('duckdb');

const uri = "mongodb+srv://DP_mongopython:yV0qDTUeZOebABy9@cluster0.2xxit.mongodb.net/";
const client = new MongoClient(uri);

let fechainicio = new Date();

console.log("################ INCIO ##################");
console.log("    ", fechainicio.toLocaleDateString(), "-", fechainicio.toLocaleTimeString());
console.log("#########################################");

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
  }
});

//Cruzamos las dos tablas

db.all("SELECT A.id, A.firstname, A.email, A.birth, A.job, A.vehicle, A.model, A.phone, B.salary FROM Employees A LEFT JOIN Salary B ON A.job = B.job", async (err, res)=> {
  if (err) {
      console.log('Error al cruzar');
      console.warn(err);
      return;
  }else{
    try {
      console.log('Entramos a cargar los datos: ', res.length);
      const database = client.db('test_python');
      const coll = database.collection('test_duckdb');
      await coll.deleteMany();
      await coll.insertMany(res);
    }catch(error){
      console.error(error);
    }
    finally {
      console.log('Terminó la carga');
      console.log('Cerramos conexión');
      client.close();
      console.log("Vamos a cerrar la BBDD");
      db.close();
      let fechafin = new Date();
      console.log("################## FIN ##################");
      console.log("    ", fechafin.toLocaleDateString(), "-", fechafin.toLocaleTimeString());
      console.log("#########################################");
      tiempoentrefechas = (fechafin.getTime() - fechainicio.getTime())/1000;
      console.log("Duración: ",tiempoentrefechas, " segundos");
    }
  }
});
