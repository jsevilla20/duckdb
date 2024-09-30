const { MongoClient } = require("mongodb");
const duckdb = require('duckdb');

const uri = "mongodb+srv://DP_mongopython:yV0qDTUeZOebABy9@cluster0.2xxit.mongodb.net/";
const client = new MongoClient(uri);

async function run(csvData) {
    
}

const db = new duckdb.Database(':memory:', {
    "access_mode": "READ_WRITE",
    "max_memory": "1024MB",
    "threads": "4"
}, (err) => {
  if (err) {
    console.error(err);
  }
});

//Importamos el CSV de Employees
"id";"firstname";"lastname";"email";"birth";"job";"vehicle";"model";"phone"
"1";"Marcella";"Schuppe";"Marcella21@gmail.com";"28/09/2024";"Global Markets Producer";"Polestar";"Impala";"677241141"
db.all("CREATE TABLE Employees FROM read_csv('Employees1.csv', types = {'id': 'INTEGER', 'firstname': 'VARCHAR', 'lastname': 'VARCHAR', 'email': 'VARCHAR', 'birth': 'DATETIME', 'job': 'VARCHAR', 'vehicle': 'VARCHAR', 'model': 'VARCHAR', 'phone': 'VARCHAR'});", function(err, res) {
    if (err) {
      console.log('Error al importar Employees');
      console.warn(err);
      return;
    }else{
      console.log("Tabla Employees creada correctamente")
    }
});

//Importamos el CSV de Salarios

db.all("CREATE TABLE Salary AS FROM read_csv('files/Salary.csv', header = true);", function(err, res) {
  if (err) {
    console.log('Error al importar Salary');
    console.warn(err);
    return;
  }else{
    console.log("Tabla Salary creada correctamente")
  }
  
});

//Cruzamos las dos tablas

db.all("SELECT * FROM Employees as a, Salary as b where a.job = b.job", function(err, res) {
  if (err) {
      console.log('Error al cruzar');
      console.warn(err);
      return;
  }

  try {
    console.log('Entramos a cargar el csv');
    const database = client.db('test_python');
    const coll = database.collection('test_duckdb');
    coll.deleteMany();
    coll.insertMany(res);
    
    console.log('Terminó la carga');
  } finally {
  // Ensures that the client will close when you finish/error
  client.close();
  console.log('Cerramos conexión');
  }
});