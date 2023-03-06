/************************** DEFINIR LOS RUCS DE LOS CLIENTES A EXPORTAR ************************************** */

const rucs = [
  "1719070292001",
  "1718962283001",
  "2390057019001",  
];

/************************** PRIMERA PARTE EXPORTACION DE CADA BASE DE DATOS ************************************** */
import mysql from 'mysql2';
import util from 'util';
import { spawn } from 'node:child_process';
import { resolve } from 'path';
import { exit } from 'process';

/************************** CONEXION CON LA BASE DE DATOS MASTER ************************************** */
const configAmeliaMaster =  {
  user: "support",
  password: "Soporte@2022",
  database: "ameliapro_master",
  host: "186.4.146.197",
  port: "4198",
};
const connection = mysql.createConnection({
  user: "support",
  password: "Soporte@2022",
  database: "ameliapro_master",
  host: "186.4.146.197",
  port: "4198",
});

connection.execute = util.promisify(connection.execute)
/************************** LEER TABLA DE EMPRESAS BDD MASTER ************************************** */

const getEmpresa = (ruc) => {
  const sqlQuery = `SELECT * FROM empresas WHERE EMP_RUC = ? LIMIT 1`;
   
  return connection
  .execute(
      sqlQuery,
      [ruc]
    ).then(data =>{
      if(data.length > 0) return data[0];
      return null;
    });
}
const getEmpresaDatabase = (database) => {
  const sqlQuery = `show databases like  '%${database}%'`;
   
  return connection
  .execute(
      sqlQuery,
      []
    ).then(data =>{
      if(data.length > 0) return true;
      return false;
    });
}
const iniciarExportacion = (database) => {

  const resultFileName = `${database}.sql`;
  const databaseToExport = database;

  const rootUser = 'support';
  const rootPassword = 'Soporte@2022';

  const mysqldump = spawn('mysqldump', 
  [
      databaseToExport, 
      `--result-file=${resultFileName}`, 
      '-u', rootUser, 
      `-p${rootPassword}`
  ]);
  return new Promise((res)=>{
    mysqldump.stdout.on('data', (data) => {
      console.log(`stdout: ${data}`);
    });
  
    mysqldump.stderr.on('data', (data) => {
      console.error(`stderr: ${data}`);
    });
  
    mysqldump.on('close', (code) => {
      console.log(`child process exited with code ${code}`);
      res(code);
    });
  })  
}

rucs.forEach(async (ruc) => {
  const empresa = await getEmpresa(ruc);
  
  if(!empresa) return;

  const existDatabase = await getEmpresaDatabase(empresa.EMP_DBNAME);
  
  console.log(existDatabase);
  
  if(!existDatabase) return;

  console.log(`Exportando BDD ${empresa.EMP_DBNAME} RUC ${empresa.EMP_RUC}`);

  iniciarExportacion(empresa.EMP_DBNAME).then();

})
exit(0);
/************************** PRIMERA PARTE EXPORTACION DE CADA BASE DE DATOS ************************************** */