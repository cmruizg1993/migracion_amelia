/************************** DEFINIR LOS RUCS DE LOS CLIENTES A EXPORTAR ************************************** */

export const rucs = [
  "1713960290001",
  "0102392602001",
  "1716739568001",
  "0801561390001",
  "0915613780001",
  "1500688526001",
  "1205193715001",
  "0601488588001",
  "1755775440001",
  "0801270489001",
  "0501654438001",
  "1704841509001",
  "1716657257001",
  "1102052980001",
  "0800866667001",
  "1720382165001",
  "1713188652001",
  "1103506612001",
  "0202065330001",
  "1720832466001",
];

/************************** PRIMERA PARTE EXPORTACION DE CADA BASE DE DATOS ************************************** */

import { spawn } from 'node:child_process';
import path from 'node:path';

/************************** CONEXION CON LA BASE DE DATOS MASTER ************************************** */

import { exportDb } from './db.js';

const master = exportDb;
/************************** LEER TABLA DE EMPRESAS BDD MASTER ************************************** */

const getEmpresa = (ruc) => {
  const sqlQuery = `SELECT * FROM empresas WHERE EMP_RUC = ? LIMIT 1`;
   
  return master
  .execute(
      sqlQuery,
      [ruc]
    ).then(data =>{
      if(data.length > 0) return data[0];
      return null;
    });
}
export const getEmpresaDatabase = (database) => {
  const sqlQuery = `show databases like  '${database}'`;
   
  return master
  .execute(
      sqlQuery,
      []
    ).then(data =>{
      console.log(data)
      if(data.length > 0) return true;
      return false;
    });
}
const dumpDatabase = (database) => {

  const resultFileName = path.resolve('./export', `${database}.sql`);
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
const iniciarExportacion = () => {
  rucs.forEach(async (ruc) => {
    const empresa = await getEmpresa(ruc);
    
    if(!empresa) return;
  
    const existDatabase = await getEmpresaDatabase(empresa.EMP_DBNAME);
    
    console.log(existDatabase);
    
    if(existDatabase){
      console.log(`Exportando BDD ${empresa.EMP_DBNAME} RUC ${empresa.EMP_RUC}`);
  
      dumpDatabase(empresa.EMP_DBNAME).then();
    }
  
  })
}


/************************** SEGUNDA PARTE IMPORTACION DE CADA BASE DE DATOS ************************************** */