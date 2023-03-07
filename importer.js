import {rucs} from './exporter';
import { importDb, createConnection, configAmeliaMasterImport, executeQuery } from './db';
import fs from 'fs';
import path from 'path';

const connection = importDb;
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

const createDatabase = async (database) => {

    const sqlCreateDatabase = `CREATE DATABASE /*!32312 IF NOT EXISTS*/ ${database} /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;`;

    return importDb.execute(sqlCreateDatabase, []);

}
const importSql = (database, dumpFile) => {
    
    const config = configAmeliaMasterImport();
    const databaseToImport = database;    
    const rootUser = config.user;
    const rootPassword = config.password;

    const mysql = spawn('mysql', 
    [
        '-u', rootUser, 
        `-p${rootPassword}`,
        databaseToImport,
        '<',
        dumpFile
    ]);

    return new Promise((res)=>{
        mysql.stdout.on('data', (data) => {
            console.log(`stdout: ${data}`);
        });
    
        mysql.stderr.on('data', (data) => {
            console.error(`stderr: ${data}`);
        });
    
        mysql.on('close', (code) => {
            console.log(`child process exited with code ${code}`);
            res(code);
        });
    })  
      
}

const exportData = (database) => {
    
  const resultFileName = path.resolve('./import', `${database}.sql`);
  const databaseToExport = database;

  const rootUser = 'support';
  const rootPassword = 'Soporte@2022';

  const mysqldump = spawn('mysqldump', 
  [
      databaseToExport, 
      `--no-create-info`,
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


const alterConfigTable = (connection) => {

    const sqlQuery = `ALTER TABLE config_sistema ADD COM_CODIGO INTEGER NOT NULL, ADD PRIMARY KEY (COM_CODIGO, codigo_config)`;

    return executeQuery(sqlQuery, [], connection);
}

const getAllTables = (connection) => {    

    const sqlQuery = `SHOW TABLES;`

    return executeQuery(sqlQuery, [], connection);  
}

const getAllColumns = (connection, table) => {    

    const sqlQuery = `SHOW COLUMNS FROM ${table};`

    return executeQuery(sqlQuery, [], connection);  
}

const updateComCodigo = (connection, table, comCodigo) => {
    
    const sqlQuery = `UPDATE ${table} SET COM_CODIGO = ? ;`

    return executeQuery(sqlQuery, connection, [comCodigo]);    
}

const iniciarMigracion = ()=>{
    //Creación Amelia unificada

    const bddAmeliaUnificada = "ameliapro_1";

    //createDatabase(bddAmeliaUnificada);

    //Eliminar columnas que sobrecargan el sistema

    rucs.forEach(async (ruc) => {
        const empresa = await getEmpresa(ruc);

        if(!empresa) return;

        console.log(`Importando BDD ${empresa.EMP_DBNAME} RUC ${empresa.EMP_RUC}`);
        
        const database = empresa.EMP_DBNAME;

        const comCodigo = empresa.EMP_CODIGO;

        const dumpFile = path.resolve('export', `${database}.sql`);

        if(!fs.existsSync(dumpFile)) {
            console.log(`Error: No se encontro el archivo: ${dumpFile}`)
            return;
        }
        console.log(`Creando base de datos: ${database}`)

        const result  = await createDatabase(database);

        console.log(`Se ha creado la base de datos: ${database}`)
        
            
        const code = await importSql(database, dumpFile);
        
        if(code != 0 )
        {
            console.log('Error al importar archivo SQL');
            return;
        } 
        /*
        const config = configAmeliaMasterImport();

        config.database = database;

        const connection = createConnection(config);

        await alterConfigTable(connection);

        const tables = getAllTables(connection);

        tables.forEach(async (table) => {
            
            const columns = await getAllColumns(connection, table);

            const hasComCodigo = columns.filter( column => column == 'COM_CODIGO').length > 0;

            if(!hasComCodigo) return;

            updateComCodigo(connection, table, comCodigo).then();

        })
        // Se exporta solo los datos de cada base de datos
        
        const codeExportData = await exportData(database);

        if(codeExportData != 0) {
            console.log('Hubo en error al exportar los datos de '+database)
            return;
        }

        // Se importa datos en Amelia unificada
        const importDumpFile = path.resolve('import', `${database}.sql`);

        importSql(bddAmeliaUnificada, importDumpFile);

        */
        
    })
}
