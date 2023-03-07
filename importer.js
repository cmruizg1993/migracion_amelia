import {rucs} from './exporter.js';
import { importDb, createConnection, configAmeliaMasterImport, executeQuery } from './db.js';
import fs from 'fs';
import path from 'path';
import { spawn, exec } from 'node:child_process';


const tablasComCodigo = [
    {
        tabla: "config_sistema",
        pk: "codigo_config"
    }
]

const getEmpresa = (ruc) => {
    const sqlQuery = `SELECT * FROM empresas WHERE EMP_RUC = ? LIMIT 1`;
     
    return importDb
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

    

    return new Promise((res)=>{
        exec(`mysql -u ${rootUser} -p${rootPassword} ${database}  < ${dumpFile}`, (error, stdout, stderr) => {
            if (error) {
                console.log(`error: ${error.message}`);
                
            }
            if (stderr) {
                console.log(`stderr: ${stderr}`);
                
            }
            console.log(`stdout: ${stdout}`);
            res({error, stdout, stderr});
        })
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


const addPrimaryKey = async (tabla, pk, connection) => {

    const sqlQueryDrop = `ALTER TABLE ${tabla} DROP PRIMARY KEY;`;

    const dropResult = await executeQuery(sqlQueryDrop, connection, []);

    console.log(dropResult);

    const sqlQuery = `ALTER TABLE ${tabla} ADD COM_CODIGO INTEGER NOT NULL, ADD PRIMARY KEY (COM_CODIGO, ${pk})`;

    return executeQuery(sqlQuery, connection, [] );
}

const getAllTables = (connection) => {    

    const sqlQuery = `SHOW TABLES;`

    return executeQuery(sqlQuery, connection, []);  
}

const getComCodigoColumn = (connection, table) => {    

    const sqlQuery = `SHOW COLUMNS FROM ${table} LIKE 'COM_CODIGO';`

    return executeQuery(sqlQuery, connection, []);  
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
        
        /*
        const { error, stdout, stderr } = await importSql(database, dumpFile);
        
        console.log({ error, stdout, stderr })
        
        if(error != null )
        {
            console.log('Error al importar archivo SQL');
            return;
        } 
        */
        
        // se va  agregar el com_codigo a las tablas que lo necesiten

        const config = configAmeliaMasterImport();

        config.database = database;

        const connection = await createConnection(config);
        /*

        tablasComCodigo.forEach( async ( tabla ) => {

            await addPrimaryKey(tabla.tabla, tabla.pk, connection);
        
        })
        */
        
        
        const tables = await getAllTables(connection);
        //console.log(tables)
        tables.forEach(async (row) => {
            const table = row[`Tables_in_${database}`];
            const columns = await getAllColumns(connection, table);

            console.log(columns);
            /*
            const hasComCodigo = columns.filter( column => column == 'COM_CODIGO').length > 0;

            if(!hasComCodigo) return;

            updateComCodigo(connection, table, comCodigo).then();
            */

        })
        /*
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
iniciarMigracion();