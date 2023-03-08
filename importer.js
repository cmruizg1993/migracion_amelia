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
];

const columnasEliminar = [
    
    {
        tabla: "com_encfac",
        columna: "ENCFAC_XML"
    },
    {
        tabla: "com_enclic",
        columna: "ENCLIC_XML"
    },
    {
        tabla: "com_encret",
        columna: "ENCRET_XML"
    },
    {
        tabla: "ven_encfac",
        columna: "ENCFAC_XML"
    },
    {
        tabla: "ven_encguia",
        columna: "ENCGUIA_XML"
    },
    {
        tabla: "ven_encncr",
        columna: "ENCNCR_XML"
    },
    {
        tabla: "ven_encndb",
        columna: "ENCNDB_XML"
    },
    {
        tabla: "ven_encpro",
        columna: "ENCPRO_XML"
    }
    
]

const tablasExportar = [
    "age_actividad",
    "age_actividad_documentos",
    "age_agenda_actividad",
    "age_comentarios",
    "age_frecuencia_mensual",
    "ban_maeban",
    "bot_orel_api",
    "com_codretproveedor",
    "com_detfac",
    "com_detlic",
    "com_detproveedor",
    "com_detret",
    "com_encfac",
    "com_encfac_datos_adicionales",
    "com_enclic",
    "com_enclic_datos_adicionales",
    "com_encret",
    "com_maeproveedor",
    "com_proveedores",
    "com_proveedores_ats",
    "compras",
    "config_sistema",
    "cxc_trncobro",
    "cxc_trnfacturas",
    "cxc_trnpago",
    "cxc_trnretenciones",
    "fac_cab",
    "fac_cab_infadi",
    "fac_cab_tci",
    "fac_det",
    "fac_det_detadi",
    "fac_det_imp",
    "inv_maearticulo",
    "inv_maeartprecio",
    "inv_maebodega",
    "inv_maegrupo",
    "inv_maeunidad",
    "mae_bancos",
    "mae_cuentasbancarias",
    "mae_establecimientos",
    "mae_puntoemision",
    "mae_puntoemision_detfij",
    "ncr_cab",
    "ncr_cab_infadi",
    "ncr_cab_tci",
    "ncr_det",
    "ncr_det_detadi",
    "ncr_det_imp",
    "ndb_cab",
    "ndb_cab_infadi",
    "ndb_cab_tci",
    "ndb_det",
    "ndb_det_detadi",
    "ndb_det_imp",
    "orel_noguardadorecibidos",
    "orel_trntxt",
    "orel_trnxml",
    "orel_txtemitidos",
    "orel_txtrecibidos",
    "ret_cab",
    "ret_cab_infadi",
    "ret_det_imp",
    "retenciones",
    "seg_maecompania",
    "seg_maeperfil",
    "seg_maeusuario",
    "tb_detegreso",
    "tb_detingreso",
    "tb_encegreso",
    "tb_encingreso",
    "ven_datosadicionalcliente",
    "ven_detfac",
    "ven_detguia",
    "ven_detncr",
    "ven_detndb",
    "ven_detpro",
    "ven_detproesp",
    "ven_encfac",
    "ven_encfac_datos_adicionales",
    "ven_encfac_formas_pago",
    "ven_encguia",
    "ven_encguia_datos_adicionales",
    "ven_encncr",
    "ven_encncr_datos_adicionales",
    "ven_encndb",
    "ven_encndb_datos_adicionales",
    "ven_encpro",
    "ven_encpro_datos_adicionales",
    "ven_encproesp",
    "ven_encproespeproveedor",
    "ven_maecliente",
    "ven_maedocumentoscliente",
    "ven_maegrupo",
    "ven_maetipocliente",
    "ven_maevendedor",
    "ventas"    
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
  const config = configAmeliaMasterImport();
    const databaseToExport = database;    
    const rootUser = config.user;
    const rootPassword = config.password;
    const tablas = tablasExportar.join(' ');
    return new Promise((res)=>{
        exec('mysqldump '+
            databaseToExport+
            ` --no-create-info`+
            ` --tables ${tablas}`+
            ` --result-file=${ resultFileName }`+ 
            ' -u ' + rootUser+
            ` -p${rootPassword}`, 
            
            (error, stdout, stderr) => {
                if (error) {
                    console.log(`error: ${error.message}`);
                    
                }
                if (stderr) {
                    console.log(`stderr: ${stderr}`);
                    
                }
                console.log(`stdout: ${stdout}`);
                res({error, stdout, stderr});
            }
        )
  });
}


const addPrimaryKey = async (tabla, pk, connection) => {

    const sqlQueryDrop = `ALTER TABLE ${tabla} DROP PRIMARY KEY;`;

    const dropResult = await executeQuery(sqlQueryDrop, connection, []);

    console.log(dropResult);

    const sqlQuery = `ALTER TABLE ${tabla} ADD COM_CODIGO INTEGER NOT NULL, ADD PRIMARY KEY (COM_CODIGO, ${pk})`;

    return executeQuery(sqlQuery, connection, [] );
}

const dropColumn = async(tabla, columna, connection) => {
    const sqlQueryDrop = `ALTER TABLE ${tabla} DROP COLUMN ${columna};`;

    return executeQuery(sqlQueryDrop, connection, [] );
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

const recrearBdd = async (database) => {
    
       
    const dumpFile = path.resolve('export', `${database}.sql`);

    if(!fs.existsSync(dumpFile)) {
        console.log(`Error: No se encontro el archivo: ${dumpFile}`)
        return;
    }
    console.log(`Creando base de datos: ${database}`)

    const result  = await createDatabase(database);

    console.log(`Se ha creado la base de datos: ${database}`)
    
    
    const { error1, stdout, stderr } = await importSql(database, dumpFile);
    
    if(error1 != null )
    {
        console.log('Error al importar archivo SQL');
        return;
    }

    console.log(`Se ha importado los datos de la base de datos: ${database}`)

}
const agregarLlavePrimaria = async (connection)=>{
    console.log('AGREGANDO COM_CODIGO COMO PK...');
    for(let i = 0; i < tablasComCodigo.length; i++){
        const row = tabla[0];
        await addPrimaryKey(row.tabla, row.pk, connection);
    }
    console.log(' SE HA AGREGANDO COM_CODIGO COMO PK...');
    
}

const actualizarComCodigo = async (connection, database, comCodigo) => {

    const tables = await getAllTables(connection);

    for( let i = 0; i < tables.length; i++){
        const row = tables[i];
        const table = row[`Tables_in_${database}`];
        const columns = await getComCodigoColumn(connection, table);

        
        const hasComCodigo = columns.length > 0;

        if(!hasComCodigo) return;
        console.log(`ACTUAIZANDO COM_CODIGO EN LA TABLA ${table}`);
        await updateComCodigo(connection, table, comCodigo).then();
        console.log(`TABLA ${table} ACTUALIZADA`);
    }
    
}

const eliminarColumnasXml = async (connection) => {
    console.log('ELIMINANDO COLUMNAS ...');
    for ( let i = 0; i < columnasEliminar.length; i++){
        const row = columnasEliminar[i];
        console.log({ tabla: row.tabla , columna: row.columna});
        await dropColumn(row.tabla, row.columna, connection);
    }
    console.log('SE HAN ELIMINADO LAS COLUMNAS CORRECTAMENTE !');
}

const iniciarMigracion = async ()=>{
    //Creación Amelia unificada

    const bddAmeliaUnificada = "ameliapro_test";

    for(let i = 0; i < rucs.length; i++){

        const ruc = rucs[i];
        const empresa = await getEmpresa(ruc);
        if(!empresa) return;

        console.log(`Importando BDD ${empresa.EMP_DBNAME} RUC ${empresa.EMP_RUC}`);
        
        const database = empresa.EMP_DBNAME;

        const comCodigo = empresa.EMP_CODIGO;

        await recrearBdd(database);

        // se crea la conexion directa la base de datos recreada

        const config = configAmeliaMasterImport();

        config.database = database;

        const connection = await createConnection(config);
        console.log(`ACTUAIZANDO BASE DE DATOS ${database}`);

        
        //se agrega la columna com_codigo como parte de la llave primaria de las tablas
        
        await agregarLlavePrimaria(connection);

        //se hace el update de la columna com_codigo en base al emp_codigo

        await actualizarComCodigo(connection, database, comCodigo);

        //se eliminan las columnas que sobrecargan la bdd
        await eliminarColumnasXml(connection);

        // Se exporta solo los datos de cada base de datos
        console.log('exportando los datos de: '+database)
        const { error } = await exportData(database);

        if(error) {
            console.log('Hubo en error al exportar los datos de '+database)
            return;
        }
        console.log('fin de la exportacion de datos de: '+database)
        
        // Se importa datos en Amelia unificada

        console.log('importando los datos de '+database+ ' en amelia unificada')
        const importDumpFile = path.resolve('import', `${database}.sql`);

        await importSql(bddAmeliaUnificada, importDumpFile);

        
        console.log(`FIN DE ACTUALIZACION BDD ${database}`);

    }
    
}
iniciarMigracion();