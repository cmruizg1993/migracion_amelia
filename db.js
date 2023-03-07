import mysql from 'mysql2';
import util from 'util';

export const configAmeliaMasterExport = () => {
    return  {
        user: "support",
        password: "Soporte@2022",
        database: "ameliapro_master",
        host: "localhost",//"186.4.146.197",
        port: "3306"//"4198",   
    };
}


export const configAmeliaMasterImport = () => {
    return {
        user: "support",
        password: "Soporte@2022",
        database: "ameliapro_master",
        host: "localhost",//"186.4.146.197",
        port: "3306"//"4198",   
    }
};

/**
 * 
 * @param {string} sqlQuery 
 * @param {mysql.Connection} connection 
 * @param {any} params 
 * @returns {any}
 */
export const executeQuery = (sqlQuery, connection, params) => {
    
    return connection.execute(sqlQuery, params); 

}


/**
 * 
 * @param {mysql.ConnectionOptions} config 
 * @returns { mysql.Connection }
 */
export const createConnection = (config) => {
    const connection = mysql.createConnection(config);
    connection.execute = util.promisify(connection.execute);
    return connection;
}

export const exportDb = createConnection(configAmeliaMasterExport());
export const importDb = createConnection(configAmeliaMasterImport());

