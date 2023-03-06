import mysqldump from 'mysqldump';
import fs from 'fs';
import path from 'path';
import Importer from 'mysql-import';
import mysql from 'mysql2';
const connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    database: 'mysql'
  });
const database = 'ameliapro_master';
const sqlCreateDatabase = `CREATE DATABASE /*!32312 IF NOT EXISTS*/ ${database} /*!40100 DEFAULT CHARACTER SET latin1 */ /*!80016 DEFAULT ENCRYPTION='N' */;`;
connection.execute(sqlCreateDatabase, [], (err, result)=>{
    if(err) {
        console.log(err)
        return;
    }
    console.log(result);
})
const fileName = path.resolve('./dump.sql');

console.time('Timer')
const configAmelia =  {
    user: "support",
    password: "Soporte@2022",
    database: "ameliapro_master",
    host: "186.4.146.197",
    port: "4198",
}
//const file = fs.createWriteStream(fileName)
const config = {
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'restaurante',
}
mysqldump({
    connection: configAmelia,
    dumpToFile: fileName,
    //compressFile: true,
}).then(()=>{
    console.timeLog('Timer')
    console.timeEnd('Timer')

})
/*
const host = 'localhost';
const user = 'root';
const password = 'password';
const database = 'mydb';


const importer = new Importer({host, user, password, database});

importer.onProgress(progress=>{
  var percent = Math.floor(progress.bytes_processed / progress.total_bytes * 10000) / 100;
  console.log(`${percent}% Completed`);
});

importer.import('path/to/dump.sql').then(()=>{
  var files_imported = importer.getImported();
  console.log(`${files_imported.length} SQL file(s) imported.`);
}).catch(err=>{
  console.error(err);
})
*/