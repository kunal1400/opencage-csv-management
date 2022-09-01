const axios = require("axios");
const sqlite3 = require('sqlite3');
const fs = require('fs');
const { parse } = require("csv-parse");

// CSV Indexes
const startIndex = 0;
const endIndex = 10000;

// Configuring the sqlite database
const SQLite3 =  sqlite3.verbose();
const db = new SQLite3.Database('posts.db');

/**
 * Promised version of the sqlite3 driver
 * 
 * @param {*} command 
 * @param {*} method 
 * @returns 
 */
const query = (command, method = 'all') => {
    return new Promise((resolve, reject) => {
        db[method](command, (error, result) => {
            if (error) {
                reject(error);
            } else {
                resolve(result);
            }
        });
    });
};

/**
 * Creating table if not exist
 */
db.serialize(async () => {    
    await query("CREATE TABLE IF NOT EXISTS posts (Title,Owner_Name,Google_Address,Phone,Email,Category,Gallery,id,Location,Lat,Lng)", 'run');
});

/**
 * Finding record by id
 * 
 * @param {*} id 
 * @returns 
 */
const findRecord = async (id) => {
    return await query(`SELECT * FROM posts WHERE id = '${id}'`);
}

/**
 * Inserting record in db
 * 
 * @param {*} recordObject 
 * @returns 
 */
const insertRecord = async ( recordObject ) => {
    const keys = Object.keys( recordObject ).join(', ');
    const values = Object.values( recordObject ).map((d) => `'${d}'`).join(', ');
    const sql = `INSERT INTO posts (${keys}) VALUES (${values})`;
    return await query(sql);
}

/**
 * Reading csv data row by row and inserting record in db
 */
var parser = parse({ columns: true }, async function (err, records) {
    try {
        if(err) {
            console.log(err, "Err")
        } else {
            for (let i = startIndex; i < endIndex; i++) {
                if ( !records[i].id ) {
                    console.log(`id is not present in csv`);
                } 
                else {
                    const csvRow = records[i];
                    const recordFromDb = await findRecord( csvRow.id );                    

                    if( recordFromDb.length === 0 ) {
                        // API Url
                        const URL = `https://api.opencagedata.com/geocode/v1/json?q=${encodeURIComponent(csvRow?.Google_Address)}&key=ef935e6fa4b048e2beff131b156c0318`;
                        
                        // Calling API and saving response in db
                        const recordToInsert = await axios.get(URL).then((response) => {
                            if ( response && response.data && response.data.results && response.data.results.length > 0 ) {
                                const data = response.data.results.map((e) => {
                                    return e;
                                });
    
                                if ( data && data.length > 0 && data[0].geometry) {
                                    const fetchedLat = data[0].geometry.lat ? data[0].geometry.lat : "undefined";
                                    const fetchedLng = data[0].geometry.lng ? data[0].geometry.lng : "undefined";                                
                                    return { ...csvRow, Lat: fetchedLat, Lng: fetchedLng }
                                } else {
                                    return csvRow;
                                }
                            } else {
                                console.log(`Record not found for this address: `, csvRow.Google_Address);
                                return csvRow;
                            }
                        });
    
                        // Inserting only one record in db
                        const insertResponse = await insertRecord(recordToInsert);
    
                        console.log(insertResponse, "<== insertResponse")
                    }
                    else {
                        console.log(csvRow.id, "Record Already in db")
                    }
                }
            }
        }    
    } catch (error) {
        console.log(error);
    }
    
});

fs.createReadStream(__dirname + '/import-file.csv').pipe(parser);
