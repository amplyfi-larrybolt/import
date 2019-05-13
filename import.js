const glob = require('glob-promise');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

const url = 'mongodb://localhost:27017';

const importPath = './JSON/*.JSON';
const dbName = 'amplyfi';
const tableName = 'articles';

const splitInChunks = (inputArray, perChunk) => inputArray.reduce((resultArray, item, index) => {
    const chunkIndex = Math.floor(index / perChunk);

    if (!resultArray[chunkIndex]) {
        resultArray[chunkIndex] = [];
    }

    resultArray[chunkIndex].push(item);

    return resultArray;
}, []);

const importFromGlob = async (path, db) => {
    const files = await glob(path);

    const rows = files.map(file => JSON.parse(fs.readFileSync(file)));

    console.info(`There are ${rows.length} records to import`);
    const rowsInChunks = splitInChunks(rows, 100);
    console.info(`Split into ${rowsInChunks.length} chunks`);

    try {
        const collection = db.collection(tableName);
        for (let i = 0; i < rowsInChunks.length; i++) {
            console.info(`Importing ${i}/${rowsInChunks.length}`);
            await collection.insertMany(rowsInChunks[i]);
            console.log('done'+i);
        }
    } catch (err) {
        console.error(err);
    }
};
MongoClient.connect(url, function (err, client) {
    assert.equal(null, err);
    console.log('Connected successfully to server');

    const db = client.db(dbName);

    importFromGlob(importPath, db)
        .then(() => {
            console.info('done');
            client.close();
        })
        .catch(err => {
            console.error(err);
        });
});
