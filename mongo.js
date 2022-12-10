const { MongoClient } = require("mongodb");
let env = process.env.NODE_ENV || "development";
let url;

let client, db;
async function connect(useEnv, uri) {
  try {
    if (useEnv) env = useEnv;
    url =
      env === "development" ? process.env.MONGO_URL_DEV : process.env.MONGO_URL;
    if (uri) url = uri;
    if (!url) console.log("URL undefined");
    client = new MongoClient(url);
    console.log(`Connecting to the ${env} database.`);
    console.log(url);
    await client.connect();
    console.log("Connected.");
    db = client.db("ArkRecordWiki");
    return client;
  } catch (e) {
    client.close().catch(console.log);
    console.log("Error ", e);
    throw e;
  }
}

async function close() {
  await client.close();
  console.log("Database client closed successfully");
}

function getClient() {
  return client;
}

function getCollection(collectionName) {
  return db.collection(collectionName);
}

async function _insertDocuments(collection, documents) {
  const insertedIds = [];
  const bulkWrites = [];
  
  await documents.forEach((doc) => {
    const { _id } = doc;
    
    insertedIds.push(_id);
    bulkWrites.push({
      replaceOne: {
        filter: { _id },
        replacement: doc,
        upsert: true,
      },
    });
  });
  
  if (bulkWrites.length)
    await collection.bulkWrite(bulkWrites, { ordered: false });
  
  return insertedIds;
}

async function _deleteDocuments(collection, documents) {
  const bulkWrites = [];
  
  await documents.forEach(({ _id }) => {
    bulkWrites.push({
      deleteOne: {
        filter: { _id },
      },
    });
  });
  
  if (bulkWrites.length)
    await collection.bulkWrite(bulkWrites, { ordered: false });
}

async function moveDocuments(src_collection, tgt_collection, query) {
  try {
    const _src_collection = getCollection(src_collection);
    const _tgt_collection = getCollection(tgt_collection);
    const _src_docs = await _src_collection.find(query);
    // console.log(
    //   `Collection ready, moving ${await _src_docs.count()} documents from ${
    //     _src_collection.collectionName
    //   } to ${_tgt_collection.collectionName}`
    // );
    
    const idsOfCopiedDocs = await _insertDocuments(_tgt_collection, _src_docs);
    const _tgt_docs = await _tgt_collection.find({
      _id: { $in: idsOfCopiedDocs },
    });
    await _deleteDocuments(_src_collection, _tgt_docs);
  } catch (err) {
    console.log(err);
    
    throw err;
  }
}

module.exports = {
  connect,
  close,
  getClient,
  getCollection,
  moveDocuments,
};
