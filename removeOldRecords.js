const mongodb = require("mongodb");
const Utils = require("./lib/utils");

/*
  ENVIRONMENT VARIABLES
  1. MONGODB_URI (Mongodb Connection Uri)
  3. MONGODB_DB_NAME
  3. MONGODB_COLLECTION_NAME
 */

const getDbClient = () =>{
    return new Promise((resolve, reject) => {
        mongodb.connect(process.env.MONGODB_URI, {useUnifiedTopology: true}, (err, client) => {
            if (err) return reject(err);
            return resolve(client.db(process.env.MONGODB_DB_NAME).collection(process.env.MONGODB_COLLECTION_NAME));
        });
    });
};

const aggregate = async (client, timeRange, group, step) =>{
        let q = {
            time: timeRange,
            ag: step === 1 ? {$exists: false} : "1"
        };
        let records = await client.aggregate([
            {$match: q},
            { $group: {
                    "_id": {
                        time: {
                            "$toDate": {
                                "$subtract": [
                                    { "$toLong": "$time" },
                                    { "$mod": [ { "$toLong": "$time" }, group] }
                                ]
                            }
                        },
                        ip: "$ip"
                    },
                    "count": { "$sum": "$count" }
                }
            }]).toArray();
        if(records.length > 0){
            let insertOp = await Promise.all(records.map(el=>{
                return client.insertOne({ip: el._id.ip, count: el.count, time: el._id.time, ag: step === 1 ? "1" : "2"});
            }));
            if(insertOp.length > 0 && insertOp[0].result.ok === 1){
                await client.deleteMany(q);
            }
        }
        return true;
};

exports.handler = async (event, context) =>{
    if(process.env.DEBUG && process.env.DEBUG === "1"){
        console.log("Lambda Event: ", event);
        console.log("Lambda Context: ", context);
    }
    if(!process.env.MONGODB_URI || !process.env.MONGODB_DB_NAME || !process.env.MONGODB_COLLECTION_NAME){
        throw new Error("Mongodb Connection Config {uri, db, collection_name} Required");
    }
    try {
        return Utils.buildResponse(200, await new Promise(async (resolve, reject) => {
            try {
                const client = await getDbClient();
                const oneHour = 60 * 60 * 1000;
                const oneDay = 24 * oneHour;
                const currentTime = Date.now();
                // Remove before last 28 days record
                await client.deleteMany({time: {$lt: new Date(currentTime - 48 * oneDay)}});
                // Get records of last 48 hours, group by day and ip
                let timeRange = {$lt: new Date(currentTime - 2 * oneHour), $gt: new Date(currentTime - 2 * oneDay)};
                await aggregate(client, timeRange, oneHour, 1);
                // Get records of last 28 days, group by day and ip
                timeRange = {$lt: new Date(currentTime - 2 * oneDay)};
                await aggregate(client, timeRange, oneDay, 2);
                return resolve(true);
            }catch (e) {
                return reject(e);
            }
        }));
    }catch (e) {
        return Utils.buildResponse(500, {err: e.toLocaleString()});
    }
};