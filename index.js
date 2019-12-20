const mongodb = require("mongodb");
const redis = require("redis");

/*
  ENVIRONMENT VARIABLES
  1. MONGODB_URI (Mongodb Connection Uri)
  3. MONGODB_DB_NAME
  3. MONGODB_COLLECTION_NAME
  2. REDIS_IP_BLOCK_KEY
  2. REDIS_HOST (Redis Host name it can be multiple by comma separated)
  2. REDIS_PORT (Redis PORT it can be multiple by comma separated)
 */

const populateRedis = (data, host, port) =>{
    return new Promise((resolve, reject) => {
        const client = redis.createClient(port, host);
        client.del(process.env.REDIS_IP_BLOCK_KEY, (err)=>{
            if(err) return reject(err);
            if(!Array.isArray(data) || data.length === 0) return resolve(1);
            client.sadd(process.env.REDIS_IP_BLOCK_KEY, data, (err, res)=>{
               if(err) return reject(err);
               return resolve(res);
            });
        });
    });
};

const buildResponse = (code, body) =>{
    return {
        statusCode: code,
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
    }
};

exports.handler = async (event, context) =>{
    if(process.env.DEBUG && process.env.DEBUG === "1"){
        console.log("Lambda Event: ", event);
        console.log("Lambda Context: ", context);
    }
    if(!process.env.MONGODB_URI || !process.env.MONGODB_DB_NAME || !process.env.MONGODB_COLLECTION_NAME){
        throw new Error("Mongodb Connection Config {uri, db, collection_name} Required");
    }
    if(!process.env.REDIS_HOST || !process.env.REDIS_IP_BLOCK_KEY){
        throw new Error("Redis Host Required");
    }
    try {
        let redisHOSTS = process.env.REDIS_HOST.split(",").filter(el=> !!el);
        let redisPORTS = (process.env.REDIS_PORT || '').split(",").filter(el=> !!el);
        return buildResponse(200, await new Promise((resolve, reject) => {
            mongodb.connect(process.env.MONGODB_URI, {useUnifiedTopology: true}, (err, client) => {
                if (err) return reject(err);
                client.db(process.env.MONGODB_DB_NAME).collection(process.env.MONGODB_COLLECTION_NAME)
                    .find({cidr: {$exists: true}}).toArray((err, result) => {
                    if (err) return reject(err);
                    let r = result.map(el => el.cidr);
                    const p = Promise.all(redisHOSTS.map((el, i) => {
                        return populateRedis(r, el, redisPORTS[i] || 6379)
                    }));
                    p.then(res => resolve(res)).catch(err => reject(err));
                });
            });
        }));
    }catch (e) {
        return buildResponse(500, {err: e});
    }
};



