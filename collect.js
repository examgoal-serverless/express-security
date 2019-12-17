const mongodb = require("mongodb");
const redis = require("redis");
const Utils = require("./lib/utils");

/*
  ENVIRONMENT VARIABLES
  1. MONGODB_URI (Mongodb Connection Uri)
  3. MONGODB_DB_NAME
  3. MONGODB_COLLECTION_NAME
  2. REDIS_PREFIX
  2. REDIS_HOST (Redis Host name it can be multiple by comma separated)
  2. REDIS_PORT (Redis PORT it can be multiple by comma separated)
 */

const getValueFromKey = (client, k) =>{
    return new Promise((resolve, reject) => {
        client.get(k, (err, res)=>{
            if(err) return reject(err);
            return resolve(res);
        })
    });
};

const deleteKey = (client, k) =>{
    return new Promise((resolve, reject) => {
         client.del(k, (err, res)=>{
             if(err) return reject(err);
             return resolve(res);
         })
    });
};

const collectFromRedis = (client, host, port) =>{
    return new Promise((resolve, reject) => {
        const RedisClient = redis.createClient(port, host);
        RedisClient.keys((process.env.REDIS_PREFIX || '')+"request:*", async (err, keys)=>{
           if(err) return reject(err);
           try {
              let keyValues = await Promise.all(keys.map(el=> getValueFromKey(RedisClient, el)));
              await Promise.all(keys.map(el=> deleteKey(RedisClient, el)));
              await Promise.all(keys.map((el, i)=>{
                  const ip = el.replace((process.env.REDIS_PREFIX || '')+"request:", "");
                  return client.insertOne({ip: ip, count: keyValues[i] || 0, time: new Date()})
              }));
              return resolve(true);
           }catch (e) {
               return reject(e);
           }
        });
    });
};

exports.handler = async (event, context)=>{
    if(process.env.DEBUG && process.env.DEBUG === "1"){
        console.log("Lambda Event: ", event);
        console.log("Lambda Context: ", context);
    }
    if(!process.env.MONGODB_URI || !process.env.MONGODB_DB_NAME || !process.env.MONGODB_COLLECTION_NAME){
        throw new Error("Mongodb Connection Config {uri, db, collection_name} Required");
    }
    if(!process.env.REDIS_HOST){
        throw new Error("Redis Host and prefix Required");
    }
    try {
        let redisHOSTS = process.env.REDIS_HOST.split(",").filter(el=> !!el);
        let redisPORTS = (process.env.REDIS_PORT || '').split(",").filter(el=> !!el);
        return Utils.buildResponse(200, await new Promise((resolve, reject) => {
            mongodb.connect(process.env.MONGODB_URI, {useUnifiedTopology: true}, (err, client) => {
                if (err) return reject(err);
                const p = Promise.all(redisHOSTS.map((el, i) => {
                    const c = client.db(process.env.MONGODB_DB_NAME).collection(process.env.MONGODB_COLLECTION_NAME);
                    return collectFromRedis(c, el, redisPORTS[i] || 6379);
                }));
                p.then(res => resolve(res)).catch(err => reject(err));
            });
        }));
    }catch (e) {
        return Utils.buildResponse(500, {err: e});
    }
};