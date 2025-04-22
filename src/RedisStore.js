/**
 * RedisStore
 * 
 * RedisStore for koa2-ratelimit
 * 
 * @author Ashok Vishwakarma <akvlko@gmail.com>
 */

/**
 * Store
 * 
 * Existing Store class
 */
const Store = require('./Store.js');

/**
 * redis
 * 
 * node-redis module
 */
const redis = require('redis');

/**
 * RedisStore
 * 
 * Class RedisStore
 */
class RedisStore extends Store {
  /**
   * constructor
   * @param {*} config 
   * 
   * config is redis config
   */
  constructor(config){
    super();
    this.client = redis.createClient(config);
    this.client.on('error', (err) => console.log('Redis Client Error', err));
    this.client.connect()
    this.txAttempts = config.txAttempts || 1;
  }

  /**
   * _hit
   * @access private
   * @param {*} key 
   * @param {*} options 
   * @param {*} weight 
   */
  async _hit(key, options, weight) {

    let [counter, dateEnd] = [weight, Date.now() + options.interval];

    for (let attempts=0; attempts < this.txAttempts; attempts++ ){
      await this.client.watch(key);
      try{
        counter = await this.client.get(key);
        dateEnd =  await this.client.ttl(key);
      } catch (err) {
        console.log(err);
      }
    
      const seconds = Math.ceil(options.interval / 1000);
      if(counter === null || dateEnd === -2 || dateEnd === -1) {
        if ((dateEnd === -2 || dateEnd === -1)){
          counter = counter + weight;
        }
        try{
          await this.client.multi().setEx(key, seconds.toString(), counter.toString()).exec();
          break; //ends loop in case of success 
        } catch (err) {
          console.log(err);
        }
      } else {
        try{
          counter = await this.client.multi().incrBy(key, weight).exec();
          break; //ends loop in case of success
        } catch (err) {
          console.log(err);
        }
      }
    }

    return {
      counter,
      dateEnd
    }
  }

  /**
   * incr
   * 
   * Override incr method from Store class
   * @param {*} key 
   * @param {*} options 
   * @param {*} weight 
   */
  async incr(key, options, weight) {
    return await this._hit(key, options, weight);
  }

  /**
   * decrement
   * 
   * Override decrement method from Store class
   * @param {*} key 
   * @param {*} options 
   * @param {*} weight 
   */
  async decrement(key, options, weight) {
    await this.client.decrBy(key, weight);
  }

  /**
   * saveAbuse
   * 
   * Override saveAbuse method from Store class
   */
  saveAbuse() {}
}

module.exports = RedisStore;
