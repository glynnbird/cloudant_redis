
import { Writable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { ChangesFollower, CloudantV1 } from '@ibm-cloud/cloudant'
import { createClient } from 'redis'

// which Cloudant database to cache
const CLOUDANT_DATABASE = process.env.CLOUDANT_DATABASE
const REDIS_URL = process.env.REDIS_URL

const main = async () => {
  if (!CLOUDANT_DATABASE) {
    console.error('Missing required environment variable: CLOUDANT_DATABASE')
    process.exit(1)
  }

  // connect to redis
  const redisClient = await createClient(REDIS_URL ? { url: REDIS_URL } : undefined)
    .on('error', (err) => console.log('Redis Client Error', err))
    .connect()

  // create a cloudant-node-sdk client - configuration via env variables
  const client = CloudantV1.newInstance({})

  // Enable automatic retries (with max retries 5, max retry interval 20 seconds).
  client.enableRetries({ maxRetries: 3, maxRetryInterval: 20 });

  // create a ChangesFollower, starting from the beginning of the
  // database's changes feed with the document body included.
  const changesParams = {
    db: CLOUDANT_DATABASE, // the database to monitor
    since: '0', // the sequence token defining where in the changes feed to begin
    includeDocs: true // return full document bodies with the change
  }
  const changesFollower = new ChangesFollower(client, changesParams)

  // start the changes feed - which generates a stream of changes
  // the `start` function runs "forever", whereas `startOneOff` runs
  // until no more changes are found.
  const changesItemsStream = changesFollower.start()

  // Create a writable stream to handle the stream of changes.
  // The 'write' function is called once for each change. Its
  // 'callback' function is called when processing is complete,
  // a mechansim used for flow control in the pipeline.
  const destinationStream = new Writable({
    objectMode: true,
    async write(changesItem, _, callback) {
      // do something with change item, in this case log the change
      const key = `${CLOUDANT_DATABASE}/${changesItem.doc._id}`

      if (changesItem.deleted) {
        console.log('DELETE', key)
        await redisClient.del(key)
      } else {
        console.log('UPSERT', key)
        await redisClient.set(key, JSON.stringify(changesItem.doc))
      }

      // call back to say we're done processing this change
      callback()
    }
  })

  // create a simple pipeline, connecting the changes feed stream
  // to our writable stream. The pipeline returns a promise which
  // resolves when all the changes are consumed, or rejects on an
  // error condition. As we started the Changes Follower with a
  // call to `start()`, then this promise will never resolve.
  pipeline(changesItemsStream, destinationStream)
    .then(() => {
      console.log('Stopped')
    })
    .catch((err) => {
      console.log(err)
    })
}

main()
