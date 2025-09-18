# cloudant_redis

A simple changes feed follower that caches Cloudant documents in Redis.

The Redis keys are of the form:

```
<database name>/<document id>
```

The changes feed is followed any deleted documents result in the associated Redis key being deleted too.

## Running

Set credentials in environment variables

```sh
export CLOUDANT_URL="https://mycloudant.url.com"
export CLOUDANT_APIKEY="myIAMapikey"
export CLOUDANT_DATABASE="mydatabase"
export REDIS_URL="redis://localhost:6379"
```

Run the script

```sh
node index.js
```
