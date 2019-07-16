const
  express       = require('express'),           // the Express HTTP framework
  app           = express();
  require('express-ws')(app);   

const
  async         = require('async'),
  redis         = require('redis'),             // node_redis to manage the redis connection
  client        = redis.createClient(),

  EventEmitter  = require('eventemitter2').EventEmitter2,  // Allows for events with wildcards
  evEmitter     = new EventEmitter({            // init event emitter
    wildcard    : true
  });

var Twitter = require('twitter');
 
var tclient = new Twitter({
  consumer_key: '',
  consumer_secret: '',
  access_token_key: '',
  access_token_secret: '' 
});

var stream = tclient.stream('statuses/filter', {track: 'trump'});

stream.on('data', function(tweet) {
  console.log(tweet);
});

let perSec = 0; 

stream.on('data', function (tw) {             // twitter emits a 'tweet' event when one comes in
  "use strict";
  perSec += 1;                                       
  let 
    tweetEntities = tw.entities,                        
    mentions = tweetEntities.user_mentions
      .map((mention) => mention.screen_name)  // extract mentions
      .join(','),                             // results in a comma delimited list of screen names mentioned
    urls = tweetEntities.urls           
      .map((aUrlEntity) => aUrlEntity.url)    // extract URL
      .join(',');                             // results in a comma delimited list of URLs

  client.xadd('tweets',                       // add to the stream `tweets`
    '*',                                      // at the latest sequence
    'id',tw.id_str,                           // stream field `id` with the tweet id (in string format because JS is bad with big numbers!)
    'screen_name',tw.user.screen_name,        // stream field `screen_name` with the twitter screen name
                                              // stream field `text` with either the extended (>140 chars), if present, or normal if (<140 chars)
    'text',(tw.extended_tweet && tw.extended_tweet.full_text) ? tw.extended_tweet.full_text : tw.text,
    'mentions',mentions,                      // stream field `mentions` with the mentions comma delimited list
    'urls',urls,                              // stream field `urls` with urls comma delimited list
    function(err) {
        if (err) { throw err; }               // handle any errors - a production service would need better error handling.
    }
  );
});

setInterval(function() {                     // simple interval to respond incoming tweets per second
  "use strict";
  console.log('Tweets per/sec',perSec);
  perSec = 0;                                // reset the counter
},1000); 

stream.on('error', function (err) {          // Handle errors, just in case.
  "use strict";
  console.error('Twitter Error', err);       // Twitter does have quite a few limits, so it's very possible to exceed them
});                                          // Twitter stream module is not robust - 
                                             // dropped connections may not result in errors being thrown. Use with caution.

// If you wanted to make it fully resilient, you would store this in Redis or some other data store
async.forever(                               // repeat forever
  function(done) {                           // done says we're done processing ...but it could cause problems. Will revise in next version.
    client.xread(                            // Read from the streams
      'BLOCK',                               // in a BLOCKing fashion - e.g. block the redis client until we get data. This isn't in the array because of quirk in how node_redis sees commands - usually the first command is always the key
      5000,
      'STREAMS',
      'tweets',
      '$'
    );
    // evEmitter.on('tweets', () => console.log('Tweet event occured'));
    // evEmitter.emit('tweets');
  },

  function(err) {  
    throw err;                             // this should only happen if there is an error
    // do more here                        // otherwise, throw.
  }
);
  
app.ws('/',function(ws,req) {
  let proxyToWs = function(data) {           // when we get the wildcard event, this is run
    if (ws.readyState === 1) {               // make sure the websocket is not closed
      "use strict";
      ws.send("Sending data from server");
      // ws.send(JSON.stringify(data.filter((ignore,i) => i % 2 ))); // send the data - we actually don't need the ranking (just the order), so we can filter out ever other result!
    }
    
    ws.on('message',function(someData) {                                                  // when we get a websocket message
      outClient.xadd(                                                                     // add to a stream
        'search-query',                                                                   // to our stream of queries
        '*',                                                                              // at the most recent sequence
        'queryTerms',                                                                     // our field
        someData,                                                                         // our query
        'queryId',                                                                        // query ID field
        req.params.queryId,                                                               // query ID value
        function(err) {
          if (err) { console.error(err); } else {                                         // log the error if we have one
            ws.send('QUERY SENT');                                                        // notify the client that we've sent the query
          }
        }
      );
    })

  };

  evEmitter.on('tweet-data', proxyToWs);     // do the actual event assignment

  ws.on('close',function() {                 // gracefully handle the closing of the websocket
    evEmitter.off('tweet-data',proxyToWs);   // so we don't get closed socket responses
  });

})

app                                          // our server app
  .use(express.static('static'))             // static pages (HTML)
  .listen(4000);
