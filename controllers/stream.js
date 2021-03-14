const express = require("express");
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const needle = require('needle');
const bqsvcs = require('.././services/bigquery.js');
const hptbq_svcs = require('.././services/hpt-bq-test.js');

var ruleCategory;

const bearerToken = 'Bearer AAAAAAAAAAAAAAAAAAAAAN1GKgEAAAAAPJSDmoI8hY9vB6ZgxeBgU9OVSrM%3DMRUYjL7LktexG7QAMMy0UaCkfYmVzEIyi1juXQePwADiMBkjFE'

axiosRetry(axios, {
  retries: 3,
  retryDelay: axiosRetry.exponentialDelay,
  shouldResetTimeout: true,
  retryCondition: (axiosError) => {
    return true;
  },
});

const router = express.Router();

router.get("/", function (req, res) {
  streamTweets();
  res.send("Now streaming tweets ..");
});

router.post("/rules", function (req, res) {
  let config = {
    method: 'post',
    url: 'https://api.twitter.com/2/tweets/search/stream/rules',
    headers: { 'Authorization' : bearerToken},
    data: req.body
  };
  axios(config)
  .then(function (response) {
    console.log('inside method',response.data.data);
    res.json(response.data.data);
  })
  .catch(function (error) {
    console.log(error);
  });  
});

router.get("/rules", function (req, res) {
  console.log('get rules')
  var rulesData;
  let config = {
    method: 'get',
    url: 'https://api.twitter.com/2/tweets/search/stream/rules',
    headers: { 'Authorization' : bearerToken}
  };

  axios(config)
    .then(function (response) {
      rulesData = response.data.data;
      res.send(rulesData);
    })
    .catch(function (error) {
      console.log(error);
    });  
});

async function streamTweets() {
    const streamURL = "https://api.twitter.com/2/tweets/search/stream";

    //Listen to the stream
    const options = {
        timeout: 20000
      }
    
    const stream = needle.get(streamURL, {
        headers: { 
            Authorization: bearerToken
        }
    }, options);

    stream.on('data', data => {
    try {
        const json = JSON.parse(data);
        //console.log('Matching rules ',json.matching_rules[0].tag);
        ruleCategory = json.matching_rules[0].tag;
        processTweets(json);
    } catch (e) {
        // Keep alive signal received. Do nothing.
    }
    }).on('error', error => {
        if (error.code === 'ETIMEDOUT') {
            stream.emit('timeout');
        }
    });

    return stream;
    
}

async function processTweets(jsonTweet)  {
    let tweetId = jsonTweet.data.id;
    console.log('tweet --',tweetId);
    getSingleTweet(tweetId);
  }

async function getSingleTweet(id) {
    let tweetData;
    let config = {
        method: 'get',
        url: 'https://api.twitter.com/2/tweets/'+id+'?tweet.fields=author_id,created_at,context_annotations,public_metrics,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,referenced_tweets,source,text,withheld',
        headers: { 'Authorization' : bearerToken}
      };
      
      axios(config)
      .then(function (response) {
//        console.log('-- Single tweet -- ',JSON.stringify(response.data.data));
        tweetData = response.data.data;
        bqsvcs.insertTweet(response.data.data,ruleCategory);
      })
      .catch(function (error) {
        console.log(error);
      });
  }

  router.get("/hpt", function (req, res) {
    loadHTPData();
    res.send("Loading HPT tweets ..");
  });

  async function loadHTPData()  {
    hptbq_svcs.loadJSONFromGCS()
    .then(function(response)  {
      console.log('Response ',response);
    })
    .catch(function (error) {
      console.log(error);
    });
  }

module.exports = router;
