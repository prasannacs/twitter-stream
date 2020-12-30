const express = require("express");
const axios = require("axios").default;
const axiosRetry = require("axios-retry");
const needle = require('needle');
const bqsvcs = require('.././services/bigquery.js');
var ruleCategory;

const bearerToken = 'Bearer ZZxcffxcxcvcv%2Fs1P9XZltxpgGbs7ERsQ%3DR85fPDZZk764urVLGQzyVkgyJJpgS39MKOBneEMqPz6beY9oGp'

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

module.exports = router;
