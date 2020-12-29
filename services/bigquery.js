const { BigQuery } = require("@google-cloud/bigquery");

const projectId = "twitter-299613";
const datasetId = "twitter";
const table_tweet = "tweet";
const table_context_annotations = "context_annotations";
const table_geo = "geo";
const table_entities_annotations = "entities_annotations";
const table_entities_hashtags = "entities_hashtags";
const table_entities_cashtags = "entities_cashtags";
const table_entities_mentions = "entities_mentions";
const table_public_metrics = "public_metrics";

async function insertRowsAsStream(tableId, rows) {
  const bigqueryClient = new BigQuery();

  // Insert data into a table
  try {
    const result = await new Promise((resolve, reject) => {
      bigqueryClient
        .dataset(datasetId)
        .table(tableId)
        .insert(rows)
        .then((results) => {
          console.log(`Inserted ${rows.length} rows`);
          resolve(rows);
        })
        .catch((err) => {
          reject(err);
        });
    });
  } catch (error) {
    console.log("----JSON Error ---", JSON.stringify(error));
    throw new Error(error);
  }
}

async function insertTweet(tweet, ruleCategory) {
  const rows = [
    {
      id: tweet.id,
      text: tweet.text,
      author_id: tweet.author_id,
      conversation_id: tweet.conversation_id,
      created_at: BigQuery.datetime(tweet.created_at),
      in_reply_to_user_id: tweet.in_reply_to_user_id,
      lang: tweet.lang,
      possiby_sensitive: Boolean(tweet.possiby_sensitive),
      reply_settings: tweet.reply_settings,
      source: tweet.source,
      category : ruleCategory
    },
  ];
  insertRowsAsStream(table_tweet, rows);

  var public_metrics = tweet.public_metrics;
  if (public_metrics) {
    const pm_rows = [
      {
        tweet_id: tweet.id,
        retweet_count: parseInt(public_metrics.retweet_count),
        reply_count: parseInt(public_metrics.reply_count),
        like_count: parseInt(public_metrics.like_count),
        quote_count: parseInt(public_metrics.quote_count),
      },
    ];
    insertRowsAsStream(table_public_metrics, pm_rows);
  }

  var entities = tweet.entities;
  if (entities) {
    var annotations = entities.annotations;
    if (
      annotations != undefined &&
      Array.isArray(annotations) &&
      annotations.length
    ) {
      const annotateRows = [];
      annotations.forEach(function (annotate, index) {
        if (annotate) {
          annotateRows.push({
            tweet_id: tweet.id,
            probability: parseFloat(annotate.probability),
            type: annotate.type,
            text: annotate.text,
          });
        }
      });
      insertRowsAsStream(table_entities_annotations, annotateRows);
    }

    var hashtags = entities.hashtags;
    if (
        hashtags != undefined &&
        Array.isArray(hashtags) &&
        hashtags.length
      ) {
        const hashRows = [];
        hashtags.forEach(function (hash, index) {
          if (hash) {
            hashRows.push({
              tweet_id: tweet.id,
              tags: hash.tag
            });
          }
        });
        insertRowsAsStream(table_entities_hashtags, hashRows);
      }

      var cashtags = entities.cashtags;
      if (
          cashtags != undefined &&
          Array.isArray(cashtags) &&
          cashtags.length
        ) {
          const cashRows = [];
          cashtags.forEach(function (cash, index) {
            if (cash) {
              cashRows.push({
                tweet_id: tweet.id,
                tags: cash.tag
              });
            }
          });
          insertRowsAsStream(table_entities_cashtags, cashRows);
        }    

        var mentions = entities.mentions;
        if (
            mentions != undefined &&
            Array.isArray(mentions) &&
            mentions.length
          ) {
            const mentionRows = [];
            mentions.forEach(function (mention, index) {
              if (mention) {
                mentionRows.push({
                  tweet_id: tweet.id,
                  username: mention.username
                });
              }
            });
            insertRowsAsStream(table_entities_mentions, mentionRows);
          }    

  }

  var context_annotations = tweet.context_annotations;
  if (
    context_annotations != undefined &&
    Array.isArray(context_annotations) &&
    context_annotations.length
  ) {
    context_annotations.forEach(function (ca_domain, index) {
      if (ca_domain) {
        const ca_rows = [
          {
            tweet_id: tweet.id,
            type: "DOMAIN",
            id: ca_domain.domain.id,
            name: ca_domain.domain.name,
            desc: ca_domain.domain.description,
          },
          {
            tweet_id: tweet.id,
            type: "ENTITY",
            id: ca_domain.entity.id,
            name: ca_domain.entity.name,
            desc: ca_domain.entity.description,
          },
        ];
        insertRowsAsStream(table_context_annotations, ca_rows);
        //        console.log("== CA_ROWS == ", JSON.stringify(ca_rows));
      }
    });
  }
}

module.exports = { insertTweet };
