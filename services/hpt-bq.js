const { BigQuery } = require("@google-cloud/bigquery");
const { Storage } = require('@google-cloud/storage');

// Instantiate clients
const bigquery = new BigQuery();
const storage = new Storage();

const bucketName = 'hpt-results-bucket';
const filename = 'job-w06t7dhh4j/activities.json';

async function loadJSONFromGCS() {
  // Imports a GCS file into a table with manually defined schema.
  const datasetId = "HPT";
  const tableId = "hpt_tweets3";

  const metadata = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    schema: {
      fields: [
        { name: 'created_at', type: 'STRING', mode: 'REQUIRED' },
        { name: 'id', type: 'INT64', mode: 'REQUIRED' },
        { name: 'id_str', type: 'STRING' },
        { name: 'text', type: 'STRING', mode: 'REQUIRED' },
        { name: 'source', type: 'STRING', mode: 'NULLABLE' },
        { name: 'truncated', type: 'BOOL', mode: 'NULLABLE' },
        { name: 'in_reply_to_status_id', type: 'INT64', mode: 'NULLABLE' },
        { name: 'in_reply_to_status_id_str', type: 'STRING', mode: 'NULLABLE' },
        { name: 'in_reply_to_user_id', type: 'INT64', mode: 'NULLABLE' },
        { name: 'in_reply_to_user_id_str', type: 'STRING', mode: 'NULLABLE' },
        { name: 'in_reply_to_screen_name', type: 'STRING', mode: 'NULLABLE' },
        {
          name: 'user', type: 'RECORD', mode: 'REPEATED',
          "fields": [
            { name: 'id', type: 'INT64', mode: 'REQUIRED' },
            { name: 'id_str', type: 'STRING', mode: 'REQUIRED' },
            { name: 'name', type: 'STRING', mode: 'REQUIRED' },
            { name: 'screen_name', type: 'STRING', mode: 'NULLABLE' },
            { name: 'location', type: 'STRING', mode: 'NULLABLE' },
            { name: 'url', type: 'STRING', mode: 'NULLABLE' },
            { name: 'description', type: 'STRING', mode: 'NULLABLE' },
            { name: 'translator_type', type: 'STRING', mode: 'NULLABLE' },
            { name: 'protected', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'verified', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'followers_count', type: 'INT64', mode: 'NULLABLE' },
            { name: 'friends_count', type: 'INT64', mode: 'NULLABLE' },
            { name: 'listed_count', type: 'INT64', mode: 'NULLABLE' },
            { name: 'favourites_count', type: 'INT64', mode: 'NULLABLE' },
            { name: 'statuses_count', type: 'INT64', mode: 'NULLABLE' },
            { name: 'created_at', type: 'STRING', mode: 'NULLABLE' },
            { name: 'utc_offset', type: 'STRING', mode: 'NULLABLE' },
            { name: 'time_zone', type: 'STRING', mode: 'NULLABLE' },
            { name: 'geo_enabled', type: 'STRING', mode: 'NULLABLE' },
            { name: 'lang', type: 'STRING', mode: 'NULLABLE' },
            { name: 'contributors_enabled', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'is_translator', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'profile_background_color', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_background_image_url', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_background_image_url_https', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_background_tile', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'profile_link_color', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_sidebar_border_color', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_sidebar_fill_color', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_text_color', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_use_background_image', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'profile_image_url', type: 'STRING', mode: 'NULLABLE' },
            { name: 'profile_image_url_https', type: 'STRING', mode: 'NULLABLE' },
            { name: 'default_profile', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'default_profile_image', type: 'BOOL', mode: 'NULLABLE' },
            { name: 'following', type: 'STRING', mode: 'NULLABLE' },
            { name: 'follow_request_sent', type: 'STRING', mode: 'NULLABLE' },
            { name: 'notifications', type: 'STRING', mode: 'NULLABLE' },
            {
              name: 'derived', type: 'RECORD', mode: 'REPEATED',
              "fields": [
                {
                  name: 'locations', type: 'RECORD', mode: 'REPEATED',
                  "fields": [
                    { name: 'country', type: 'STRING', mode: 'NULLABLE' },
                    { name: 'country_code', type: 'STRING', mode: 'NULLABLE' },
                    { name: 'locality', type: 'STRING', mode: 'NULLABLE' },
                    { name: 'region', type: 'STRING', mode: 'NULLABLE' },
                    { name: 'full_name', type: 'STRING', mode: 'NULLABLE' },
                    {
                      name: 'sub_region', type: 'STRING', mode: 'NULLABLE',
                      "fields": [
                        {
                          name: 'geo', type: 'RECORD', mode: 'REPEATED',
                          "fields": [
                            {
                              name: 'coordinates', type: 'RECORD', mode: 'REPEATED',
                              "fields": [
                                { name: 'LONG', type: 'STRING', mode: 'NULLABLE' },
                                { name: 'LAT', type: 'STRING', mode: 'NULLABLE' }
                              ]
                            },
                            { name: 'point', type: 'STRING', mode: 'NULLABLE' }
                          ]
                        },
                      ]
                    },
                  ]
                },
              ]
            },
          ]
        },
        {
          name: 'matching_rules', type: 'RECORD', mode: 'REPEATED',
          "fields": [
            { name: 'tag', type: 'STRING', mode: 'NULLABLE' },
            { name: 'id', type: 'INT64', mode: 'REQUIRED' },
            { name: 'id_str', type: 'STRING', mode: 'REQUIRED' },
          ]
        },
      ],
    },
  };

  // Load data from a Google Cloud Storage file into the table
  const [job] = await bigquery
    .dataset(datasetId)
    .table(tableId)
    .load(storage.bucket(bucketName).file(filename), metadata);
  // load() waits for the job to finish
  console.log(`Job ${job.id} completed.`);

  // Check the job's status for errors
  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}

module.exports = { loadJSONFromGCS };
