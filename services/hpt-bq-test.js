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
    const tableId = "hpt_tweets";

    const metadata = {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        schema: {
            fields: [
                {
                    "name": "created_at",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                },
                {
                    "name": "id_str",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "text",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "source",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "truncated",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE"
                },
                {
                    "name": "in_reply_to_status_id",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "in_reply_to_status_id_str",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "in_reply_to_user_id",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "in_reply_to_user_id_str",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "in_reply_to_screen_name",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "quoted_status_id",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "quoted_status_id_str",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "is_quote_status",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE"
                },
                {
                    "name": "quote_count",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "reply_count",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "retweet_count",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "favorite_count",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                },
                {
                    "name": "favorited",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE"
                },
                {
                    "name": "retweeted",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE"
                },
                {
                    "name": "possibly_sensitive",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE"
                },
                {
                    "name": "filter_level",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "contributors",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "lang",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "timestamp_ms",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "user",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "id",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "id_str",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "screen_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "location",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "url",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "description",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "protected",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "verified",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "followers_count",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "friends_count",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "listed_count",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "favourites_count",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "statuses_count",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "created_at",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "profile_banner_url",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "profile_image_url_https",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "profile_image_url",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "default_profile",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "default_profile_image",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "utc_offset",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "geo_enabled",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "is_translator",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "lang",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "following",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "follow_request_sent",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "has_extended_profile",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "notifications",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "profile_location",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "contributors_enabled",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "updated",
                            "type": "STRING",
                            "mode": "REPEATED"
                        },
                        {
                            "name": "derived",
                            "type": "RECORD",
                            "fields": [
                                {
                                    "name": "locations",
                                    "type": "RECORD",
                                    "mode": "REPEATED",
                                    "fields": [
                                        {
                                            "name": "country",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "country_code",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "locality",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "region",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "sub_region",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "full_name",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "geo",
                                            "type": "RECORD",
                                            "fields": [
                                                {
                                                    "name": "type",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"
                                                },
                                                {
                                                    "name": "coordinates",
                                                    "type": "FLOAT",
                                                    "mode": "REPEATED"
                                                }
                                            ],
                                            "mode": "NULLABLE"
                                        }
                                    ]
                                }
                            ],
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "time_zone",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "translator_type",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "profile_use_background_image",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "profile_background_tile",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        }
                    ],
                    "mode": "NULLABLE"
                },
                {
                    "name": "coordinates",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "coordinates",
                            "type": "FLOAT",
                            "mode": "REPEATED"
                        },
                        {
                            "name": "type",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ],
                    "mode": "NULLABLE"
                },
                {
                    "name": "place",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "attributes",
                            "type": "RECORD",
                            "mode": "NULLABLE",
                            "fields": [
                                {
                                    "name": "test",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                }
                            ]
                        },
                        {
                            "name": "bounding_box",
                            "type": "RECORD",
                            "fields": [
                                {
                                    "name": "coordinates",
                                    "type": "FLOAT",
                                    "mode": "REPEATED"
                                },
                                {
                                    "name": "type",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                }
                            ],
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "id",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "url",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "place_type",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "full_name",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "country_code",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "country",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ],
                    "mode": "NULLABLE"
                },
                {
                    "name": "entities",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "hashtags",
                            "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [
                                {
                                    "name": "text",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "indices",
                                    "type": "INTEGER",
                                    "mode": "REPEATED"
                                }
                            ]
                        },
                        {
                            "name": "urls",
                            "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [
                                {
                                    "name": "display_url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "expanded_url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "indices",
                                    "type": "INTEGER",
                                    "mode": "REPEATED"
                                }
                            ]
                        },
                        {
                            "name": "user_mentions",
                            "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [
                                {
                                    "name": "id",
                                    "type": "INTEGER",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "id_str",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "name",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "screen_name",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "indices",
                                    "type": "INTEGER",
                                    "mode": "REPEATED"
                                }
                            ]
                        },
                        {
                            "name": "symbols",
                            "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [
                                {
                                    "name": "text",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "indices",
                                    "type": "INTEGER",
                                    "mode": "REPEATED"
                                }
                            ]
                        },
                        {
                            "name": "polls",
                            "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [
                                {
                                    "name": "end_datetime",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "duration_minutes",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "options",
                                    "type": "RECORD",
                                    "mode": "REPEATED",
                                    "fields": [
                                        {
                                            "name": "text",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "position",
                                            "type": "INTEGER",
                                            "mode": "NULLABLE"
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "name": "media",
                            "type": "RECORD",
                            "mode": "REPEATED",
                            "fields": [
                                {
                                    "name": "display_url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "expanded_url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "id",
                                    "type": "INTEGER",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "id_str",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "indices",
                                    "type": "INTEGER",
                                    "mode": "REPEATED"
                                },
                                {
                                    "name": "media_url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "media_url_https",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "source_status_id",
                                    "type": "INTEGER",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "source_status_id_str",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "type",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "url",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                }
                            ]
                        }
                    ],
                    "mode": "NULLABLE"
                },
                {
                    "name": "matching_rules",
                    "type": "RECORD",
                    "mode": "REPEATED",
                    "fields": [
                        {
                            "name": "tag",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "id",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "id_str",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ]
                },
                {
                    "name": "geo",
                    "type": "RECORD",
                    "fields": [
                        {
                            "name": "coordinates",
                            "type": "RECORD",
                            "fields": [
                                {
                                    "name": "type",
                                    "type": "STRING",
                                    "mode": "NULLABLE"
                                },
                                {
                                    "name": "coordinates",
                                    "type": "INTEGER",
                                    "mode": "REPEATED"
                                }
                            ],
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "place_id",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ],
                    "mode": "NULLABLE"
                },
                {
                    "name": "display_text_range",
                    "type": "INTEGER",
                    "mode": "REPEATED"
                }
            ],
        },
        location: 'US',
        ignore_unknown_values: true,
        max_bad_records: 5
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
