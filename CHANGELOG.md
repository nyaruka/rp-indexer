v9.2.0 (2024-07-17)
-------------------------
 * Test against postgresql 15
 * Update dependencies
 * Test against Elasticsearch 8

v9.1.9 (2024-06-10)
-------------------------
 * Record stats inside indexing batch loop
 * Split up created vs updated in progress logging
 * Add track_total_hits to GetESLastModified

v9.1.8 (2024-06-05)
-------------------------
 * Update github actions versions
 * Add healthcheck for elastic service in CI tests
 * Update goreleaser config to v2

v9.1.7 (2024-06-05)
-------------------------
 * Remove multi-search-db CI testing because it's unreliable

v9.1.6 (2024-06-04)
-------------------------
 * Remove opensearch from CI

v9.1.5 (2024-06-04)
-------------------------
 * Update slog-multi

v9.1.4 (2024-06-03)
-------------------------
 * Update deps
 * Use std lib errors

v9.1.3 (2024-05-16)
-------------------------
 * Reduce batch size to 100,000
 * Test with OpenSearch 2.13 as well

v9.1.2 (2024-05-14)
-------------------------
 * Remove deprecated _doc from mapping to support ES8 / OS2
 * Test against Elastic 7.x and 8.x and OpenSearch 1.x
 * Rewrite tests to make raw HTTP calls to elastic and not use the old library

v9.1.1 (2024-04-25)
-------------------------
 * Add reporting of lag as a metric

v9.1.0 (2024-04-15)
-------------------------
 * Update deps and bump main package to v9
 * Finish switching to use slog for logging

v9.0.0 (2024-01-05)
-------------------------
 * Update dependencies

v8.3.1 (2023-11-02)
-------------------------
 * Update docker dev image to go 1.21
 * Start replacing logrus with slog
 * Bump golang.org/x/net from 0.14.0 to 0.17.0
 * Replace deprecated ioutil calls
 * Pass context to fetching of contacts

v8.3.0 (2023-08-10)
-------------------------
 * Update to go 1.20

v8.2.0 (2023-07-31)
-------------------------
 * Fix docker file

v8.0.3 (2023-05-22)
-------------------------
 * Change some debug logging to trace level

v8.0.2 (2023-05-22)
-------------------------
 * Stop logging all ES requests in debug mode (only log errors)

v8.0.1 (2023-05-19)
-------------------------
 * Add major version to import path to allow use as library
 * Use services in github actions

v8.0.0 (2023-01-09)
-------------------------
 * Test against postgres 14
 * Test against latest elastic search 7.17.x

v7.5.0
----------
 * Change index setting to contacts_index
 * Use go 1.19
 * Allow configurable number of shards/replicas

v7.4.0
----------
 * Update README
 * Tweak startup logging

v7.3.10
----------
 * Log app version on startup

v7.3.9
----------
 * Use analytics package from gocommon instead of librato directly
 * Add arm64 as a build target

v7.3.8
----------
 * Update dependencies and go version to 1.18
 * Don't panic on connection failure to ES

v7.3.7
----------
 * Better logging within batches during rebuilds
 * Test with latest ES 7.17

v7.3.6
----------
 * Ignore malformed field value numbers
 * Drop the flow and groups fields which have been replaced by flow_id and group_ids

v7.3.5
----------
 * Log batch progress during rebuilds

v7.3.4
----------
 * Add group_ids field to replace groups

v7.3.3
----------
 * Include flow id history as flow_history_ids and current flow id as flow_id 

v7.3.1
----------
 * If indexing fails, log status code from elasticsearch
 * Poll interval is configurable

v7.3.0
----------
 * Add stats reporting cron task and optional librato config
 * Refactor to support different indexer types
 * Update golang.org/x/sys

v7.2.0
----------
 * Tweak README

v7.1.0
----------
 * Index contact.current_flow_id as flow uuid
 * CI with go 1.17

v7.0.0
----------
 * Test on PG12 and 13

v6.5.0
----------
 * Include contact.ticket_count as tickets in index
 * Update to go 1.16
 * Use embedded file for index settings
 * Remove no longer used is_blocked and and is_stopped fields

v6.4.0
----------
 * 6.4.0 candidate

v6.3.0
----------
 * Fix creating of location keyword fields when values have punctuation

v6.2.0
----------
 * add rp-indexer to .gitignore
 * 6.2.0 RC

v6.1.0
----------
 * Change ElasticSearch version to v7 (backwards incompatible change)

v6.0.0
----------
 * Update README

v5.7.2
----------
 * add status field to index for querying

v5.7.1
----------
 * Use contact status instead of is_stopped / is_blocked
 * Retry HTTP calls to ES

v5.7.0
----------
 * Index last_seen_on

v5.6.0
----------
 * 5.6.0 Release Candidate

v5.4.0 
----------
 * touch README for 5.4 release

v5.2.0
----------
 * Sync release with RapidPro 5.2

v2.0.0
----------
 * Ignore value of is_test on contacts

v1.0.27
----------
 * update ES shards to match current ES best-practice guidance

v1.0.26
----------
 * move to go module, dont ignore any keywords

v1.0.25
----------
 * Changes to support both PG 10 and 9.6

v1.0.24
----------
 * increase batch size to 500k

v1.0.23
----------
 * Add created_on to the mapping

v1.0.22
----------
 * Update location index spec so we can sort in location fields

v1.0.21
----------
 * make sure to close response body so we don't run out of handles

v1.0.20
----------
 * add cleanup option to remove old indexes that are no longer used

v1.0.19
----------
 * better indexing rate calculation

v1.0.18
----------
 * fix indexer getting stalled if there are more than 500 contacts with same modified_on

v1.0.17
----------
 * change to number instead of decimal field
 * add example not exists query

v1.0.16
----------
 * look a bit behind for updated contacts so we don't run into races

v1.0.15
----------
 * plug in sentry, log errors on unexpected indexing responses

v1.0.14
----------
 * Deal with long name searches

v1.0.13
----------
 * specify the 'standard' search_analyzer for name so it doesn't need to be specified at query time

v1.0.12
----------
 * add modified_on_mu for sorting / index creation
 * add prefix name for index building

v1.0.11
----------
 * refactor main loop in indexer

v1.0.10
----------
 * more logging, deal with missing physical indexes

v1.0.9
----------
 * add logging of physical indexes that are looked up

v1.0.8
----------
 * more complete logging of request parameters

v1.0.7
----------
 * add debug log level, more logging of errors

v1.0.6
----------
 * use trigram tokenizer instead of filter to allow for phrase queries on urn paths
 * store both a keyword and tokenized version of locations (without any path)

v1.0.5
----------
 * Fix Travis goreleaser releases

v1.0.4
----------
 * Put rp-indexer binary in root dir

v1.0.3
----------
 * Fix goreleaser config

v1.0.2
----------
 * index groups by uuid for each contact
 * add case insensitive location query in unit test
 * add test for contact with multiple tel urns

v1.0.1
----------
 * Add changelog, move to fancy revving
