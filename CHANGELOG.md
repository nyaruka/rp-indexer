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
