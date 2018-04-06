# RapidPro Indexer [![Build Status](https://travis-ci.org/nyaruka/rp-indexer.svg?branch=master)](https://travis-ci.org/nyaruka/rp-indexer) [![codecov](https://codecov.io/gh/nyaruka/rp-indexer/branch/master/graph/badge.svg)](https://codecov.io/gh/nyaruka/rp-indexer) [![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/rp-indexer)](https://goreportcard.com/report/github.com/nyaruka/rp-indexer)

Simple service for indexing RapidPro contacts into ElasticSearch.

This service can run in two modes:

1) the default mode, which simply queries the ElasticSearch database, finds the most recently
modified contact, then on a schedule queries the `contacts_contact` table on the RapidPro
database for contacts to add or delete. You should run this as a long running service which
constantly keeps ElasticSearch in sync with your RapidPro contacts.

2) a rebuild mode, started with `--rebuild`. This builds a brand new index from nothing, querying
all contacts on RapidPro. Once complete, this switches out the alias for the contact index
with the newly build index. This can be run on a cron (in parallel with the mode above) to rebuild
your index occasionally to get rid of bloat.

## Usage

It is recommended to run the service with two environment variables set:

 * `INDEXER_DB`: a URL connection string for your RapidPro database
 * `INDEXER_ELASTIC_URL`: the URL for your ElasticSearch endpoint

```
Indexes RapidPro contacts to ElasticSearch

Usage of indexer:
  -db string
        the connection string for our database (default "postgres://localhost/rapidpro")
  -debug-conf
        print where config values are coming from
  -elastic-url string
        the url for our elastic search instance (default "http://localhost:9200")
  -help
        print usage information
  -index string
        the alias for our contact index (default "contacts")
  -poll int
        the number of seconds to wait between checking for updated contacts (default 5)
  -rebuild
        whether to rebuild the index, swapping it when complete, then exiting (default false)

Environment variables:
                                  INDEXER_DB - string
                         INDEXER_ELASTIC_URL - string
                               INDEXER_INDEX - string
                                INDEXER_POLL - int
                             INDEXER_REBUILD - bool
```
