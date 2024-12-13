# 🗃️ Indexer

[![Build Status](https://github.com/nyaruka/rp-indexer/workflows/CI/badge.svg)](https://github.com/nyaruka/rp-indexer/actions?query=workflow%3ACI) 
[![codecov](https://codecov.io/gh/nyaruka/rp-indexer/branch/main/graph/badge.svg)](https://codecov.io/gh/nyaruka/rp-indexer) 
[![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/rp-indexer)](https://goreportcard.com/report/github.com/nyaruka/rp-indexer)

Service for indexing RapidPro/TextIt contacts into Elasticsearch.

## Deploying

As it is a Go application, it compiles to a binary and that binary along with the config file is all
you need to run it on your server. You can find bundles for each platform in the
[releases directory](https://github.com/nyaruka/rp-indexer/releases). You should only run a single
instance for a deployment.

It can run in two modes:

1) the default mode, which simply queries the ElasticSearch database, finds the most recently
modified contact, then on a schedule queries the `contacts_contact` table in the 
database for contacts to add or delete. You should run this as a long running service which
constantly keeps ElasticSearch in sync with your contacts.

2) a rebuild mode, started with `--rebuild`. This builds a brand new index from nothing, querying
all contacts on RapidPro. Once complete, this switches out the alias for the contact index
with the newly build index. This can be run on a cron (in parallel with the mode above) to rebuild
your index occasionally to get rid of bloat.

## Configuration

The service uses a tiered configuration system, each option takes precendence over the ones above it:

 1. The configuration file
 2. Environment variables starting with `INDEXER_` 
 3. Command line parameters

We recommend running it with no changes to the configuration and no parameters, using only
environment variables to configure it. You can use `% rp-indexer --help` to see a list of the
environment variables and parameters and for more details on each option.

### RapidPro

For use with RapidPro, you will want to configure these settings:

 * `INDEXER_DB`: a URL connection string for your RapidPro database or read replica
 * `INDEXER_ELASTIC_URL`: the URL for your ElasticSearch endpoint
 
Recommended settings for error reporting:

 * `INDEXER_SENTRY_DSN`: DSN to use when logging errors to Sentry

## Development

Once you've checked out the code, you can build the service with:

```
go build github.com/nyaruka/rp-indexer/cmd/rp-indexer
```

This will create a new executable in $GOPATH/bin called `rp-indexer`.

To run the tests you need to create the test database:

```
$ createdb elastic_test
```

To run all of the tests:

```
go test ./... -p=1
```
