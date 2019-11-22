# Elasticsearch GitHub Action

This [GitHub Action](https://github.com/features/actions) sets up a Elasticsearch server.

# Usage

See [action.yml](action.yml)

Basic:
```yaml
steps:
- uses: nyaruka/elasticsearch@v1
  with:
    elastic version: '6.8.5'  # See https://hub.docker.com/_/elasticsearch for available versions
```

# License

The scripts and documentation in this project are released under the [MIT License](LICENSE)