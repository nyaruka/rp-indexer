#!/bin/sh

docker_run="docker run -d -p 9200:9200 -p 9300:9300 -e 'discovery.type=single-node' elasticsearch:$INPUT_ELASTIC_VERSION"

echo "RUNNING: $docker_run"
sh -c "$docker_run"