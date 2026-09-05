#!/bin/bash
docker run -d \
  --name=node-exporter \
  --restart=always \
  --net=host \
  --pid=host \
  -v "/:/host:ro,rslave" \
  prom/node-exporter \
  --path.rootfs=/host