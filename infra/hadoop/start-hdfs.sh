#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  namenode)
    if [ ! -f /data/name/current/VERSION ]; then
      # Never format a nonempty or partially initialized volume automatically.
      if [ -n "$(find /data/name -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo "Refusing to format nonempty NameNode storage" >&2
        exit 1
      fi
      hdfs namenode -format -nonInteractive
    fi
    exec hdfs namenode
    ;;
  datanode) exec hdfs datanode ;;
  init)
    hdfs dfsadmin -safemode wait
    hdfs dfs -mkdir -p /trendcast/raw /trendcast/processed /trendcast/analytics /trendcast/checkpoints/streaming
    hdfs dfs -chmod 775 /trendcast /trendcast/raw /trendcast/processed /trendcast/analytics /trendcast/checkpoints /trendcast/checkpoints/streaming
    ;;
  *) exec "$@" ;;
esac
