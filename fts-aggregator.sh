#!/bin/bash

echo "started at:"
date
startDate=$(date -u '+%Y-%m-%d' -d "-48hour")
echo "processing date" $startDate
python3.8 fts-aggregator.py $startDate
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running fts-aggregator. Exiting."
    exit $rc
fi