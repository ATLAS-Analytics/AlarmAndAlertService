#!/bin/bash

echo "started at:"
date
python3 -m ensurepip --upgrade
python3 -m pip install -r requirements.txt
startDate=$(date -u '+%Y-%m-%d' -d "-48hour")
echo "processing date" $startDate
python3 fts-aggregator.py $startDate > fts-aggregator.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running fts-aggregator. Exiting."
    exit $rc
fi