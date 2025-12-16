#!/bin/bash
date
python3 varnish.py > /var/log/varnish.log 2>&1
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running Varnish alarm. Exiting."
    exit $rc
fi