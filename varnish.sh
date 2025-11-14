#!/bin/bash
date
python3 varnish.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem running Varnish alarm. Exiting."
    exit $rc
fi