#!/bin/bash
date
python3.8 packet-loss.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem checking packet loss. Exiting."
    exit $rc
fi