#!/bin/bash
date
# service sendmail start
python3.8 alerts.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem sending PacketLoss alerts. Exiting."
    exit $rc
fi