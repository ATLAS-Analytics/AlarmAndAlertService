#!/bin/bash
date
service sendmail start
python3 sending-mails-PacketLoss.py
rc=$?; if [[ $rc != 0 ]]; then 
    echo "problem sending PacketLoss alerts. Exiting."
    exit $rc
fi