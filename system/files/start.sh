#!/bin/bash
/etc/init.d/atd start     # to detect file changes
/etc/init.d/xinetd start  # for rsync test
echo $WZDAT_CFG
cd /wzdat
pip install -e .
# Re-install of requirements to apply changes after base image build.
pip install -r requirements.txt
python -m wzdat.rundb create
python -m wzdat.jobs cache-all
/usr/bin/supervisord
