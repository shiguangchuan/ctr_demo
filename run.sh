#!/bin/bash


export PYTHONPATH=/app/cli

DEMO_ROOT=`pwd`
base_conf_dir=./conf

rm -rf ./working_conf
cp -rf ./conf ./working_conf

DEMO_ROOT=${DEMO_ROOT//\//\\\/}
DEMO_ROOT=${DEMO_ROOT//\-/\\\-}
DEMO_ROOT=${DEMO_ROOT//\./\\\.}

for file in `ls ./working_conf/streaming*`
do
    sed -e "s/#DEMO_ROOT#/${DEMO_ROOT}/g" -i ${file}
done

./ctr_demo.py

