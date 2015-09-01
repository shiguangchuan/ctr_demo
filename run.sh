#!/bin/bash


export PYTHONPATH=/app/cli

DEMO_ROOT=`pwd`
base_conf_dir=./conf

rm -rf ./working_conf
mkdir -p ./working_conf

DEMO_ROOT=${DEMO_ROOT//\//\\\/}
DEMO_ROOT=${DEMO_ROOT//\-/\\\-}
DEMO_ROOT=${DEMO_ROOT//\./\\\.}

for file in `ls ${base_conf_dir}/streaming*`
do
    cat ${file} | sed -e "s/#DEMO_ROOT#/${DEMO_ROOT}/g" > ./working_conf/`basename ${file}`
done

./ctr_demo.py

