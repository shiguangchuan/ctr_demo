#!/bin/bash

set -x

RND_SEED=57885161
map_id="${mapreduce_task_partition}"
if [ "${map_id}" = "" ]; then
    map_id="0"
fi

awk 'BEGIN {
    srand(xor('${RAND_SEED}', '${map_id}'));
    MAX = 2147483647;
} {
    print int(rand() * MAX)"\t"$0;
}'

