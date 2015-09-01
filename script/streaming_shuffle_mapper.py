#!/usr/bin/python

import os
import random
import sys

RND_SEED = 57885161
MAX = 2147483647

if 'mapreduce_task_partition' in os.environ:
    map_id = int(os.environ['mapreduce_task_partition'])
else:
    map_id = 0

random.seed(RND_SEED ^ map_id)

for line in sys.stdin:
    line = line.strip()
    print '%d\t%s' % (random.randint(0, MAX), line)


