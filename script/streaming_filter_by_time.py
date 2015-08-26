#!/usr/bin/python

import sys

if len(sys.argv) != 2:
    sys.stderr.write('invalid args, usage: %s <time_col> <comp_op> <thres_value>\n'%sys.argv[0])
    sys.stderr.write('\ttime_col start with 0\n')
    sys.stderr.write('\tcomp_op support eq/ne/gt/lt/ge/le')
    exit(-1)

time_col = int(sys.argv[1])
comp_op = sys.argv[2].lower()
thres_value = sys.argv[3]

for line in sys.stdin:
    line.strip()
    tokens = line.split('\t')
    cur_time = tokens[time_col]
    if (comp_op == 'eq' and cur_time == thres_value):
        print line
    else if (comp_op == 'ne' and cur_time != thres_value):
        print line
    else if (comp_op == 'gt' and cur_time > thres_value):
        print line
    else if (comp_op == 'lt' and cur_time < thres_value):
        print line
    else if (comp_op == 'ge' and cur_time >= thres_value):
        print line
    else if (comp_op == 'le' and cur_time <= thres_value):
        print line

