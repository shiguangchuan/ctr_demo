#!/usr/bin/python

import sys

if len(sys.argv) != 4:
    sys.stderr.write('invalid args, usage: %s <time_col> <comp_op> <thres_value>\n'%sys.argv[0])
    sys.stderr.write('\ttime_col start with 0\n')
    sys.stderr.write('\tcomp_op support eq/ne/gt/lt/ge/le')
    exit(-1)

time_col = int(sys.argv[1])
comp_op = sys.argv[2].lower()
thres_value = sys.argv[3]

for line in sys.stdin:
    line = line.strip()
    tokens = line.split('\t')
    cur_time = int(tokens[time_col])
    if (comp_op == 'eq' and cur_time == int(thres_value)):
        print line
    elif (comp_op == 'ne' and cur_time != int(thres_value)):
        print line
    elif (comp_op == 'gt' and cur_time > int(thres_value)):
        print line
    elif (comp_op == 'lt' and cur_time < int(thres_value)):
        print line
    elif (comp_op == 'ge' and cur_time >= int(thres_value)):
        print line
    elif (comp_op == 'le' and cur_time <= int(thres_value)):
        print line
    elif (comp_op == 'between'):
        token = thres_value.split(',')
        min_thres = int(token[0])
        max_thres = int(token[1])
        if (cur_time >= min_thres and cur_time < max_thres):
            print line

