#!/usr/bin/python

import sys
import datetime

if len(sys.argv) != 2:
    sys.stderr.write('invalid args, usage: %s <time_col>\n'%sys.argv[0])
    sys.stderr.write('\ttime_col start with 0\n')
    exit(-1)

time_col = int(sys.argv[1])

for line in sys.stdin:
    line = line.strip()
    tokens = line.split('\t')
    sep = ''
    for i in range(0, len(tokens)):
        if (i == time_col):
            hour = int(datetime.datetime.strptime('20%s'%tokens[i],'%Y%m%d%H').strftime('%s'))/3600
            sys.stdout.write('%s%d' % (sep, hour))
        else:
            sys.stdout.write('%s%s' % (sep, tokens[i]))
        sep = '\t'
    sys.stdout.write('\n')


