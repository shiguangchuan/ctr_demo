#!/usr/bin/python

import sys

if len(sys.argv) != 2:
    sys.stderr.write('invalid args, usage: %s <label_col>\n'%sys.argv[0])
    sys.stderr.write('\tlabel_col start with 0\n')
    exit(-1)

label_col = int(sys.argv[1])

for line in sys.stdin:
    line.strip()
    tokens = line.split('\t')
    sep = ''
    for i in range(0, len(tokens)):
        if (i == label_col):
            if tokens[i] == '' or int(tokens[i]) == 0:
                new_label = '-1'
            else:
                new_label = '1'
            sys.stdout.write('%s%s' % (sep, new_label))
        else:
            sys.stdout.write('%s%s' % (sep, tokens[i]))
        sep = '\t'
    sys.stdout.write('\n')


