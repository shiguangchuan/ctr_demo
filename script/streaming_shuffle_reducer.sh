#!/bin/bash

awk -F"\t" '{
    sep = "";
    for (i = 2; i <= NF; i++) {
        printf("%s%s", sep, $(i));
        sep = "\t";
    }
    printf("\n");
}'

