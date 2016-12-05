#!/bin/bash

common="01 02 03 04 05 06 07 08 09"

for i in $common 10
do
    echo Showing SERVER $i ...
    ssh teng9@fa16-cs425-g06-$i.cs.illinois.edu 'jps'
done

