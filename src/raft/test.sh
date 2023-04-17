#!/bin/bash

for (( i=0; i<15; i=i+1 )); do
  echo out$i
  go test -run TestBackup2B -race > out$i
done

