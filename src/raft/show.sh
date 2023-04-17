#!/bin/bash

for (( i=0; i<15; i=i+1 )); do
  echo out$i:
  tail -n 1 out$i
  echo ""
done
