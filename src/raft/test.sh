#!/bin/bash

for (( i=0; i<100; i=i+1 )); do
  echo out$i
  go test -run TestPersist22C -race > out$i
done

