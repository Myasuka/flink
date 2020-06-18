#!/bin/bash
for i in {1..10}
do
  mvn test >> build-$i
done
