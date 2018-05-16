#!/bin/bash

echo "Running spark program:" $1
exec 5>&1
APPLICATION_ID=$(spark-submit --deploy-mode cluster --master yarn --num-executors 9 --driver-memory 2g --executor-memory 2g --executor-cores 4 $1 2>&1 | tee >(cat - >&5) | awk '!found && /INFO.*Yarn.*Submitted application/ {tmp=gensub(/^.*Submitted application (.*)$/,"\\1","g");print tmp; found=1}')
echo "================================================="
echo "Applicaton id: $APPLICATION_ID"
echo "================================================="
echo "=                  stderr                       ="
echo "================================================="
yarn logs -applicationId "$APPLICATION_ID" | awk -F: '/^LogType/ {if($2=="stderr") {output=1} else {output=0}} output==1 {print}'
echo "================================================="
echo "=                  result                       ="
echo "================================================="
yarn logs -applicationId "$APPLICATION_ID" | awk -F: '/^LogType/ {if($2=="stdout") {output=1} else {output=0}} output==1 {print}' | grep -v "WARN\|INFO"

