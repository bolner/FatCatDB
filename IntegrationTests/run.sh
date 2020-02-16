#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

rm -fr var/data

EXEC="bin/Release/netcoreapp3.1/publish/IntegrationTests"

${EXEC} test/add
${EXEC} test/query1 >Results/query1.txt
${EXEC} test/query2 >Results/query2.txt
${EXEC} test/query3 >Results/query3.txt
${EXEC} test/query4 >Results/query4.txt

${EXEC} test/update
${EXEC} test/query4 >Results/query4_after_update.txt
${EXEC} test/queryPlan1 >Results/queryPlan1.txt
${EXEC} test/queryPlan2 >Results/queryPlan2.txt
