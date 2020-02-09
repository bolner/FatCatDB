#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

rm -fr var/data

EXEC="bin/Release/netcoreapp3.1/publish/IntegrationTests"

${EXEC} test/add
${EXEC} test/query1 >IntegrationTests/Results/query1.txt
${EXEC} test/query2 >IntegrationTests/Results/query2.txt
${EXEC} test/query3 >IntegrationTests/Results/query3.txt
