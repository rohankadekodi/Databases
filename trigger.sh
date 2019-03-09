#!/bin/bash

if [ "$#" != 1 ]; then
    echo "Usage: ./trigger.sh <Run_ID>"
    exit
fi

set -x

databaseDir=/home/rohan/projects/Databases
runId=$1

# Compile java codes
javac com/postgres/db/*.java

# Create result dir
resultDir=$databaseDir/Results/$runId
mkdir -p $resultDir

for index in none columnA columnB both
do
    java -cp .:postgresql-42.2.5.jar com.postgres.db.JdbcDBCreateTable $index
    for layout in seq rand
    do
	for query in 1 2 3
	do
	    echo "$layout and query number $query:" >> $resultDir/$index
	    java -cp .:postgresql-42.2.5.jar com.postgres.db.JdbcDBClient $layout $query >> $resultDir/$index	    	    
	done
    done
done
