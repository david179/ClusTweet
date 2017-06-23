#!/bin/bash
java -cp ./build/libs/postgresql-42.1.1.jar:./build/libs/db-project-1.0-SNAPSHOT-all.jar it.unipd.dei.db.ClusteringTweets >log.txt
