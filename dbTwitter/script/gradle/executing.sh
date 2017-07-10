#!/bin/bash
java -cp ../../libs/postgresql-42.1.1.jar:../../build/libs/db-project-1.0-SNAPSHOT-all.jar:../../libs/stanford-postagger.jar:../../libs/stanford-postagger-3.8.0.jar:../../libs/stanford-postagger-3.8.0-javadoc.jar:../../libs/stanford-postagger-3.8.0-sources.jar it.unipd.dei.db.ClusTweets >log.txt
