#!/usr/bin/env sh
set -e

mvn package
JAVA_HOME=/usr/lib/jvm/default-java/
rm -rf text-frequency/output
rm -rf inverse-document-frequency/output
rm -rf top-words/output
../hadoop-2.9.0/bin/hadoop jar target/hadoop-tfidf-1.jar local
