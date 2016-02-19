#!/bin/bash

rm -rf logs peerlist.properties
rm -rf 10.0.0*
mkdir logs logs/keyset logs/groups logs/minha dataout datain
rm -rf datain/*.txt
cp dataout/*.txt datain/

mvn package

mycommand="java -Djava.library.path=$1 -Xmx10G -jar target/DataFlasks-0.0.1-SNAPSHOT-jar-with-dependencies.jar "
echo $mycommand
$mycommand


#-Dorg.slf4j.simpleLogger.defaultLogLevel=warn