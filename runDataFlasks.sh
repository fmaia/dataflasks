#!/bin/bash

rm -rf logs peerlist.properties
rm -rf 10.0.0*
mkdir logs logs/keyset logs/groups dataout datain
rm -rf datain/*.txt
cp dataout/*.txt datain/

mycommand="java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -Dlog4j.configurationFile=file:config/log4j2.xml -Xmx10G -jar build/stratus.jar "
echo $mycommand
$mycommand


# -Dlog4j.configurationFile=file:config/log4j2.xml
#-Dorg.slf4j.simpleLogger.defaultLogLevel=error -Djava.library.path=lib/csrc/ -DnetworkCalibration=networkCalibration.cfg
#-XX:ReservedCodeCacheSize=512M -XX:PermSize=5120M -XX:MaxPermSize=5120M