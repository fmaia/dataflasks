#!/bin/bash

rm -rf logs peerlist.properties
rm -rf 10.0.0*
mkdir logs logs/keyset logs/groups

mycommand="java -Dorg.slf4j.simpleLogger.defaultLogLevel=warn -Xmx10G -jar build/stratus.jar "
echo $mycommand
$mycommand



#-Dorg.slf4j.simpleLogger.defaultLogLevel=error -Djava.library.path=lib/csrc/ -DnetworkCalibration=networkCalibration.cfg
#-XX:ReservedCodeCacheSize=512M -XX:PermSize=5120M -XX:MaxPermSize=5120M