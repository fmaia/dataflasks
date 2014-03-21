#!/bin/bash

rm -rf logs peerlist.properties
rm -rf 10.0.0*
mkdir logs logs/keyset

mycommand="java -Dorg.slf4j.simpleLogger.defaultLogLevel=error -Xmx10G -XX:ReservedCodeCacheSize=512M -XX:PermSize=5120M -XX:MaxPermSize=5120M -jar build/stratus.jar "
echo $mycommand
$mycommand



#-Dorg.slf4j.simpleLogger.defaultLogLevel=error -Djava.library.path=lib/csrc/ -DnetworkCalibration=networkCalibration.cfg