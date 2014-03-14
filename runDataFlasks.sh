#!/bin/bash

rm -rf logs
mkdir logs logs/keyset writesnapshot

mycommand="java -Xmx10G -XX:ReservedCodeCacheSize=512M -XX:PermSize=5120M -XX:MaxPermSize=5120M -jar build/stratus.jar "
echo $mycommand
$mycommand



#-Dorg.slf4j.simpleLogger.defaultLogLevel=error -Djava.library.path=lib/csrc/ -DnetworkCalibration=networkCalibration.cfg