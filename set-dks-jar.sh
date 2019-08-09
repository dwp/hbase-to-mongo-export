#!/bin/bash
JAR_NAME=$(curl --silent --insecure "https://api.github.com/repos/dwp/data-key-service/releases/latest" | jq -r '.assets[0].name')
java -jar "$JAR_NAME"
$*