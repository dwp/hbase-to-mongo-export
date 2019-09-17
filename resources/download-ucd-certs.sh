#!/bin/bash

rm -rf UCD-*.crt
wget https://github.ucds.io/patrickdowney/docker-mermaidjs/blob/master/UCD-CA.crt --no-check-certificate --output-document UCD-CA.crt
wget https://github.ucds.io/patrickdowney/docker-mermaidjs/blob/master/UCD-CLIENTCA.crt --no-check-certificate --output-document UCD-CLIENTCA.crt
wget https://github.ucds.io/patrickdowney/docker-mermaidjs/blob/master/UCD-RSA-CA.crt --no-check-certificate --output-document UCD-RSA-CA.crt