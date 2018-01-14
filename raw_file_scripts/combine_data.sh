#!/bin/bash

if [ $# -ne 1 ];
then
	echo "combine_data.sh <year>"
	exit 1
fi

YEAR=$1

cat ${YEAR}/0* > ${YEAR}-0.txt
cat ${YEAR}/1* > ${YEAR}-1.txt
cat ${YEAR}/2* > ${YEAR}-2.txt
cat ${YEAR}/3* > ${YEAR}-3.txt
cat ${YEAR}/4* > ${YEAR}-4.txt
cat ${YEAR}/5* > ${YEAR}-5.txt
cat ${YEAR}/6* > ${YEAR}-6.txt
cat ${YEAR}/7* > ${YEAR}-7.txt
cat ${YEAR}/8* > ${YEAR}-8.txt
cat ${YEAR}/9* > ${YEAR}-9.txt