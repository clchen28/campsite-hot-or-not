#!/bin/bash
if [ $# -ne 1 ]
then
        echo "dl_noaa.sh <year> to download all data for that year"
        exit 1
fi

YEAR=$1
ncftpls ftp://ftp.ncdc.noaa.gov/pub/data/noaa/${YEAR} > ${YEAR}.txt
sed -i -e 's/^/https:\/\/www1.ncdc.noaa.gov\/pub\/data\/noaa\//' ${YEAR}.txt
mkdir ${YEAR}
aria2c -i ${YEAR}.txt -d ${YEAR}