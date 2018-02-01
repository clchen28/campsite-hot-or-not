#!/bin/bash

# Package all dependencies into zip file for submission with spark-submit

CURRENT=`pwd`
TMPZIP="$CURRENT/$RANDOM.zip"
( cd "venv/lib/python3.5/site-packages" && zip -q -r $TMPZIP . )
mv $TMPZIP $CURRENT/packages.zip