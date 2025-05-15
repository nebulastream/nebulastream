#!/usr/bin/bash
set -e

FILE=$1
FILENAME=$(basename $FILE)
CHECKSUM=$(md5sum $FILE | awk '{print $1}')

cp $FILE /tmp/MD5_$CHECKSUM
gh release upload --repo nebulastream/nes-datasets v1 /tmp/MD5_$CHECKSUM

echo "$CHECKSUM" > $FILENAME.md5
git add $FILENAME.md5
rm $FILE
