#!/bin/bash -e

# Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

cleanup() {
  rm -f syms.*
}
trap cleanup ERR EXIT

TOTAL=$(wc -l < symbols.txt)
SIZE=25
COUNT=0
split -l $SIZE symbols.txt syms.

for CHUNK in $(find . -name 'syms.*'); do
    PIDS=
    for SYMBOL in $(cat "$CHUNK"); do
        FILE="stocks/$SYMBOL.csv"
        COUNT=$(expr $COUNT + 1)
        if [ ! -s "$FILE" ]; then
            curl -sSL --create-dirs -o "$FILE" "http://ichart.yahoo.com/table.csv?s=$SYMBOL&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv" &
            PIDS="$PIDS $!"
        fi
    done
    if [ -n "$PIDS" ]; then
        for PID in $PIDS; do
            wait $PID
        done
    fi
    rm "$CHUNK"
    echo -n -e "$COUNT of $TOTAL stocks downloaded\r"
done

PIDS=
for FACTOR in ^GSPC ^IXIC; do
    FILE="factors/$FACTOR.csv"
    if [ ! -s "$FILE" ]; then
        curl -sSL --create-dirs -o "$FILE" "http://ichart.yahoo.com/table.csv?s=$FACTOR&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv" &
        PIDS="$PIDS $!"
    fi
done
for PID in $PIDS; do
    wait $PID
done

# remove any stocks not found
NOT_FOUNDS=
for NOT_FOUND in $(find stocks -name '*.csv' -exec grep -l '404 Not Found' {} \;); do
    NOT_FOUNDS="$NOT_FOUNDS $NOT_FOUND"
    rm $NOT_FOUND
done
#echo "The following stocks were removed because Yahoo returned 404: $NOT_FOUNDS"
