#!/bin/bash -e

# Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
#
# See LICENSE file for further information.

for x in wc echo split curl wait ; do
    if [ -z `which $x` ]; then
      echo "$x must be on the path"
      exit 1
    fi
done

CHUNK_PREFIX=.symbols.txt.

cleanup() {
  rm -f "$CHUNK_PREFIX*"
}
trap cleanup ERR EXIT

TOTAL=$(wc -l < symbols.txt) # get count of symbols
TOTAL=$(echo -n $TOTAL | xargs) # strip spaces
SIZE=25
COUNT=0
SYMBOL_FILTER_REGEX=^.*$ # everything by default; change this to read only symbols that match the regex, like ^AA.*$, for all stocks that begin with "AA"

split -l $SIZE symbols.txt $CHUNK_PREFIX # parallelize downloads for speed, else it takes a loooooooooong time

for CHUNK in $(find . -type f -name "$CHUNK_PREFIX*"); do
    PIDS=
    for SYMBOL in $(cat "$CHUNK"); do
        if [[ $SYMBOL =~ $SYMBOL_FILTER_REGEX ]]; then
          echo Fetching data for symbol $SYMBOL
          FILE="stocks/$SYMBOL.csv"
          COUNT=$(expr $COUNT + 1)
          if [ ! -s "$FILE" ]; then
              curl -sSL --create-dirs -o "$FILE" "http://ichart.yahoo.com/table.csv?s=$SYMBOL&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv" &
              PIDS="$PIDS $!" # store process id of last curl job
          fi
        fi
    done
    # ensure no more than SIZE curl jobs are running at once by waiting for them all to finish
    if [ -n "$PIDS" ]; then
        for PID in $PIDS; do
            wait $PID
        done
    fi
    # now done with chunk
    rm "$CHUNK"
    echo -n -e "$COUNT of $TOTAL stocks downloaded\r"
done

PIDS=
for FACTOR in ^GSPC ^IXIC; do # get S&P500 and NASDAQ Composite Index
    FILE="factors/$FACTOR.csv"
    if [ ! -s "$FILE" ]; then
        curl -sSL --create-dirs -o "$FILE" "http://ichart.yahoo.com/table.csv?s=$FACTOR&a=0&b=1&c=2000&d=0&e=31&f=2013&g=d&ignore=.csv" &
        PIDS="$PIDS $!"
    fi
done
for PID in $PIDS; do
    wait $PID
done

echo 'Removing any stocks not found by Yahoo! Finance (HTTP 404 response code)'
NOT_FOUNDS=
for NOT_FOUND in $(find stocks -name '*.csv' -exec grep -l '404 Not Found' {} \;); do
    NOT_FOUNDS="$NOT_FOUNDS $NOT_FOUND"
    rm $NOT_FOUND
done

NOT_FOUNDS=$(echo -n $NOT_FOUNDS | sed 's/stocks\///g' | sed 's/\.csv//g')
COUNT=$(echo -n $NOT_FOUNDS | wc -w | xargs)
echo "The following $COUNT stocks were removed because Yahoo! Finance returned HTTP response code 404:"
echo $NOT_FOUNDS
