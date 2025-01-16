#!/bin/bash

set -e

cd /home/danny/dev/k12-inspector/school_crawler

while [ 1 ];
do
    date
    sqlite3 -header -csv 2024-10-21.sqlite < get_third_party_urls.sql > third_party_urls.csv.pending
    mv third_party_urls.csv.pending third_party_urls.csv
    date
    echo Sleeping
    sleep 600
done



