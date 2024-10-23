#!/bin/bash

# Use the following config for nginx
# location /k12-inspector-sql-dashboard {
#         proxy_set_header   Host                 $host;
#         proxy_pass http://localhost:38725/k12-inspector-sql-dashboard;
# }

datasette 2024-10-21.sqlite \
    --host 0.0.0.0 \
    --port 38725 \
    --setting base_url "/k12-inspector-sql-dashboard/" \
    --setting sql_time_limit_ms 60000 \
    --setting max_returned_rows 66666 \
    --setting max_csv_mb 0 \
    --setting num_sql_threads 20

