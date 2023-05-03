#!/bin/bash
for location in "Bentley, WA 6102" \
        "Burswood, WA 6100" \
        "Carlisle, WA 6101" \
        "Como, WA 6152" \
        "East Perth, WA 6004" \
        "East Victoria Park, WA 6101" \
        "South Perth, WA 6151" \
        "St James, WA 6102" \
        "Subiaco, WA 6008" \
        "Victoria Park, WA 6100"
do
    ./populate_db.py \
        --channel "sold" \
        --sql_url "${SQL_URL}" \
        --locations $location
    ./populate_db.py \
        --channel "buy" \
        --sql_url "${SQL_URL}" \
        --locations $location
done
# "Rivervale, WA 6103"
