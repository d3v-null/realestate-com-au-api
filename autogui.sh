#!/bin/bash
. .venv/bin/activate
export DISPLAY=:0.0
pgrep firefox || firefox &
./autogui.py --sql_url "${SQL_URL}" --browser 'firefox' \
    --fresh --urls urls.txt \
    --script "(new WebSocket('ws://localhost.local:42069')).onopen=(e)=>{e.target.send(JSON.stringify(_.pick(REA,[\
    'avmData','bathrooms','bedrooms', 'canonicalUrl','carSpaces','channel','floorArea','fullSuburb','landArea',\
    'lat','lon','longStreetAddress','offMarket','propertyId','propertyListing','propertyMarketTrends',\
    'propertyTimeline','state','suburb','yearBuilt','propertyType'])))}"
ret=$?
exit $ret

./autogui.py --sql_url "${SQL_URL}" --browser 'firefox' \
    --script "(new WebSocket('ws://localhost.local:42069')).onopen=(e)=>{e.target.send(JSON.stringify(_.pick(REA,[\
    'avmData','bathrooms','bedrooms', 'canonicalUrl','carSpaces','channel','floorArea','fullSuburb','landArea',\
    'lat','lon','longStreetAddress','offMarket','propertyId','propertyListing','propertyMarketTrends',\
    'propertyTimeline','state','suburb','yearBuilt','propertyType'])))}"
ret=$?
sleep 5
killall firefox
exit $ret