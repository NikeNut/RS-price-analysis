#!/bin/bash
# run docker container
/usr/local/bin/docker run -e TZ=America/New_York --volume /Users/justin/Documents/Python_Projects/RS_Price_Analysis/data:/data --volume /Users/justin/Documents/Python_Projects/RS_Price_Analysis/public:/public rs_pricing_analysis
# commit & push output HTML
cd ~/Documents/Python_Projects/RS_Price_Analysis/
git add public/
cur_date=$(date +"%Y-%m-%dT%H:%M:%S")
git commit -m "$cur_date site pub"
git push -u origin main