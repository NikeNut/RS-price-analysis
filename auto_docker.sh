#!/bin/bash
# run docker container
# must be run locally first: docker build -t rs_pricing_analysis .
/usr/local/bin/docker run -e TZ=America/New_York --memory "250m" --memory-swap "500m" --cpus="0.5" --volume /Users/justin/Documents/Python_Projects/RS_Price_Analysis/data:/data --volume /Users/justin/Documents/Python_Projects/RS_Price_Analysis/public:/public rs_pricing_analysis
# commit & push output HTML
cd ~/Documents/Python_Projects/RS_Price_Analysis/
git add public/
cur_date=$(date +"%Y-%m-%dT%H:%M:%S")
git commit -m "$cur_date site pub"
git push -u origin main