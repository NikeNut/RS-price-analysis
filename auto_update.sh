#!/bin/bash
cd ~/Documents/Python_Projects/RS_Price_Analysis/
/Users/justin/.local/bin/pipenv run python RS_Pricing.py
git add public/
cur_date=$(date +"%Y-%m-%dT%H:%M:%S")
git commit -m "$cur_date site pub"
git push -u origin main