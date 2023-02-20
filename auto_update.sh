#!/bin/bash
cd ~/Documents/Python_Projects/RS_Price_Analysis/
pipenv_path=$(where pipenv)
$pipenv_path run python RS_Pricing.py
git add public/
cur_date=$(date +"%Y-%m-%dT%H:%M:%S")
git commit -m "$cur_date site pub"
git push -u origin main