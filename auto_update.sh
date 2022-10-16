#!/bin/bash
cd ~/Documents/Python_Projects/RS_Price_Analysis/
pipenv run python RS_Pricing.py
git add public/
git commit -m "2022-10-16 17 site pub"
git push -u origin main