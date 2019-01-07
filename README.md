## Crypto-analysis
Provide aggregated information for these events:
- A currency wins for each interval. Winning = highest change in pct.
- There is no trade for x currencies in during an interval

Open ```Crytocurrencies analysis.html``` for a quick review

## How to run
Firstly, restore packages with Pipenv

The program has 2 phases
- Aggregating the data
- Run analysis on the data

To aggregate the data
- You need a postgres instance
- Fetch data using this https://github.com/fuksi/bfxdata
    * branch: tradings
    * flags: --includetradings
    * make sure you have correct connection string in ```db.py```
- Aggregate using ```main.py``` in this project

The analysis is done with jupyter notebook
- notebook file ```Crytocurrencies analysis.ipynb```
- just re-execute all cells
