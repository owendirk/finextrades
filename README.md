# finextrades.py

## super simple script to pull trades/tick data from bitfinex exchange
set desired start date, set symbol, and will start getting historical trades from start date to current todays date
adhere to bitfinex rate limits of 10000 trades at a time.
will create a folder directory called bitfinex_data, by year, by month, all trades for the month in the .csv file.
saves to the file's(finextrades.py) location in a folder
<img width="1492" height="684" alt="image" src="https://github.com/user-attachments/assets/79bf6611-a1a9-474f-a249-0c3ce60974db" />

to run the script in terminal:
python finextrades.py
