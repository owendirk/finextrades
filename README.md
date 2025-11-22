# finextrades.py

## super simple script to pull trades/tick data from bitfinex exchange
set desired start date, set symbol, and will start getting historical trades from start date to current todays date
adhere to bitfinex rate limits of 10000 trades at a time.
will create a folder directory called bitfinex_data, by year, by month, all trades for the month in the .csv file.
csv file naming format: symbol_year-month.csv
csv headers and data
id,mts,datetime,amount,price
4489165,1420070400000,2015-01-01T00:00:00+00:00,-7.88069332,322.27
4489166,1420070400000,2015-01-01T00:00:00+00:00,-3.50365591,322.29
4489164,1420070400000,2015-01-01T00:00:00+00:00,-49.84,322.3
4489163,1420070400000,2015-01-01T00:00:00+00:00,-0.77565077,322.31
4489169,1420070433000,2015-01-01T00:00:33+00:00,-0.77213037,322.09


saves to the file's(finextrades.py) location in a folder

<img width="720" height="320" alt="image" src="https://github.com/user-attachments/assets/79bf6611-a1a9-474f-a249-0c3ce60974db" />

to run the script in terminal:
python finextrades.py
