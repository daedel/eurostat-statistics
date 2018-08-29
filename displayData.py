import pandas as pd
import sqlite3
import sys
import matplotlib.pyplot as plt



userInput = [] #0: country, 1: tradeType

if len(sys.argv) > 1 and len(sys.argv[1]) == 2:
    userInput.append(sys.argv[1]) # Country
else:
    print("Wrong country input")
    sys.exit(0)


if len(sys.argv) > 2 and ('I' in sys.argv[2] or 'E' in sys.argv[2]):
    userInput.append(sys.argv[2]) # Trade type
else:
    print("Wrong Trade type input")
    sys.exit(0)

conn = sqlite3.connect("MyDb.db")

query = r"SELECT `VALUE_IN_EUROS`, `PERIOD` FROM Eurostat WHERE DECLARANT_ISO='{}' AND TRADE_TYPE='{}';".format(userInput[0], userInput[1])

df = pd.read_sql_query(query, conn, parse_dates={'PERIOD':'%Y%m'})
if df.empty:
    print("No available data!")
    sys.exit(0)
gb = df.groupby('PERIOD').sum()
gb2 = gb.copy()
try:
    gb2['pChange'] = gb2.VALUE_IN_EUROS.pct_change().plot(legend=True, label='Change(percentage) compared to prev. month')
except:
    print("Not enough data")

try:
    gb2['pChange12'] = gb2.VALUE_IN_EUROS.pct_change(12).plot(legend=True, label='Change(percentage) compared to prev. year')
except:
    print("Not enough data")


gb['MA_12'] = gb.VALUE_IN_EUROS.rolling(12).mean()

if gb['MA_12'].empty:
    gb.drop('MA_12', 1)

gb.plot()
plt.show()

