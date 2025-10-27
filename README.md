## 22_WatchlistStreamer

# Use case 

My basic assumption is that equity pricing in stock markets is not completely efficient. Investors and traders are not rational and that there are mispricings.
There can be overwhelming amount of information when watching multiple stocks moving in US stock market. This can lead to decision fatigue and poor quality decisions. Therefore some of that monitoring should be automated. 

How to monitor multiple stocks in realtime and generate user alarms. 

# Project structure

alarms folder 
Responsible for generating alarms and sending telegram messages

common folder
Codes that are used in multiple projects

database folder
PostgreSQL database communicating codes are here

helpers folder
Not core codes but essential ones

streamer
Actual data streamer which initilizes database and prepares it for incoming stream data

symbol_loader
This is use for input data reading. User can give list of stocks

tickers
Input data in .txt format is here

strategies.py is responsible for running multiple strategies on livestream. In some sense this can be considered data consumer.


# Software architecture

<img width="1113" height="809" alt="image" src="https://github.com/user-attachments/assets/b4fc4d4b-c5bd-4b35-86cc-564d7ddfb376" />

# Solution

Strategy parameters are defined according my historical dataset. I define couple of thresholds and certain triggers to generate alarms. 

Because there can be a quite many little tasks ongoing simultaneously for multiple symbols, I have build part of the system asyncronously. This has allowed much greater system performance. Even with dozens of symbols running through strategies it takes only couple of seconds. There is eventloop for listening livedatastream and it's inserted to database. Initial part is done synchronously.
