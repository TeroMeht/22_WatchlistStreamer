# 22_WatchlistStreamer

## Use case 

How to monitor multiple stocks in realtime and generate user alarms. There can be overwhelming amount of information when watching multiple stocks moving in US stock market. This can lead to decision fatigue and poor quality decisions.
Therefore some of that monitoring should be automated. This is attempt to do that. 


## Project structure

alarms folder 
Responsible for generating alarms and sending telegram messages

common folder
Codes that are used in multiple projects

database folder
PostgreSQL database communicating codes are here

helpers folder
Not very clear where they belong

streamer
Actual data streamer which initilizes database and prepares it for incoming stream data

symbol_loader
This is use for input data reading. User can give list of stocks

tickers
Input data in .txt format is here

strategies.py is responsible for running multiple strategies on livestream. In some sense this can be considered data consumer.


## Software architecture

<img width="557" height="407" alt="image" src="https://github.com/user-attachments/assets/4f6b52da-a1d4-4f2d-a4ac-099968bffda9" />


## Solution

Define strategies based on historical data collection. Let the code do the heavy lifting and wait for alarms. 


