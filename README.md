# 22_WatchlistStreamer

## Use case 

There can be overwhelming amount of information when watching multiple stocks moving in US stock market. This can lead to decision fatigue and eventually poor decision making. Human brain has only limited capacity to handle multiple decisions. 

Therefore my conclusion has been the some of this monitoring could and should be automated. This is my attempt to do that. 

How to monitor multiple stocks in realtime and generate user alarms?


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

Because dozens of tickers may need to be tracked simultaneously, the accumulation of small tasks can become significant. To handle this efficiently, parts of the software have been implemented asynchronously. I have also tried to scale my previous codes as efficiently as possible. We are still not quite there yet, but nowadays I have this common folder where I try to share codes between different projects. 


<img width="557" height="407" alt="image" src="https://github.com/user-attachments/assets/4f6b52da-a1d4-4f2d-a4ac-099968bffda9" />


## Solution

Define strategies based on historical data collection. Let the code do the heavy lifting and wait for alarms. 


