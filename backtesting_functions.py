import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import itertools
import concurrent.futures
import pickle

# when_open: this_bar | next_bar
# when_close: this_bar | next_bar
# price_open_long: column reference
# price_open_short: column reference
# price_close_long: column reference
# price_close_short: column reference

# order_out: lista = ['stop_loss', 'take_profit', 'trigger_out']



def motor_run(universe_filter, universe_size, when_open, when_close, price_close_long, price_close_short, price_open_long, price_open_short, order_out, df_dict, open_positions_long, open_positions_short, account_size_fw, available, tickers_list_b, delta, margin, position_size_st, short_selling, time_stop, stop_selection, trailing, comision, s_l, t_p):


    closed_positions = pd.DataFrame()
    closed_positions['Close_Date'] = ''
    closed_positions['Ticker'] = ''
    closed_positions['Direction'] = ''
    closed_positions['Open_Date'] = ''
    closed_positions['Open_Price'] = ''
    closed_positions['Close_Price'] = ''
    closed_positions['Position_Size'] = ''
    closed_positions['Indiv_PL%'] = ''
    closed_positions['Closed_Size'] = ''
    closed_positions['Comissions'] = ''
    closed_positions['Profit/Loss_$'] = ''
    closed_positions['Profit/Loss_%'] = ''
    closed_positions['Account_Size'] = ''

    trade_id = 0

    #acá defino el df más largo de todos los activos para que velas tenga el index más largo de todos:
    longest = ''
    longest_len = 0
    for token in tickers_list_b:
        if len(df_dict[token]) > longest_len:
            longest_len = len(df_dict[token])
            longest = token

    velas = pd.DataFrame()
    velas = df_dict[longest].copy()
    velas['Positions'] = 0
    velas['Closed_Size'] = 0
    velas['Open_pl'] = 0
    velas['Curr_Size'] = 0

    #print(velas)
    for i in velas.index:

        if universe_filter == None:
            todays_universe = tickers_list_b
        else:
            todays_universe = universe_filter(df_dict, i, universe_size)

        for ticker in open_positions_long.copy():
            # desactivado por universo variable diariamente, puede volver a servir
            # if ticker not in tickers_list_b:  # esto puede ocurrir en walk_forward con cambio de cartera
            #     closed_positions.loc[trade_id,'Close_Date'] = velas.loc[i,'Date']
            #     closed_positions.loc[trade_id,'Ticker'] = ticker
            #     closed_positions.loc[trade_id,'Direction'] = 'long'
            #     closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
            #     closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
            #     closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Last_Price']
            #     closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
            #     closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
            #     closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
            #     closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
            #     closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
            #     closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
            #     account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
            #     closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
            #     available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
            #     trade_id += 1
            #     del open_positions_long[ticker]
            # elif
            if (time_stop > 0) and (open_positions_long[ticker]['Bars_In'] >= time_stop): # esto se chequea en el Open
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]

            elif (order_out[0] == 'stop_loss') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'Low'] <= open_positions_long[ticker]['Stop_Value']): #salida de long por SL
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] <= open_positions_long[ticker]['Stop_Value']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Stop_Value']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[0] == 'stop_loss') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] <= open_positions_long[ticker]['Stop_Value']): #salida de long por SL
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[0] == 'take_profit') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'High'] >= open_positions_long[ticker]['Take_Profit']): #salida de long por TP
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] >= open_positions_long[ticker]['Take_Profit']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']   
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Take_Profit']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]  

            elif (order_out[0] == 'take_profit') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] >= open_positions_long[ticker]['Take_Profit']): #salida de long por TP
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[0] == 'trigger_out') and df_dict[ticker].loc[i,'trigger_long_out'] == 1: #salida de long por señal de salida
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                    except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,price_close_long]
                    except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,price_close_long]
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]    

            elif (order_out[1] == 'stop_loss') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'Low'] <= open_positions_long[ticker]['Stop_Value']): #salida de long por SL
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] <= open_positions_long[ticker]['Stop_Value']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Stop_Value']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[1] == 'stop_loss') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] <= open_positions_long[ticker]['Stop_Value']): #salida de long por SL
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[1] == 'take_profit') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'High'] >= open_positions_long[ticker]['Take_Profit']): #salida de long por TP
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] >= open_positions_long[ticker]['Take_Profit']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']   
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Take_Profit']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]  

            elif (order_out[1] == 'take_profit') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] >= open_positions_long[ticker]['Take_Profit']): #salida de long por TP
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[1] == 'trigger_out') and df_dict[ticker].loc[i,'trigger_long_out'] == 1: #salida de long por señal de salida
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                    except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,price_close_long]
                    except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,price_close_long]
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]

            elif (order_out[2] == 'stop_loss') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'Low'] <= open_positions_long[ticker]['Stop_Value']): #salida de long por SL
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] <= open_positions_long[ticker]['Stop_Value']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Stop_Value']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[2] == 'stop_loss') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] <= open_positions_long[ticker]['Stop_Value']): #salida de long por SL
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[2] == 'take_profit') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'High'] >= open_positions_long[ticker]['Take_Profit']): #salida de long por TP
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] >= open_positions_long[ticker]['Take_Profit']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']   
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_long[ticker]['Take_Profit']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (open_positions_long[ticker]['Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]  

            elif (order_out[2] == 'take_profit') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] >= open_positions_long[ticker]['Take_Profit']): #salida de long por TP
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker] 

            elif (order_out[2] == 'trigger_out') and df_dict[ticker].loc[i,'trigger_long_out'] == 1: #salida de long por señal de salida
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                    except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'long'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_long[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_long[ticker]['Open_Price']
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,price_close_long]
                    except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,price_close_long]
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_long[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Close_Price'] - closed_positions.loc[trade_id,'Open_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1+closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Closed_Size']-closed_positions.loc[trade_id,'Position_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_long[ticker]
                
            else: #acá debajo actualizo lo que quiera actualizar si la posición sigue abierta, trailing, open_pnl, etc:
                open_positions_long[ticker]['Bars_In'] += 1
                open_positions_long[ticker]['Open_pl'] = (open_positions_long[ticker]['Position_Size']*((df_dict[ticker].loc[i,'Close'] - open_positions_long[ticker]['Open_Price'])/open_positions_long[ticker]['Open_Price']))
                velas.loc[i,'Open_pl'] += open_positions_long[ticker]['Open_pl']
                open_positions_long[ticker]['Last_Price'] = df_dict[ticker].loc[i,'Close']
                #actualizo el trailing stop si corresponde
                # if (trailing == True) and (df_dict[ticker].loc[i,'Close'] *(1-s_l) > open_positions_long[ticker]['Stop_Value']): 
                #     open_positions_long[ticker]['Stop_Value'] = df_dict[ticker].loc[i,'Close'] *(1-s_l)
                # if (trailing == True) and (df_dict[ticker].loc[i,'Close'] < df_dict[ticker].loc[i,'ema']): 
                #     open_positions_long[ticker]['Stop_Value'] = df_dict[ticker].loc[i,'Low']
                if (trailing == True): 
                    open_positions_long[ticker]['Stop_Value'] = df_dict[ticker].loc[i,'Low']
                        
                
        for ticker in open_positions_short.copy():
            # desactivado por universo variable diariamente; puede volver a servir
            # if ticker not in tickers_list_b:  # esto puede ocurrir en walk_forward con cambio de cartera
            #     closed_positions.loc[trade_id,'Close_Date'] = velas.loc[i,'Date'] 
            #     closed_positions.loc[trade_id,'Ticker'] = ticker
            #     closed_positions.loc[trade_id,'Direction'] = 'short'
            #     closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
            #     closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
            #     closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Last_Price']
            #     closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
            #     closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
            #     closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
            #     closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
            #     closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
            #     closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
            #     account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
            #     closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
            #     available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
            #     trade_id += 1
            #     del open_positions_short[ticker]  
            # elif
            if (time_stop > 0) and (open_positions_short[ticker]['Bars_In'] >= time_stop): # esto se chequea al open
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] 
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]  

            elif (order_out[0] == 'stop_loss') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'High'] >= open_positions_short[ticker]['Stop_Value']): #salida de short por SL
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] >= open_positions_short[ticker]['Stop_Value']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Stop_Value']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]   

            elif (order_out[0] == 'stop_loss') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] >= open_positions_short[ticker]['Stop_Value']): #salida de short por SL
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker] 

            elif (order_out[0] == 'take_profit') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'Low'] <= open_positions_short[ticker]['Take_Profit']): #salida de short por TP
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] <= open_positions_short[ticker]['Take_Profit']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Take_Profit']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker] 

            elif (order_out[0] == 'take_profit') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] <= open_positions_short[ticker]['Take_Profit']): #salida de short por TP
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]  

            elif (order_out[0] == 'trigger_out') and df_dict[ticker].loc[i,'trigger_short_out'] == 1: #salida de short por señal de salida
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                    except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,price_close_short]
                    except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,price_close_short]
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]

            elif (order_out[1] == 'stop_loss') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'High'] >= open_positions_short[ticker]['Stop_Value']): #salida de short por SL
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] >= open_positions_short[ticker]['Stop_Value']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Stop_Value']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]   

            elif (order_out[1] == 'stop_loss') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] >= open_positions_short[ticker]['Stop_Value']): #salida de short por SL
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker] 

            elif (order_out[1] == 'take_profit') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'Low'] <= open_positions_short[ticker]['Take_Profit']): #salida de short por TP
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] <= open_positions_short[ticker]['Take_Profit']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Take_Profit']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker] 

            elif (order_out[1] == 'take_profit') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] <= open_positions_short[ticker]['Take_Profit']): #salida de short por TP
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]  

            elif (order_out[1] == 'trigger_out') and df_dict[ticker].loc[i,'trigger_short_out'] == 1: #salida de short por señal de salida
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                    except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,price_close_short]
                    except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,price_close_short]
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]  

            elif (order_out[2] == 'stop_loss') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'High'] >= open_positions_short[ticker]['Stop_Value']): #salida de short por SL
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] >= open_positions_short[ticker]['Stop_Value']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Stop_Value']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]   

            elif (order_out[2] == 'stop_loss') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] >= open_positions_short[ticker]['Stop_Value']): #salida de short por SL
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker] 

            elif (order_out[2] == 'take_profit') and (stop_selection == 'seteado') and (df_dict[ticker].loc[i,'Low'] <= open_positions_short[ticker]['Take_Profit']): #salida de short por TP
                closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #si no funciona Date, entonces Name o index
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if df_dict[ticker].loc[i,'Open'] <= open_positions_short[ticker]['Take_Profit']:
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Open']
                else:
                    closed_positions.loc[trade_id,'Close_Price'] = open_positions_short[ticker]['Take_Profit']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker] 

            elif (order_out[2] == 'take_profit') and (stop_selection == 'al_cierre') and (df_dict[ticker].loc[i,'Close'] <= open_positions_short[ticker]['Take_Profit']): #salida de short por TP
                try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,'Open']
                except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]  

            elif (order_out[2] == 'trigger_out') and df_dict[ticker].loc[i,'trigger_short_out'] == 1: #salida de short por señal de salida
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                    except KeyError: closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date'] #esto es por si justo le toca cerrar el último día del df
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Date'] = df_dict[ticker].loc[i,'Date']
                closed_positions.loc[trade_id,'Ticker'] = ticker
                closed_positions.loc[trade_id,'Direction'] = 'short'
                closed_positions.loc[trade_id,'Open_Date'] = open_positions_short[ticker]['Open_Date']
                closed_positions.loc[trade_id,'Open_Price'] = open_positions_short[ticker]['Open_Price']
                if when_close == 'next_bar':
                    try: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i+delta,price_close_short]
                    except KeyError: closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,'Close']
                elif when_close == 'this_bar':
                    closed_positions.loc[trade_id,'Close_Price'] = df_dict[ticker].loc[i,price_close_short]
                closed_positions.loc[trade_id,'Position_Size'] = open_positions_short[ticker]['Position_Size']
                closed_positions.loc[trade_id,'Indiv_PL%'] = ((closed_positions.loc[trade_id,'Open_Price']-closed_positions.loc[trade_id,'Close_Price'])/closed_positions.loc[trade_id,'Open_Price']) #no incluye comisión
                closed_positions.loc[trade_id,'Closed_Size'] = closed_positions.loc[trade_id,'Position_Size'] * (1-closed_positions.loc[trade_id,'Indiv_PL%'])
                closed_positions.loc[trade_id,'Comissions'] = (closed_positions.loc[trade_id,'Position_Size']*comision) + (closed_positions.loc[trade_id,'Closed_Size']*comision)
                closed_positions.loc[trade_id,'Profit/Loss_$'] = (closed_positions.loc[trade_id,'Position_Size']-closed_positions.loc[trade_id,'Closed_Size'])-closed_positions.loc[trade_id,'Comissions']
                closed_positions.loc[trade_id,'Profit/Loss_%'] = closed_positions.loc[trade_id,'Profit/Loss_$']/account_size_fw
                account_size_fw += closed_positions.loc[trade_id,'Profit/Loss_$']
                closed_positions.loc[trade_id,'Account_Size'] = account_size_fw
                available += (closed_positions.loc[trade_id,'Position_Size'] + closed_positions.loc[trade_id,'Profit/Loss_$']*margin)
                trade_id += 1
                del open_positions_short[ticker]

            else: #acá debajo actualizo lo que quiera actualizar si la posición sigue abierta, trailing, open_pnl, etc:
                open_positions_short[ticker]['Bars_In'] += 1
                open_positions_short[ticker]['Open_pl'] = (open_positions_short[ticker]['Position_Size']*((open_positions_short[ticker]['Open_Price'] - df_dict[ticker].loc[i,'Close'])/open_positions_short[ticker]['Open_Price']))
                velas.loc[i,'Open_pl'] += open_positions_short[ticker]['Open_pl']
                open_positions_short[ticker]['Last_Price'] = df_dict[ticker].loc[i,'Close']
                #actualizo el trailing stop si corresponde
                # if (trailing == True) and (df_dict[ticker].loc[i,'Close'] *(1+s_l) < open_positions_short[ticker]['Stop_Value']): 
                #     open_positions_short[ticker]['Stop_Value'] = df_dict[ticker].loc[i,'Close'] *(1+s_l)
                if (trailing == True): 
                    open_positions_short[ticker]['Stop_Value'] = df_dict[ticker].loc[i,'High']
            
        if available >= (position_size_st*account_size_fw): #este primer chequeo se puede evitar
            for ticker in todays_universe:
                if available >= (position_size_st*account_size_fw):
                    if (ticker not in open_positions_long) and (ticker not in open_positions_short):
                        try:
                            if df_dict[ticker].loc[i,'trigger_long_in'] == 1:
                                open_positions_long[ticker] = {}
                                if when_open == 'next_bar':
                                    try: open_positions_long[ticker]['Open_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                                    except KeyError: open_positions_long[ticker]['Open_Date'] = df_dict[ticker].loc[i,'Date']
                                elif when_open == 'this_bar':
                                    open_positions_long[ticker]['Open_Date'] = df_dict[ticker].loc[i,'Date']
                                if when_open == 'next_bar':
                                    try: open_positions_long[ticker]['Open_Price'] = df_dict[ticker].loc[i+delta,price_open_long] 
                                    except KeyError: open_positions_long[ticker]['Open_Price'] = df_dict[ticker].loc[i,'Close']
                                elif when_open == 'this_bar':
                                    open_positions_long[ticker]['Open_Price'] = df_dict[ticker].loc[i,price_open_long]
                                open_positions_long[ticker]['Position_Size'] = position_size_st*account_size_fw
                                open_positions_long[ticker]['Stop_Value'] = open_positions_long[ticker]['Open_Price'] * (1 - s_l)
                                open_positions_long[ticker]['Take_Profit'] = open_positions_long[ticker]['Open_Price'] * (1 + t_p)
                                open_positions_long[ticker]['Bars_In'] = 0
                                open_positions_long[ticker]['Open_pl'] = 0
                                open_positions_long[ticker]['Last_Price'] = df_dict[ticker].loc[i,'Close']
                                available -= open_positions_long[ticker]['Position_Size'] #esto va a requerir un rechequeo si se opta por posiciones de tamaño distinto entre sí
                            elif (short_selling == True) and (df_dict[ticker].loc[i,'trigger_short_in'] == 1):
                                open_positions_short[ticker] = {}
                                if when_open == 'next_bar':
                                    try: open_positions_short[ticker]['Open_Date'] = df_dict[ticker].loc[i+delta,'Date'] 
                                    except KeyError: open_positions_short[ticker]['Open_Date'] = df_dict[ticker].loc[i,'Date']
                                elif when_open == 'this_bar':
                                    open_positions_short[ticker]['Open_Date'] = df_dict[ticker].loc[i,'Date']
                                if when_open == 'next_bar':
                                    try: open_positions_short[ticker]['Open_Price'] = df_dict[ticker].loc[i+delta,price_open_short]
                                    except KeyError: open_positions_short[ticker]['Open_Price'] = df_dict[ticker].loc[i,'Close']
                                elif when_open == 'this_bar':
                                    open_positions_short[ticker]['Open_Price'] = df_dict[ticker].loc[i,price_open_short]
                                open_positions_short[ticker]['Position_Size'] = position_size_st*account_size_fw
                                open_positions_short[ticker]['Stop_Value'] = open_positions_short[ticker]['Open_Price'] * (1 + s_l)
                                open_positions_short[ticker]['Take_Profit'] = open_positions_short[ticker]['Open_Price'] * (1 - t_p)
                                open_positions_short[ticker]['Bars_In'] = 0
                                open_positions_short[ticker]['Open_pl'] = 0
                                open_positions_short[ticker]['Last_Price'] = df_dict[ticker].loc[i,'Close']
                                available -= open_positions_short[ticker]['Position_Size'] #esto va a requerir un rechequeo si se opta por posiciones de tamaño distinto entre sí
                        except KeyError:
                            continue

        velas.loc[i,'Positions'] = len(open_positions_long)+len(open_positions_short)
        # velas.loc[i, 'Available'] = available
        velas.loc[i,'Closed_Size'] = account_size_fw
        velas.loc[i,'Curr_Size'] = velas.loc[i,'Closed_Size'] + velas.loc[i,'Open_pl']   

    return closed_positions, account_size_fw, open_positions_long, open_positions_short, available, velas


def add_results(df, settings, init_account, delta):
    acum_pl = [0]*len(df)
    op_pl = [0]*len(df)

    if type(settings['short_selling']) == list: #si settings es un dict de listas:
        #si df es un filtrado de longs o shorts solamente:
        if (True in settings['short_selling']) and ((len(df[df['Direction'] == 'short']) == len(df)) or (len(df[df['Direction'] == 'long']) == len(df))):
            for i in range(len(df)):
                op_pl[i] = df['Profit/Loss_%'][i]
            df['Op_pl'] = op_pl
            df['Acum_pl'] = (df.Op_pl + 1).cumprod().round(3)
        else: #si no lo es (osea que son todos los trades)
            for i in range(len(df)):
                acum_pl[i] = df.Account_Size[i] / init_account
            df['Acum_pl'] = acum_pl
    else: #en el caso que settings sea un dict común, no de listas:
        #si df es un filtrado de longs o shorts solamente:
        if (settings['short_selling'] == True) and ((len(df[df['Direction'] == 'short']) == len(df)) or (len(df[df['Direction'] == 'long']) == len(df))):
            for i in range(len(df)):
                op_pl[i] = df['Profit/Loss_%'][i]
            df['Op_pl'] = op_pl
            df['Acum_pl'] = (df.Op_pl + 1).cumprod().round(3)
        else: #si no lo es (osea que son todos los trades)
            for i in range(len(df)):
                acum_pl[i] = df.Account_Size[i] / init_account
            df['Acum_pl'] = acum_pl
    
    dd_ = [0]*len(df)
    max_pl = [0]*len(df)
    win_row = [0]*len(df)
    lose_row = [0]*len(df)
    for i in range(1,len(df)):
        if df.Acum_pl[i] > max_pl[i-1]:
            max_pl[i] = df.Acum_pl[i]
        else:
            max_pl[i] = max_pl[i-1]
        if max_pl[i] == 0:
            if df.Acum_pl[i] < df.Acum_pl[i-1]:
                dd_[i] = df.Acum_pl[i]
            else:
                dd_[i] = dd_[i-1]
        else:
            dd_[i] = (((df.Acum_pl[i] - max_pl[i])/max_pl[i])*100).round(2)
        if df['Profit/Loss_$'][i] >= 0:
            win_row[i] = win_row[i-1]+1
            lose_row[i] = 0
        elif df['Profit/Loss_$'][i] < 0:
            lose_row[i] = lose_row[i-1]+1
            win_row[i] = 0
    df['Drawdown_%'] = dd_
    df['Win_row'] = win_row
    df['Lose_row'] = lose_row
    
    duration = [0]*len(df)
    for i in range(len(df)):
        if df.Close_Date[i] == df.Open_Date[i]:
            duration[i] = delta
        else:
            duration[i] = (df.Close_Date[i] - df.Open_Date[i])
    df['Duration'] = duration

    return df


def get_new_metrics(df, name, settings, strategy_set, df_dict, tickers_list_b, velas, contador):
    import quantstats as qs

    metrics = pd.DataFrame()
    row_name = name
    
    metrics.loc[row_name,'sheet'] = str(contador)
    for parameteer in settings.keys():
        metrics.loc[row_name, str(parameteer)] = str(settings[parameteer])
    for parameter in strategy_set.keys():
        metrics.loc[row_name, str(parameter)] = str(strategy_set[parameter])
    
    if len(df) > 0:
        metrics.loc[row_name,'profit_%'] = (df.loc[df.index[-1],'Acum_pl'] -1)*100
        metrics.loc[row_name,'max_dd'] = min(df['Drawdown_%'])
        metrics.loc[row_name,'n_trades'] = len(df)
        metrics.loc[row_name,'perc_profitable'] =qs.stats.win_rate(df['Profit/Loss_%'])*100
        metrics.loc[row_name,'winning_trades'] = (metrics.loc[row_name,'perc_profitable']*metrics.loc[row_name,'n_trades']/100)
        metrics.loc[row_name,'losing_trades'] =(metrics.loc[row_name,'n_trades'] - metrics.loc[row_name,'winning_trades'])
        metrics.loc[row_name,'avg_trade'] = df['Profit/Loss_%'].mean()*100
        filtro_w=df[df['Profit/Loss_%']>=0]
        metrics.loc[row_name,'avg_win'] = (filtro_w['Profit/Loss_%'].mean()*100)
        filtro_l=df[df['Profit/Loss_%']<0]
        metrics.loc[row_name,'avg_loss'] = (filtro_l['Profit/Loss_%'].mean()*100)
        metrics.loc[row_name,'ratio_wl']=(metrics.loc[row_name,'avg_win']/(-metrics.loc[row_name,'avg_loss']))
        metrics.loc[row_name,'edge'] = (metrics.loc[row_name,'perc_profitable']*metrics.loc[row_name,'ratio_wl'])/100 - (1 - metrics.loc[row_name,'perc_profitable']/100)
        metrics.loc[row_name,'profit_factor'] = (filtro_w['Profit/Loss_%'].sum() / filtro_l['Profit/Loss_%'].sum()*-1)
        metrics.loc[row_name,'Concec_wins'] = max(df['Win_row'])
        metrics.loc[row_name,'Concec_losses'] = max(df['Lose_row'])
        metrics.loc[row_name,'avg_duration'] = df['Duration'].mean()
        rel_exp = lambda x: (x['Duration'] * settings['position_size_st'])
        metrics.loc[row_name,'total_time'] = (velas['Date'][-1] - velas['Date'][0])
        metrics.loc[row_name,'total_exposure'] = (df.apply(rel_exp,axis=1).sum() / metrics.loc[row_name,'total_time'])
        aux = metrics.loc[row_name,'total_time']/timedelta(days=365)
        metrics.loc[row_name,'CAGR'] = ((df.loc[df.index[-1],'Acum_pl']**(1/aux))-1)*100
        if metrics.loc[row_name,'max_dd'] == 0:
            metrics.loc[row_name, 'Calmar_ratio'] = 'max_dd = 0'
        else:
            metrics.loc[row_name, 'Calmar_ratio'] = (metrics.loc[row_name,'CAGR'] / metrics.loc[row_name,'max_dd'])*-1
    hodl = 0
    for tickera in tickers_list_b:
        try:
            hodl += (((df_dict[tickera].Close[-1] - df_dict[tickera].Close[0])/df_dict[tickera].Close[0])*100)/len(tickers_list_b)
        except IndexError:
            continue
    metrics.loc[row_name,'hodl'] = hodl

    return metrics

def get_new_metrics_beta(df, name, settings, strategy_set, velas, contador):
    import quantstats as qs
    # esta es la versión para el walk forward, no tiene hodl y se reformatea distinto la fecha
    metrics = pd.DataFrame()
    row_name = name
    
    metrics.loc[row_name,'sheet'] = str(contador)
    for parameteer in settings.keys():
        metrics.loc[row_name, str(parameteer)] = str(settings[parameteer])
    for parameter in strategy_set.keys():
        metrics.loc[row_name, str(parameter)] = str(strategy_set[parameter])
    metrics.loc[row_name,'fecha_desde_bt'] = '' #settings['fecha_desde_bt'][0].strftime('%Y-%m-%d') #esto hay que cambiarlo
    metrics.loc[row_name,'fecha_hasta_bt'] = '' #settings['fecha_hasta_bt'][0].strftime('%Y-%m-%d') #y esto

    if len(df) > 0:
        metrics.loc[row_name,'profit_%'] = (df.loc[df.index[-1],'Acum_pl'] -1)*100
        metrics.loc[row_name,'max_dd'] = min(df['Drawdown_%'])
        metrics.loc[row_name,'n_trades'] = len(df)
        metrics.loc[row_name,'perc_profitable'] =qs.stats.win_rate(df['Profit/Loss_%'])*100
        metrics.loc[row_name,'winning_trades'] = (metrics.loc[row_name,'perc_profitable']*metrics.loc[row_name,'n_trades']/100)
        metrics.loc[row_name,'losing_trades'] =(metrics.loc[row_name,'n_trades'] - metrics.loc[row_name,'winning_trades'])
        metrics.loc[row_name,'avg_trade'] = df['Profit/Loss_%'].mean()*100
        filtro_w=df[df['Profit/Loss_%']>=0]
        metrics.loc[row_name,'avg_win'] = (filtro_w['Profit/Loss_%'].mean()*100)
        filtro_l=df[df['Profit/Loss_%']<0]
        metrics.loc[row_name,'avg_loss'] = (filtro_l['Profit/Loss_%'].mean()*100)
        metrics.loc[row_name,'ratio_wl']=(metrics.loc[row_name,'avg_win']/(-metrics.loc[row_name,'avg_loss']))
        metrics.loc[row_name,'edge'] = (metrics.loc[row_name,'perc_profitable']*metrics.loc[row_name,'ratio_wl'])/100 - (1 - metrics.loc[row_name,'perc_profitable']/100)
        metrics.loc[row_name,'profit_factor'] = (filtro_w['Profit/Loss_%'].sum() / filtro_l['Profit/Loss_%'].sum()*-1)
        metrics.loc[row_name,'Concec_wins'] = max(df['Win_row'])
        metrics.loc[row_name,'Concec_losses'] = max(df['Lose_row'])
        metrics.loc[row_name,'avg_duration'] = df['Duration'].mean()
        rel_exp = lambda x: (x['Duration'] * settings['position_size_st'][0])
        metrics.loc[row_name,'total_time'] = (velas['Date'][-1] - velas['Date'][0])
        metrics.loc[row_name,'total_exposure'] = (df.apply(rel_exp,axis=1).sum() / metrics.loc[row_name,'total_time'])
        aux = metrics.loc[row_name,'total_time']/timedelta(days=365)
        metrics.loc[row_name,'CAGR'] = ((df.loc[df.index[-1],'Acum_pl']**(1/aux))-1)*100
        if metrics.loc[row_name,'max_dd'] == 0:
            metrics.loc[row_name, 'Calmar_ratio'] = 'max_dd = 0'
        else:
            metrics.loc[row_name, 'Calmar_ratio'] = (metrics.loc[row_name,'CAGR'] / metrics.loc[row_name,'max_dd'])*-1
    hodl = 0
    # for tickera in tickers_list_b:
    #     try:
    #         hodl += (((df_dict[tickera].Close[-1] - df_dict[tickera].Close[0])/df_dict[tickera].Close[0])*100)/len(tickers_list_b)
    #     except: #IndexError
    #         continue
    metrics.loc[row_name,'hodl'] = hodl
    
    return metrics


def unit_backtest(source, universe_filter, setting, account_sets, strategy, strategy_set, contador):
    #timeframe = {'1m' :  Client.KLINE_INTERVAL_1MINUTE, '3m' : Client.KLINE_INTERVAL_3MINUTE,'5m' : Client.KLINE_INTERVAL_5MINUTE,'15m' : Client.KLINE_INTERVAL_15MINUTE,'30m' : Client.KLINE_INTERVAL_30MINUTE,'1h' : Client.KLINE_INTERVAL_1HOUR,'2h' : Client.KLINE_INTERVAL_2HOUR,'4h' : Client.KLINE_INTERVAL_4HOUR,'6h' : Client.KLINE_INTERVAL_6HOUR,'8h' : Client.KLINE_INTERVAL_8HOUR,'12h' : Client.KLINE_INTERVAL_12HOUR,'1d' : Client.KLINE_INTERVAL_1DAY}
    minutes = {'1min' : 1, '3min' : 3,'5min' : 5,'15min' : 15,'30min' : 30,'1H' : 60,'90min' : 90, '2H' : 120,'4H' : 240,'6H' : 360,'8H' : 480,'12H' : 720,'1D' : 1440}
    agg_dict = {'Open' : 'first','High' : 'max','Low' : 'min','Close' : 'last','Volume' : 'sum','Date' : 'first'}

    tickers_list_b = setting['tickers_list']

    if universe_filter == None:
        universe_size = len(tickers_list_b)
    else:
        universe_size = setting['universe_size']
    # cargamos el dataframe dict:
    with open(source,'rb') as p:
        df_dict = pickle.load(p)

    # modificamos el df_dict según la estrategia:
    not_found = []
    for token1 in tickers_list_b: 
    
        data = df_dict[token1]
        # if minutes[setting['interval']] < 1440:
        #     if minutes[setting['interval']] > 60:
        #         my_offset = '1h'
        #     else: my_offset = None
        #     data = candles.resample(setting['interval'],offset=my_offset).agg(agg_dict)
        #     #data = pd.read_sql(token1,'sqlite:///prueba_df_dict.db').set_index('timestamp')
        
        strategy_set['file'] = source
        strategy_set['token'] = token1
        strategy_set['interval'] = setting['interval']

        data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short = strategy(data,strategy_set)

        del strategy_set['file']
        del strategy_set['token']
        del strategy_set['interval']

        data = data.dropna()

        data = data[data.index >= setting['fecha_desde_bt']]
        data = data[data.index <= setting['fecha_hasta_bt']]

        df_dict[token1] = data 
        

    #print(df_dict['ETH'])#.to_excel('prueba.xlsx')

    
    account_size_bt = account_sets['init_account']
    init_account_bt = account_size_bt
    available = account_size_bt * account_sets['account_margin']


    delta = timedelta(minutes=minutes[setting['interval']])
    
    open_positions_long = {} #key=ticker : {stop_value, take_profit, open_price, close_price, open_date, position_size, *bars_in, *open_pl}
    open_positions_short = {}


    closed_positions, account_size_r, open_positions_long_r, open_positions_short_r, available_r, velas = motor_run(universe_filter, universe_size, when_open, when_close, price_close_long, price_close_short, price_open_long, price_open_short, order_out, df_dict, open_positions_long, open_positions_short, account_size_bt, available, tickers_list_b, delta, account_sets['account_margin'], setting['position_size_st'], setting['short_selling'], setting['time_stop'], setting['stop_selection'], setting['trailing'], setting['comision'], setting['stop_loss'], setting['take_profit'])


    if setting['short_selling'] == True:
        closed_long = closed_positions[closed_positions.Direction == 'long'].copy()
        closed_long.reset_index(inplace=True)
        closed_short = closed_positions[closed_positions.Direction == 'short'].copy()
        closed_short.reset_index(inplace=True)

    closed_positions = add_results(closed_positions, setting, init_account_bt, delta)

    if setting['short_selling'] == True:
        add_results(closed_long, setting, init_account_bt, delta)
        add_results(closed_short, setting, init_account_bt, delta)

    if (len(tickers_list_b) == 1):
        name = str(tickers_list_b)+'_'+str(contador)
    else:
        name = 'comb_'+str(contador)

    metrics = get_new_metrics(closed_positions, name, setting, strategy_set, df_dict, tickers_list_b, velas, contador)
    
    result_dict = {}
    result_dict[name] = [setting, strategy_set]
   
    if setting['short_selling'] == True:
        metrics_long = get_new_metrics(closed_long,name, setting, strategy_set, df_dict, tickers_list_b, velas, contador)
        metrics_short = get_new_metrics(closed_short,name, setting, strategy_set, df_dict, tickers_list_b, velas, contador)
    else:
        metrics_long = pd.DataFrame()
        metrics_short = pd.DataFrame()

    sheet = 'closed_'+str(contador)
        
    velas = velas[['Positions','Closed_Size','Open_pl','Curr_Size']].copy()
  

    return contador, sheet, closed_positions, velas, metrics, metrics_long, metrics_short, result_dict


def mp_backtest(source, universe_filter, settings, account_sets, strategy, strategy_sets, save_to):
    
    with concurrent.futures.ProcessPoolExecutor() as e: 

        output_metrics = pd.DataFrame()
        output_metrics_long = pd.DataFrame()
        output_metrics_short = pd.DataFrame()
        results_dict = {}
        w = None

        if save_to != None:
            output_metrics.to_excel(save_to +'.xlsx',sheet_name = 'Métricas')
            w = pd.ExcelWriter(save_to +'.xlsx')

        Id = 0
        init_account_bt = account_sets['init_account']

        keys1, values1 = zip(*settings.items())
        setting_combos = [dict(zip(keys1, v1)) for v1 in itertools.product(*values1)]

        keys2, values2 = zip(*strategy_sets.items())
        strat_combos = [dict(zip(keys2, v2)) for v2 in itertools.product(*values2)]   

        process_list = []
        for setting in setting_combos:
            for strategy_set in strat_combos:
                Id += 1
                p1 = e.submit(unit_backtest, source, universe_filter, setting, account_sets, strategy, strategy_set, Id)
                process_list.append(p1)

        counter = 0        
        for p in process_list:
            
            resultados = p.result()
            contador = resultados[0]
            sheet = resultados[1]
            closed_positions = resultados[2]
            velas = resultados[3]
            metrics = resultados[4]
            metrics_long = resultados[5]
            metrics_short = resultados[6]
            result_dict = resultados[7]

            output_metrics = pd.concat([output_metrics,metrics],axis=0)
                    
            results_dict.update(result_dict)

            if save_to != None:
                
                if True in settings['short_selling']:
                    output_metrics_long = pd.concat([output_metrics_long,metrics_long])
                    output_metrics_short = pd.concat([output_metrics_short,metrics_short])

                closed_positions.to_excel(w,sheet_name=sheet)

                fig, axs = plt.subplots(2, figsize=(15,5))
                axs[0].plot(closed_positions['Close_Date'], closed_positions['Acum_pl'],color = 'grey', linewidth = 3)
                #axs[0].set_yscale('log')
                axs[0].title.set_text('Resultado acumulado')
                axs[1].plot(closed_positions['Close_Date'], closed_positions['Drawdown_%'],color = 'red',linestyle = 'dashed')
                axs[1].title.set_text('Drawdown%')
                axs[0].grid(which = 'major', axis = 'y', color  = 'black', alpha = 0.4)
                axs[0].grid(which = 'minor', axis = 'both', color  = 'black', alpha = 0.15)
                plt.fill_between(closed_positions['Close_Date'], closed_positions['Drawdown_%'], alpha=0.5, color='red')
                fig.subplots_adjust(hspace = 0.3)
                plt.savefig('graf'+sheet+'.png')
                worksheet = w.sheets[sheet]
                worksheet.insert_image('N2','graf'+sheet+'.png')

                if True in settings['show_daily_results']:
                    diario = velas.resample('1d').agg('last').copy()
                    diario.reset_index(inplace=True)
                    daily_pl = [0]*len(diario)
                    for z in range(len(diario)):
                        if z == 0:
                            daily_pl[z] = (diario.Curr_Size[z] - init_account_bt) / init_account_bt
                        else: daily_pl[z] = (diario.Curr_Size[z] - diario.Curr_Size[z-1]) / diario.Curr_Size[z-1]
                    diario['daily_pl'] = daily_pl
                    diario.to_excel(w,sheet_name=('daily_'+ str(contador)))      

            counter += 1
            print(f'realizado {counter} / {len(process_list)}')

        if save_to != None:
            output_metrics.to_excel(w,sheet_name='Métricas')
            if True in settings['short_selling']:
                output_metrics_long.to_excel(w,sheet_name='Métricas Long')
                output_metrics_short.to_excel(w,sheet_name='Métricas Short')
            w.save()

        print(output_metrics)
    return output_metrics, results_dict


def add_monkey(data, monkey_test_entradas, monkey_test_salidas):
    import random

    data = data.reset_index()
    longs_in = data['trigger_long_in'].value_counts()
    shorts_in = data['trigger_short_in'].value_counts()
    longs_out = data['trigger_long_out'].value_counts()
    shorts_out = data['trigger_short_out'].value_counts()
    entradas_long = longs_in[1]
    entradas_short = shorts_in[1]
    salidas_long = longs_out[1]
    salidas_short = shorts_out[1]


    if monkey_test_entradas == True:

        for i in range(len(data)): #pasamos todos los 1 a 0
            if data.loc[i,'trigger_long_in'] == 1:
                data.loc[i,'trigger_long_in'] = 0
            if data.loc[i,'trigger_short_in'] == 1:
                data.loc[i,'trigger_short_in'] = 0

        list_entries_long = random.sample(range(len(data)),entradas_long)  #genera una lista de row_n de longitud entradas_long
        list_entries_short = random.sample(range(len(data)),entradas_short)
        
        for i in range(len(data)):
            if i in list_entries_long:
                data.loc[i,'trigger_long_in'] = 1
            if i in list_entries_short:
                data.loc[i,'trigger_short_in'] = 1
    

    if monkey_test_salidas == True:
        
        for i in range(len(data)): #pasamos todos los 1 a 0
            if data.loc[i,'trigger_long_out'] == 1:
                data.loc[i,'trigger_long_out'] = 0
            if data.loc[i,'trigger_short_out'] == 1:
                data.loc[i,'trigger_short_out'] = 0

        
        list_exits_long = random.sample(range(len(data)),salidas_long)
        list_exits_short = random.sample(range(len(data)),salidas_short)
        
        for i in range(len(data)):
            if i in list_exits_long:
                data.loc[i,'trigger_long_out'] = 1
            if i in list_exits_short:
                data.loc[i,'trigger_short_out'] = 1

    data = data.set_index('timestamp')
    return data

def monkey_test(source, settings, account_sets, strategy, strategy_sets, monkey_sets, save_to):
    random_entradas = monkey_sets['random_entries']
    random_salidas = monkey_sets['random_exits']
    pasadas = monkey_sets['pasadas']

    print(random_entradas)
    print(random_salidas)
    print(pasadas)

    output_metrics = pd.DataFrame()
    output_metrics_long = pd.DataFrame()
    output_metrics_short = pd.DataFrame()
    results_dict = {}

    if save_to != None:
        output_metrics.to_excel(save_to +'.xlsx',sheet_name = 'Métricas')
        w = pd.ExcelWriter(save_to +'.xlsx')

    contador = 0

    keys1, values1 = zip(*settings.items())
    setting_combos = [dict(zip(keys1, v1)) for v1 in itertools.product(*values1)]

    keys2, values2 = zip(*strategy_sets.items())
    strat_combos = [dict(zip(keys2, v2)) for v2 in itertools.product(*values2)]   

    if (len(setting_combos) > 1) or (len(strat_combos) > 1):
        raise Exception('Demasiados parámetros BASE para monkey_test, sólo debe haber 1 valor por parámetro')

    for setting in setting_combos:
        for strategy_set in strat_combos:
            for pasada in range(pasadas+1):

                #timeframe = {'1m' :  Client.KLINE_INTERVAL_1MINUTE, '3m' : Client.KLINE_INTERVAL_3MINUTE,'5m' : Client.KLINE_INTERVAL_5MINUTE,'15m' : Client.KLINE_INTERVAL_15MINUTE,'30m' : Client.KLINE_INTERVAL_30MINUTE,'1h' : Client.KLINE_INTERVAL_1HOUR,'2h' : Client.KLINE_INTERVAL_2HOUR,'4h' : Client.KLINE_INTERVAL_4HOUR,'6h' : Client.KLINE_INTERVAL_6HOUR,'8h' : Client.KLINE_INTERVAL_8HOUR,'12h' : Client.KLINE_INTERVAL_12HOUR,'1d' : Client.KLINE_INTERVAL_1DAY}
                minutes = {'1min' : 1, '3min' : 3,'5min' : 5,'15min' : 15,'30min' : 30,'1H' : 60,'90min' : 90, '2H' : 120,'4H' : 240,'6H' : 360,'8H' : 480,'12H' : 720,'1D' : 1440}
                agg_dict = {'Open' : 'first','High' : 'max','Low' : 'min','Close' : 'last','Volume' : 'sum','Date' : 'first'}

                tickers_list_b = setting['tickers_list']
                
                if pasada == 0:
                    df_dict = {}
                    for token1 in tickers_list_b: #creamos el df_dict:
                        
                        candles = pd.read_sql(token1,source).set_index('timestamp')
                        if minutes[setting['interval']] > 60:
                            my_offset = '1h'
                        else: my_offset = None
                        data = candles.resample(setting['interval'],offset=my_offset).agg(agg_dict)
                        #data = pd.read_sql(token1,'sqlite:///prueba_df_dict.db').set_index('timestamp')

                        strategy_set['file'] = source
                        strategy_set['token'] = token1
                        strategy_set['interval'] = setting['interval']

                        data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short = strategy(data,strategy_set)

                        del strategy_set['file']
                        del strategy_set['token']
                        del strategy_set['interval']

                        data = data.dropna()

                        data = data[data.index >= setting['fecha_desde_bt']]
                        data = data[data.index <= setting['fecha_hasta_bt']]

                        df_dict[token1] = data 

                    #print(df_dict['ETH'])#.to_excel('prueba.xlsx')

                elif ((random_entradas==True) or (random_salidas==True)) and (pasada > 0): 
                    for tic in tickers_list_b:
                        if not df_dict[tic].empty:
                            df_dict[tic] = add_monkey(df_dict[tic], random_entradas, random_salidas)

                #df_dict['ETH'].to_excel('prueba.xlsx')

                account_size_bt = account_sets['init_account']
                init_account_bt = account_size_bt
                available = account_size_bt * account_sets['account_margin']

                delta = timedelta(minutes=minutes[setting['interval']])
                
                open_positions_long = {} #key=ticker : {stop_value, take_profit, open_price, close_price, open_date, position_size, *bars_in, *open_pl}
                open_positions_short = {}

                closed_positions, account_size_r, open_positions_long_r, open_positions_short_r, available_r, velas = motor_run(order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short, df_dict, open_positions_long, open_positions_short, account_size_bt, available, tickers_list_b, delta, setting['position_size_st'], setting['short_selling'], setting['time_stop'], setting['stop_selection'], setting['trailing'], setting['comision'], setting['stop_loss'], setting['take_profit'])


                if setting['short_selling'] == True:
                    closed_long = closed_positions[closed_positions.Direction == 'long'].copy()
                    closed_long.reset_index(inplace=True)
                    closed_short = closed_positions[closed_positions.Direction == 'short'].copy()
                    closed_short.reset_index(inplace=True)

                closed_positions = add_results(closed_positions, setting, init_account_bt, delta)

                if setting['short_selling'] == True:
                    add_results(closed_long, setting, init_account_bt, delta)
                    add_results(closed_short, setting, init_account_bt, delta)

                #acá incorporaba nombres para las líneas de monkey
                if (random_entradas == True) and (random_salidas == False) and (pasada > 0):
                    name = 'monkey_entry_'+str(pasada)
                elif (random_entradas == False) and (random_salidas == True) and (pasada > 0):
                    name = 'monkey_exit_'+str(pasada)
                elif (random_entradas == True) and (random_salidas == True) and (pasada > 0):
                    name = 'monkey_'+str(pasada)
                elif (len(tickers_list_b) == 1):
                    name = str(tickers_list_b)+'_base'
                else:
                    name = 'comb_base'

                metrics = get_new_metrics(closed_positions, name, setting, strategy_set, df_dict, tickers_list_b, velas, contador)
                output_metrics = pd.concat([output_metrics,metrics],axis=0)
                
                results_dict[name] = [setting, strategy_set]

                if save_to != None:
                    output_metrics.to_excel(w,sheet_name = 'Métricas')

                    if setting['short_selling'] == True:
                        metrics_long = get_new_metrics(closed_long,name, setting, strategy_set, df_dict, tickers_list_b, velas, contador)
                        metrics_short = get_new_metrics(closed_short,name, setting, strategy_set, df_dict, tickers_list_b, velas, contador)
                        output_metrics_long = pd.concat([output_metrics_long,metrics_long])
                        output_metrics_short = pd.concat([output_metrics_short,metrics_short])
                        output_metrics_long.to_excel(w,sheet_name = 'Métricas Long')
                        output_metrics_short.to_excel(w,sheet_name = 'Métricas Short')

                    sheet = name
                    closed_positions.to_excel(w,sheet_name=sheet)

                    fig, axs = plt.subplots(2, figsize=(15,5))
                    axs[0].plot(closed_positions['Close_Date'], closed_positions['Acum_pl'],color = 'grey', linewidth = 3)
                    #axs[0].set_yscale('log')
                    axs[0].title.set_text('Resultado acumulado')
                    axs[1].plot(closed_positions['Close_Date'], closed_positions['Drawdown_%'],color = 'red',linestyle = 'dashed')
                    axs[1].title.set_text('Drawdown%')
                    axs[0].grid(which = 'major', axis = 'y', color  = 'black', alpha = 0.4)
                    axs[0].grid(which = 'minor', axis = 'both', color  = 'black', alpha = 0.15)
                    plt.fill_between(closed_positions['Close_Date'], closed_positions['Drawdown_%'], alpha=0.5, color='red')
                    fig.subplots_adjust(hspace = 0.3)
                    plt.savefig('graf'+sheet+'.png')
                    worksheet = w.sheets[sheet]
                    worksheet.insert_image('N2','graf'+sheet+'.png')

                    velas = velas[['Positions','Closed_Size','Open_pl','Curr_Size']].copy()
                    #velas.to_excel(w,sheet_name=('velas_'+ name))
                    if setting['show_daily_results'] == True:
                        diario = velas.resample('1d').agg('last').copy()
                        diario.reset_index(inplace=True)
                        daily_pl = [0]*len(diario)
                        for z in range(len(diario)):
                            if z == 0:
                                daily_pl[z] = (diario.Curr_Size[z] - init_account_bt) / init_account_bt
                            else: daily_pl[z] = (diario.Curr_Size[z] - diario.Curr_Size[z-1]) / diario.Curr_Size[z-1]
                        diario['daily_pl'] = daily_pl
                        diario.to_excel(w,sheet_name=('daily_'+ str(contador)))      
                
                print(f'realizado {pasada} / {len(setting_combos)*len(strat_combos)*pasadas}')
                contador += 1

    if save_to != None:
        output_metrics.to_excel(w,sheet_name='Métricas')
        if settings['short_selling'] == True:
            output_metrics_long.to_excel(w,sheet_name='Métricas Long')
            output_metrics_short.to_excel(w,sheet_name='Métricas Short')
        w.save()

    print(output_metrics)
    return output_metrics, closed_positions, velas, results_dict


def step_forward(source, universe_filter, settings, account_sets, strategy, strategy_set):

    #timeframe = {'1m' :  Client.KLINE_INTERVAL_1MINUTE, '3m' : Client.KLINE_INTERVAL_3MINUTE,'5m' : Client.KLINE_INTERVAL_5MINUTE,'15m' : Client.KLINE_INTERVAL_15MINUTE,'30m' : Client.KLINE_INTERVAL_30MINUTE,'1h' : Client.KLINE_INTERVAL_1HOUR,'2h' : Client.KLINE_INTERVAL_2HOUR,'4h' : Client.KLINE_INTERVAL_4HOUR,'6h' : Client.KLINE_INTERVAL_6HOUR,'8h' : Client.KLINE_INTERVAL_8HOUR,'12h' : Client.KLINE_INTERVAL_12HOUR,'1d' : Client.KLINE_INTERVAL_1DAY}
    minutes = {'1min' : 1, '3min' : 3,'5min' : 5,'15min' : 15,'30min' : 30,'1H' : 60,'90min' : 90, '2H' : 120,'4H' : 240,'6H' : 360,'8H' : 480,'12H' : 720,'1D' : 1440}
    agg_dict = {'Open' : 'first','High' : 'max','Low' : 'min','Close' : 'last','Volume' : 'sum','Date' : 'first'}

    tickers_list_b = settings['tickers_list']
    
    if universe_filter == None:
        universe_size = len(tickers_list_b)
    else:
        universe_size = settings['universe_size']
    # cargamos el dataframe dict:
    with open(source,'rb') as p:
        df_dict = pickle.load(p)

    # modificamos el df_dict:
    for token1 in tickers_list_b: 
        
        data = df_dict[token1]
        # candles = pd.read_sql(token1,source).set_index('timestamp')
        # if minutes[settings['interval']] > 60:
        #     my_offset = '1h'
        # else: my_offset = None
        # data = candles.resample(settings['interval'],offset=my_offset).agg(agg_dict)

        #data = pd.read_sql(token1,'sqlite:///prueba_df_dict.db').set_index('timestamp')

        strategy_set['file'] = source
        strategy_set['token'] = token1
        strategy_set['interval'] = settings['interval']

        data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short = strategy(data,strategy_set)

        del strategy_set['file']
        del strategy_set['token']
        del strategy_set['interval']

        data = data.dropna()

        data = data[data.index >= settings['fecha_desde_bt']]
        data = data[data.index <= settings['fecha_hasta_bt']]

        df_dict[token1] = data 

    #print(df_dict['ETH'])#.to_excel('prueba.xlsx')

    
    account_size_bt = account_sets['current_size']
    available = account_sets['available']
    

    delta = timedelta(minutes=minutes[settings['interval']])

    open_positions_long = account_sets['open_positions_long'] #key=ticker : {stop_value, take_profit, open_price, close_price, open_date, position_size, *bars_in, *open_pl}
    open_positions_short = account_sets['open_positions_short']

    closed_positions, account_size, open_positions_long, open_positions_short, available, velas = motor_run(universe_filter, universe_size, when_open, when_close, price_close_long, price_close_short, price_open_long, price_open_short, order_out, df_dict, open_positions_long, open_positions_short, account_size_bt, available, tickers_list_b, delta, settings['position_size_st'], settings['short_selling'], settings['time_stop'], settings['stop_selection'], settings['trailing'], settings['comision'], settings['stop_loss'], settings['take_profit'])

    result_acc_sets = {
        'init_account': account_sets['init_account'],
        'account_margin': account_sets['account_margin'],
        'open_positions_long': open_positions_long,
        'open_positions_short': open_positions_short,
        'current_size': account_size,
        'available': available
    }
          
    return closed_positions, velas, result_acc_sets

def mp_walk_forward(source, universe_filter, settings, account_sets, strategy, strategy_sets, steps, save_to):
    output_metrics = pd.DataFrame()
    output_metrics_long = pd.DataFrame()
    output_metrics_short = pd.DataFrame()
    if save_to != None:
        output_metrics.to_excel(save_to +'.xlsx',sheet_name = 'Métricas')
        w = pd.ExcelWriter(save_to +'.xlsx')
    minutes = {'1min' : 1, '3min' : 3,'5min' : 5,'15min' : 15,'30min' : 30,'1H' : 60,'90min' : 90, '2H' : 120,'4H' : 240,'6H' : 360,'8H' : 480,'12H' : 720,'1D' : 1440}
    delta = timedelta(minutes=minutes[settings['interval'][0]])

    #guardamos valor original de desde y hasta para usar más tarde:
    desde_original = settings['fecha_desde_bt'][0]
    hasta_original = settings['fecha_hasta_bt'][0]

    #pasar las fechas desde y hasta a datetime para poder sumar y restar:
    fecha_desde = datetime.strptime(settings['fecha_desde_bt'][0],'%Y-%m-%d') 
    fecha_hasta = datetime.strptime(settings['fecha_hasta_bt'][0],'%Y-%m-%d') 

    for step in steps:
        step_back = timedelta(days=step[0])
        step_for = timedelta(days=step[1])

        closed_positions = pd.DataFrame()
        velas = pd.DataFrame()
        init_account = account_sets['init_account']
        inicio_bt = fecha_desde - step_back
        fin_bt = inicio_bt + step_back
        acc_sets_fw = account_sets
        acc_sets_fw['current_size'] = account_sets['init_account']
        acc_sets_fw['available'] = (account_sets['init_account'] * account_sets['account_margin'])
        

        while fin_bt < fecha_hasta:
            print()
            print(f'backtesting from {inicio_bt} to {fin_bt}')
            settings['fecha_desde_bt'][0] = inicio_bt
            settings['fecha_hasta_bt'][0] = fin_bt

            metricas_bt, results_dict_bt = mp_backtest(source, universe_filter, settings, account_sets, strategy, strategy_sets, save_to=None)
            
            best_row = ''
            best = -1000
            for x, row in metricas_bt.iterrows():
                #if results_bt.loc[x,'profit_%'] >= 0.10:
                    if metricas_bt.loc[x,'Calmar_ratio'] > best:
                        best = metricas_bt.loc[x,'Calmar_ratio']
                        best_row = x
                    else: continue
            settings_fw = results_dict_bt[best_row][0]
            strat_sets_fw = results_dict_bt[best_row][1]
            
            print(f'best is {best_row}')
            for key, value in settings_fw.items():
                print(f'{key} : {str(value)}')
            for key, value in strat_sets_fw.items():
                print(f'{key} : {str(value)}')

            inicio_fw = fin_bt
            if (inicio_fw + step_for) < fecha_hasta:
                fin_fw = inicio_fw + step_for
            else: fin_fw = fecha_hasta

            settings_fw['fecha_desde_bt'] = inicio_fw
            settings_fw['fecha_hasta_bt'] = fin_fw

            print()
            print(f'forward from {inicio_fw} to {fin_fw}')
            fw_closed_positions, fw_velas, fw_acc_sets = step_forward(source, universe_filter, settings_fw, acc_sets_fw, strategy, strat_sets_fw)
            closed_positions = pd.concat([closed_positions, fw_closed_positions],axis=0)
            velas = pd.concat([velas, fw_velas],axis=0)
            acc_sets_fw = fw_acc_sets

            inicio_bt += step_for
            fin_bt += step_for

        #acá termina un juego de WF
        sheet = f'step_{step[0]}_{step[1]}'
        closed_positions.reset_index(inplace=True)

        if True in settings['short_selling']:
            closed_long = closed_positions[closed_positions.Direction == 'long'].copy()
            closed_long.reset_index(inplace=True)
            closed_short = closed_positions[closed_positions.Direction == 'short'].copy()
            closed_short.reset_index(inplace=True)

        add_results(closed_positions, settings, account_sets['init_account'], delta)

        if True in settings['short_selling']:
            add_results(closed_long, settings, account_sets['init_account'], delta)
            add_results(closed_short, settings, account_sets['init_account'], delta)

        metrics = get_new_metrics_beta(closed_positions, sheet, settings, strategy_sets, velas, step)
        metrics['fecha_desde_bt'] = [desde_original]*len(metrics)
        metrics['fecha_hasta_bt'] = [hasta_original]*len(metrics)
        output_metrics = pd.concat([output_metrics,metrics])

        if True in settings['short_selling']:
            metrics_long = get_new_metrics_beta(closed_long, sheet, settings, strategy_sets, velas, step)
            metrics_short = get_new_metrics_beta(closed_short, sheet, settings, strategy_sets, velas, step)
            output_metrics_long = pd.concat([output_metrics_long,metrics_long])
            output_metrics_short = pd.concat([output_metrics_short,metrics_short])

        print(closed_positions)
        print()
        print(metrics)

        if save_to != None:

            closed_positions.to_excel(w,sheet_name=sheet)

            fig, axs = plt.subplots(2, figsize=(15,5))
            axs[0].plot(closed_positions['Close_Date'], closed_positions['Acum_pl'],color = 'grey', linewidth = 3)
            #axs[0].set_yscale('log')
            axs[0].title.set_text('Resultado acumulado')
            axs[1].plot(closed_positions['Close_Date'], closed_positions['Drawdown_%'],color = 'red',linestyle = 'dashed')
            axs[1].title.set_text('Drawdown%')
            axs[0].grid(which = 'major', axis = 'y', color  = 'black', alpha = 0.4)
            axs[0].grid(which = 'minor', axis = 'both', color  = 'black', alpha = 0.15)
            plt.fill_between(closed_positions['Close_Date'], closed_positions['Drawdown_%'], alpha=0.5, color='red')
            fig.subplots_adjust(hspace = 0.3)
            plt.savefig('graf'+sheet+'.png')
            worksheet = w.sheets[sheet]
            worksheet.insert_image('N2','graf'+sheet+'.png')

            velas = velas[['Positions','Closed_Size','Open_pl','Curr_Size']].copy()
            #velas.to_excel(w,sheet_name='velas_'+str(step))
            if True in settings['show_daily_results']:
                diario = velas.resample('1d').agg('last').copy()
                diario.reset_index(inplace=True)
                daily_pl = [0]*len(diario)
                for z in range(len(diario)):
                    if z == 0:
                        daily_pl[z] = (diario.Curr_Size[z] - init_account) / init_account
                    else: daily_pl[z] = (diario.Curr_Size[z] - diario.Curr_Size[z-1]) / diario.Curr_Size[z-1]
                diario['daily_pl'] = daily_pl
                diario.to_excel(w,sheet_name='daily_results_'+str(step))

    #hasta acá llega un step
    if save_to != None:
        output_metrics.to_excel(w,sheet_name='Métricas')
        if True in settings['short_selling']:
            output_metrics_long.to_excel(w,sheet_name='Métricas Long')
            output_metrics_short.to_excel(w,sheet_name='Métricas Short')
        w.save()

    return output_metrics, closed_positions
