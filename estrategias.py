from logging import raiseExceptions
from statistics import mean
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import pandas_ta as ta
import time
import matplotlib
import pickle

'''
# para reutilizar el 'motor' que está escrito las condiciones son:
# 'trigger_long_in' == 1 abre long
# 'trigger_long_out' == 1 cierra long (por fuera de stops y take_profits)
# 'trigger_short_in' == 1 abre short
# 'trigger_short_out' == 1 cierra short (por fuera de sl y tp)
'''
'''
# en esta versión hay que definir:
# when_open = 'this_bar' | 'next_bar'
# when_close = 'this_bar' | 'next_bar'
# price_open_long, price_open_short, price_close_long, price_close_short = 'column'
# order_out(list) = ['stop_loss', 'take_profit', 'trigger_out']
'''

#momentum : long en close > close[x] and close > close[y], cierra apenas sea menor a cualquiera de ambos:

def add_estrategia_momentum(data, settings):
    fast_lag = settings['fast_lag']
    slow_lag = settings['slow_lag']

    def add_mms(data,fast_lag,slow_lag):

        data['fast'] = data['Close'].shift(fast_lag)
        data['slow'] = data['Close'].shift(slow_lag)

    def add_trigger(data):
        # max_ = [0]*len(data)
        # min_ = [0]*len(data)
        # for i in range(len(data)):
        #     max_[i] = max(data.fast[i],data.slow[i])
        #     min_[i] = min(data.fast[i],data.slow[i])
        data['max'] = np.maximum(data['fast'],data['slow']) #max_
        data['min'] = np.minimum(data['fast'],data['slow']) #min_
        data['trigger_long_in'] = data['Close'].gt(data['max']).mul(1).diff()
        data['trigger_long_out'] = data['Close'].lt(data['max']).mul(1).diff()
        data['trigger_short_in'] = data['Close'].lt(data['min']).mul(1).diff()
        data['trigger_short_out'] = data['Close'].gt(data['min']).mul(1).diff()
    
    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    add_mms(data, fast_lag, slow_lag)
    add_trigger(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


# esta es del gordo, compra cruces de medias solo si Close < Low[1]:

def add_estrategia_cruce_con_pullback(data, settings):
    mm_fast = settings['mm_fast']
    mm_slow = settings['mm_slow']

    def add_medias(data, mm_fast, mm_slow):
        data['fast'] = data['Close'].rolling(mm_fast).mean()
        data['slow'] = data['Close'].rolling(mm_slow).mean()
        data['cross'] = data['fast'].gt(data['slow']).mul(1).diff().fillna(0)
    
    def add_trigger(data):
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        for i in range(1,len(data)):
            if data.cross[i] == 1 and data.Close[i] < data.Low[i-1]:
                long_in[i] = 1
                short_out[i] = 1
            elif data.cross[i] == -1 and data.Close[i] > data.High[i-1]:
                long_out[i] = 1
                short_in[i] = 1
        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out
    
    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    add_medias(data, mm_fast, mm_slow)
    add_trigger(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


# Entry1 compra el breackout de daysback sólo si el rango de la vela es el mayor rango de xvelas; con filtro de EMA:

def add_estrategia_Entry1(data, settings):
    xvelas = settings['xvelas']
    daysback = settings['daysback']
    EMA = settings['EMA']

    def add_indicators(data, xvelas, daysback, EMA):
        data['rrange'] = data['High'] - data['Low']
        data['ref_rrange'] = data['rrange'].rolling(xvelas).mean()+ 2*data['rrange'].rolling(xvelas).std()
        data['max_close'] = data['Close'].rolling(daysback).max()
        data['min_close'] = data['Close'].rolling(daysback).min()
        data['EMA'] = data['Close'].ewm(span = EMA, adjust = False).mean()

    def add_triggers(data):
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        for i in range(len(data)):
            if (data.rrange[i] > data.ref_rrange[i]) and (data.Close[i] >= data.max_close[i]) and (data.Close[i] > data.EMA[i]):
                long_in[i] = 1
                short_out[i] = 1
            elif (data.rrange[i] > data.ref_rrange[i]) and (data.Close[i] <= data.min_close[i]) and (data.Close[i] < data.EMA[i]):
                long_out[i] = 1
                short_in[i] = 1
        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    add_indicators(data, xvelas, daysback, EMA)
    add_triggers(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


def add_estrategia_3barhl(data, settings):
    mm = settings['mm']
    fast = settings['fast']
    slow = settings['slow']

    def add_indicators(data, mm, fast, slow):
        data['mm_high'] = data['High'].rolling(mm).mean()
        data['mm_low'] = data['Low'].rolling(mm).mean()
        data['mm_high_alt'] = data['High'].rolling(mm-1).mean().shift(1)
        data['mm_low_alt'] = data['Low'].rolling(mm-1).mean().shift(1)
        data['mm_slow'] = data['Close'].rolling(slow).mean()
        data['mm_fast'] = data['Close'].rolling(fast).mean()
        data['trend'] = data['mm_fast'].gt(data['mm_slow']).mul(1)
    
    def add_triggers(data):
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        for i in range(len(data)):
            if data.trend[i] == 1 and data.Close[i] <= data.mm_low[i]:
                long_in[i] = 1
            if data.High[i] > data.mm_high_alt[i]:
                long_out[i] = 1
            if data.trend[i] == 0 and data.Close[i] >= data.mm_high[i]:
                short_in[i] = 1
            if data.Low[i] < data.mm_low_alt[i]:
                short_out[i] = 1
        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'this_bar'
    price_close_long = 'mm_high_alt' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'mm_low_alt'
    order_out = ['stop_loss', 'trigger_out', 'take_profit']

    add_indicators(data, mm, fast, slow)
    add_triggers(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


def add_estrategia_3barhl_con_tdfi(data, settings):
    mm = settings['mm']
    fast = settings['fast']
    slow = settings['slow']
    tdfi_p = settings['tdfi_p']
    tdfi_s = settings['tdfi_s']

    def add_indicators(data, mm, fast, slow):
        data['mm_high'] = data['High'].rolling(mm).mean()
        data['mm_low'] = data['Low'].rolling(mm).mean()
        data['mm_high_alt'] = data['High'].rolling(mm-1).mean().shift(1)
        data['mm_low_alt'] = data['Low'].rolling(mm-1).mean().shift(1)
        data['mm_slow'] = data['Close'].rolling(slow).mean()
        data['mm_fast'] = data['Close'].rolling(fast).mean()
        data['trend'] = data['mm_fast'].gt(data['mm_slow']).mul(1)
    
    def add_tdfi_indicator(data, tdfi_p, tdfi_s):
        limit_up = 0.05
        limit_down = -0.05
        mma = ta.ema(data.Close, tdfi_p)
        smma = ta.ema(mma, tdfi_p)
        impetmma = ta.mom(mma,1)
        impetsmma = ta.mom(smma,1)
        divma = abs(mma - smma) / 0.01
        averimpet = ((impetmma+impetsmma)/2)/ (2*0.01)
        tdfRaw = (divma * (averimpet**3)).fillna(0)
        tdfAbsRaw = abs(tdfRaw)
        
        cand = tdfAbsRaw.rolling(3*tdfi_p-1).max()
        ratio = (tdfRaw / cand).fillna(0)

        #smoothing:
        alpha = 0.45*(tdfi_s-1) / (0.45*(tdfi_s-1)+2)
        e0 = [0]*len(data)
        e1 = [0]*len(data)
        e2 = [0]*len(data)
        smooth = [0]*len(data)
        tdf = [0]*len(data)
        tdf_trend = ['']*len(data)
        
        for x in range(1,len(data)):
            e0[x] = (1-alpha)*ratio[x] + alpha*e0[x-1]
            e1[x] = (1-alpha)*(ratio[x]-e0[x])+alpha*e1[x-1]
            e2[x] = ((1-alpha)**2)*(e0[x]+e1[x]-smooth[x-1])+((alpha**2)*e2[x-1])
            smooth[x] = e2[x] + smooth[x-1]

            tdf[x] = max(min(smooth[x],1),-1)
            if tdf[x] > limit_up:
                tdf_trend[x] = 'LONG'
            elif tdf[x] < limit_down:
                tdf_trend[x] = 'SHORT'

        data['tdfi'] = tdf
        data['tdfi_trend'] = tdf_trend
        
    def add_triggers(data):
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        for i in range(len(data)):
            if data.tdfi_trend[i] == 'LONG' and data.trend[i] == 1 and data.Close[i] <= data.mm_low[i]:
                long_in[i] = 1
            if data.High[i] > data.mm_high_alt[i]:
                long_out[i] = 1
            if data.tdfi_trend[i] == 'SHORT' and data.trend[i] == 0 and data.Close[i] >= data.mm_high[i]:
                short_in[i] = 1
            if data.Low[i] < data.mm_low_alt[i]:
                short_out[i] = 1
        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'this_bar'
    price_close_long = 'mm_high_alt' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'mm_low_alt'
    order_out = ['stop_loss', 'trigger_out', 'take_profit']

    add_indicators(data, mm, fast, slow)
    add_tdfi_indicator(data, tdfi_p, tdfi_s)
    add_triggers(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


def add_estrategia_3barhl_con_higher_filter(data, settings):
    minutes = {'1min' : 1, '3min' : 3,'5min' : 5,'15min' : 15,'30min' : 30,'1H' : 60,'90min' : 90, '2H' : 120,'4H' : 240,'6H' : 360,'8H' : 480,'12H' : 720,'1D' : 1440}
    agg_dict = {'Open' : 'first','High' : 'max','Low' : 'min','Close' : 'last','Volume' : 'sum','Date' : 'first'}
    file = settings['file']
    token1= settings['token']
    interval= settings['interval']
    mm = settings['mm']
    fast = settings['fast']
    slow = settings['slow']
    interval_2 = settings['interval_2']
    ADX_p = settings['ADX_period']
    ADX_limit = settings['ADX_limit']
    EMA_p = settings['EMA_period']

    #RaiseExeption si minutes[interval_2] <= minutes[interval]
    if minutes[interval_2] <= minutes[interval]:
        raise Exception('interval_2 debe ser mayor a interval_1')

    def add_indicators(data, mm, fast, slow):
        data['mm_high'] = data['High'].rolling(mm).mean()
        data['mm_low'] = data['Low'].rolling(mm).mean()
        data['mm_high_alt'] = data['High'].rolling(mm-1).mean().shift(1)
        data['mm_low_alt'] = data['Low'].rolling(mm-1).mean().shift(1)
        data['mm_slow'] = data['Close'].rolling(slow).mean()
        data['mm_fast'] = data['Close'].rolling(fast).mean()
        data['trend'] = data['mm_fast'].gt(data['mm_slow']).mul(1)
    
    def add_higher_filter(data, token1, interval_2, ADX_p, EMA_p):
        #crear el segundo dataframe:
        candles_b = pd.read_sql(token1, file).set_index('timestamp')
        if minutes[interval_2] > 60:
            my_offset = '1h'
        else: my_offset = None
        data_4 = candles_b.resample(interval_2,offset=my_offset).agg(agg_dict)
        
        #este sería el lugar donde agregar indicadores al dataframe:
        data_4 = pd.concat([data_4, data_4.ta.adx(length=ADX_p)], axis=1)
        #data_4 = data_4.drop(['DMP_'+str(ADX_p),'DMN_'+str(ADX_p)], axis=1)
        data_4['EMA'] = ta.ema(data_4.Close, EMA_p)

        #desfasarlo un período:
        data_4 = data_4.reset_index()
        data_4['timestamp'] = data_4['timestamp'] + pd.DateOffset(minutes = minutes[interval_2])
        data_4 = data_4.set_index(['timestamp'])
        #merge y ffill todas las filas:
        data_4.columns = [c + '_htf' for c in data_4.columns]
        multi = data.merge(data_4,on=['timestamp'],how='left')
        multi[data_4.columns] = multi[data_4.columns].transform(lambda x: x.ffill())
        multi = multi.dropna()
        return multi
    
    ADX = 'ADX_'+str(ADX_p)+'_htf'

    def add_triggers(data):
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        for i in range(len(data)):
            if (data.Close[i] > data.EMA_htf[i]) and (data[ADX][i] > ADX_limit) and (data.trend[i] == 1) and (data.Close[i] <= data.mm_low[i]):
                long_in[i] = 1
            if data.High[i] > data.mm_high_alt[i]:
                long_out[i] = 1
            if (data.Close[i] < data.EMA_htf[i]) and (data[ADX][i] > ADX_limit) and (data.trend[i] == 0) and (data.Close[i] >= data.mm_high[i]):
                short_in[i] = 1
            if data.Low[i] < data.mm_low_alt[i]:
                short_out[i] = 1
        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out
        #si error 'the truth value of a series is ambiguous' revisar que falta poner [i]

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'this_bar'
    price_close_long = 'mm_high_alt' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'mm_low_alt'
    order_out = ['stop_loss', 'trigger_out', 'take_profit']

    add_indicators(data, mm, fast, slow)
    data = add_higher_filter(data, token1, interval_2, ADX_p, EMA_p)
    add_triggers(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


# si el stochrsi cruza 2 veces hacia abajo o 2 veces hacia arriba sin cruzar valor 50 toma señal

def add_estrategia_2_picos(data, settings): #probar supertrend como filtro y alguna señal de salida como RSI o algo
    s_length = settings['super_length'] #10
    s_mult = settings['super_mult'] #3
    stoch_l = settings['stoch_l'] #14
    rsi = settings['rsi'] #16
    k = settings['k'] #3
    d = settings['d'] #3

    def add_stochrsi(data, stoch, rsi, k, d):
        data = pd.concat([data, ta.stochrsi(data.Close,stoch,rsi,k,d)],axis=1)
        data = data.rename(columns={f'STOCHRSIk_{stoch}_{rsi}_{k}_{d}':'stochrsi_k',f'STOCHRSId_{stoch}_{rsi}_{k}_{d}':'stochrsi_d'})
        data['cross_kd'] = data['stochrsi_k'].gt(data['stochrsi_d']).mul(1).diff()
        data['cross_k50'] = data['stochrsi_k'].gt(50).mul(1).diff()

        double_up = [0]*len(data)
        double_down = [0]*len(data)
        count_up = 0
        count_down = 0

        for i in range(len(data)):

            if data.cross_k50[i] == 1:
                count_down = 0
            elif data.cross_k50[i] == -1:
                count_up = 0

            if data.cross_kd[i] == 1:
                count_up += 1
            elif data.cross_kd[i] == -1:
                count_down += 1

            if count_up >= 2:
                double_up[i] = 1
                count_up = 0
            elif count_down >= 2:
                double_down[i] = 1
                count_down = 0

        data['double_up'] = double_up
        data['double_down'] = double_down

        return data

    def add_supertrend(df, atr_period, multiplier):
    
        high = df['High']
        low = df['Low']
        close = df['Close']
        
        # calculate ATR
        price_diffs = [high - low, 
                    high - close.shift(), 
                    close.shift() - low]
        true_range = pd.concat(price_diffs, axis=1)
        true_range = true_range.abs().max(axis=1)
        # default ATR calculation in supertrend indicator
        atr = true_range.ewm(alpha=1/atr_period,min_periods=atr_period).mean() 
        # df['atr'] = df['tr'].rolling(atr_period).mean()
        
        # HL2 is simply the average of high and low prices
        hl2 = (high + low) / 2
        # upperband and lowerband calculation
        # notice that final bands are set to be equal to the respective bands
        final_upperband = upperband = hl2 + (multiplier * atr)
        final_lowerband = lowerband = hl2 - (multiplier * atr)
        
        # initialize Supertrend column to True
        supertrend = [1] * len(df)
        
        for i in range(1, len(df.index)):
            curr, prev = i, i-1
            
            # if current close price crosses above upperband
            if close[curr] > final_upperband[prev]:
                supertrend[curr] = 1
            # if current close price crosses below lowerband
            elif close[curr] < final_lowerband[prev]:
                supertrend[curr] = 0
            # else, the trend continues
            else:
                supertrend[curr] = supertrend[prev]
                
                # adjustment to the final bands
                if supertrend[curr] == 1 and final_lowerband[curr] < final_lowerband[prev]:
                    final_lowerband[curr] = final_lowerband[prev]
                if supertrend[curr] == 0 and final_upperband[curr] > final_upperband[prev]:
                    final_upperband[curr] = final_upperband[prev]

            # to remove bands according to the trend direction
            if supertrend[curr] == 1:
                final_upperband[curr] = np.nan
            else:
                final_lowerband[curr] = np.nan
        
        supertrend = pd.DataFrame({
            'Supertrend': supertrend,
            'Final Lowerband': final_lowerband,
            'Final Upperband': final_upperband
        }, index=df.index)

        supertrend = supertrend.fillna(0)
        df = pd.concat([df,supertrend],axis=1)
        df['trend_chg'] = df['Supertrend'].diff()

        return df
    
    
    def add_triggers(data):
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        for i in range(len(data)):
            if data.Supertrend[i] == 1 and data.double_up[i] == 1:
                long_in[i] = 1          
            if data.Supertrend[i] == 0 and data.double_down[i] == 1:
                short_in[i] = 1
            if data['trend_chg'][i] == -1:
                long_out[i] = 1
            if data['trend_chg'][i] == 1:
                short_out[i] = 1
        #mask = (data['Supertrend']==1)&(data['double_up']==1)
        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out

        return data

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    data = add_stochrsi(data, stoch_l, rsi, k, d)
    data = add_supertrend(data, s_length, s_mult)
    data = add_triggers(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

# bollingers con momentum

def add_estrategia_bollinger(data, settings):

    bb_period = settings['bb_period']
    std_dev = settings['std_dev']
    momentum = settings['momentum']
    
    def add_bollingers(data, bb_period, std_dev):
        data = pd.concat([data, data.ta.bbands(length=bb_period, std=std_dev)], axis=1)
        data['BB_UP'] = data[f'BBU_{bb_period}_{std_dev}'].astype(float)
        data['BB_LO'] = data[f'BBL_{bb_period}_{std_dev}'].astype(float)
        data = data.drop([f'BBM_{bb_period}_{std_dev}', f'BBB_{bb_period}_{std_dev}', f'BBP_{bb_period}_{std_dev}',f'BBU_{bb_period}_{std_dev}',f'BBL_{bb_period}_{std_dev}'], axis=1)
        return data

    def add_trend(data, momentum):
        data[f'close_{momentum}_ago'] = data['Close'].shift(momentum)
        data['trend'] = data['Close'].gt(data[f'close_{momentum}_ago']).mul(1)
        return data

    def add_trigger(data):
       
        data['cross_up'] = data['Close'].gt(data['BB_LO']).mul(1).diff() #me interesa == 1
        data['cross_down'] = data['Close'].gt(data['BB_UP']).mul(1).diff() #me interesa == -1

        data['trigger_long_in'] = [0]*len(data)
        data['trigger_long_out'] = [0]*len(data)
        data['trigger_short_in'] = [0]*len(data)
        data['trigger_short_out'] = [0]*len(data)
 
        long_in = ((data['cross_up']==1)&(data['trend']==1))
        short_in = ((data['cross_down']==-1)&(data['trend']==0))
        
        data.loc[long_in,'trigger_long_in'] = 1
        data.loc[short_in,'trigger_long_out'] = 1
        data.loc[short_in,'trigger_short_in'] = 1
        data.loc[long_in,'trigger_short_out'] = 1
        
        return data

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    data = add_bollingers(data, bb_period, std_dev)    
    data = add_trend(data, momentum)
    data = add_trigger(data)
    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

# time defined range breakouts:

def add_estrategia_time_breakout(data, settings):
    
    open_range = settings['time_range'][0] # hora a la que inicia el conteo de high y low
    close_range = settings['time_range'][1] # hora a la que finaliza el conteo de high y low, a partir de esta hora opera el break
    close_trades = settings['time_range'][2] # a esta hora cualquier trade abierto se cierra
    buffer = settings['buffer'] # ej: 0.025 = 2.5% // distancia a la cual poner la stop_entry_order

    def add_indicators(data, open_range, close_range, close_trades):
        data['str_time'] = [datetime.strftime(i,'%Y-%m-%d %H:%M:%S')[11:16] for i in data['Date']] # [11:16] es text slicing
        range_high = [0]*len(data)
        range_low = [0]*len(data)
        open_high = [0]*len(data)
        open_low = [0]*len(data)
        operar = [0]*len(data)
        filtro = 0
        for i in range(len(data)):
            if data['str_time'][i] == open_range:
                filtro = 1
            elif data['str_time'][i] == close_range:
                filtro = 0
            elif data['str_time'][i] == close_trades:
                filtro = 2  
            if filtro == 1:
                if data['str_time'][i] == open_range:
                    range_high[i] = data.High[i]
                    open_high[i] = range_high[i]*(1+buffer)
                    range_low[i] = data.Low[i]
                    open_low[i] = range_low[i]*(1-buffer)
                else:
                    range_high[i] = max(data.High[i],range_high[i-1])
                    open_high[i] = range_high[i]*(1+buffer)
                    range_low[i] = min(data.Low[i],range_low[i-1])
                    open_low[i] = range_low[i]*(1-buffer)
            elif filtro == 0 and i != 0:
                open_high[i] = open_high[i-1]
                open_low[i] = open_low[i-1]
            if filtro == 0:
                operar[i] = 1

        data['operar'] = operar
        data['range_high'] = range_high
        data['range_low'] = range_low
        data['open_high'] = open_high
        data['open_low'] = open_low

        return data

    def add_triggers(data,close_trades):
        data['cross_long'] = data['High'].gt(data['open_high']).mul(1).diff()
        data['cross_short'] = data['Low'].lt(data['open_low']).mul(1).diff()
        long_in = [0]*len(data)
        long_out = [0]*len(data)
        short_in = [0]*len(data)
        short_out = [0]*len(data)
        trades_today = 0
        
        for i in range(len(data)):
            if data['operar'][i] == 1 and trades_today == 0 and data['cross_long'][i] == 1:
                long_in[i] = 1
                trades_today += 1
            elif data['operar'][i] == 1 and trades_today == 0 and data['cross_short'][i] == 1:
                short_in[i] = 1
                trades_today += 1
            if (i < len(data)-1):
                if data['str_time'][i+1] == close_trades:
                    long_out[i] = 1
                    short_out[i] = 1
                    trades_today = 0
                

        data['trigger_long_in'] = long_in
        data['trigger_long_out'] = long_out
        data['trigger_short_in'] = short_in
        data['trigger_short_out'] = short_out

        return data

    when_open = 'this_bar'
    price_open_long = 'open_high'
    price_open_short = 'open_low'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    data = add_indicators(data, open_range, close_range, close_trades)
    data = add_triggers(data,close_trades)

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short   

# SSL | compra cambio de SSL con filtro de SMAs:

def add_estrategia_ssl(data, settings):

    data = data.copy()

    ssl_period = settings['ssl_period']
    sma_1 = settings['sma_1']
    sma_2 = settings['sma_2']
    sma_3 = settings['sma_3']
    sma_4 = settings['sma_4']
    sma_5 = settings['sma_5']
    sma_6 = settings['sma_6']

    def add_indicators(data, ssl_period, sma_1, sma_2, sma_3, sma_4, sma_5, sma_6):

        data['ssl_high'] = data['High'].rolling(ssl_period).mean()
        data['ssl_low'] = data['Low'].rolling(ssl_period).mean()

        ssl_state = [0]*len(data)
        for i in range(1,len(data)):
            if data.Close[i] > data.ssl_high[i]:
                ssl_state[i] = 1
            elif data.Close[i] < data.ssl_low[i]:
                ssl_state[i] = 0
            else:
                ssl_state[i] = ssl_state[i-1]
        
        data['ssl_trend'] = ssl_state
        data['ssl_signal'] = data['ssl_trend'].diff()


        data['SMA_1'] = data['Close'].rolling(sma_1).mean()
        data['SMA_2'] = data['Close'].rolling(sma_2).mean()
        data['SMA_3'] = data['Close'].rolling(sma_3).mean()
        data['SMA_4'] = data['Close'].rolling(sma_4).mean()
        data['SMA_5'] = data['Close'].rolling(sma_5).mean()
        data['SMA_6'] = data['Close'].rolling(sma_6).mean()
        
        data['sma_highest'] = data[['SMA_1','SMA_2','SMA_3','SMA_4','SMA_5','SMA_6',]].max(axis=1)
        data['sma_lowest'] = data[['SMA_1','SMA_2','SMA_3','SMA_4','SMA_5','SMA_6',]].min(axis=1)
        data['bull_filter'] = data['Close'].gt(data.sma_highest).mul(1)
        data['bear_filter'] = data['Close'].lt(data.sma_lowest).mul(1)

        return data
        

    def add_trigger(data):
        
        data['trigger_long_in'] = np.select([(data.bull_filter == 1)&(data.ssl_signal==1)],[1],0)
        data['trigger_long_out'] = np.select([(data.ssl_signal==-1)],[1],0)
        data['trigger_short_in'] = np.select([(data.bear_filter == 1)&(data.ssl_signal==-1)],[1],0)
        data['trigger_short_out'] = np.select([(data.ssl_signal==1)],[1],0)

        return data
    
    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    data = add_indicators(data, ssl_period, sma_1, sma_2, sma_3, sma_4, sma_5, sma_6)
    data = add_trigger(data)

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

def add_estrategia_ibs(data, settings):
    n_open_long = settings['n_open_long']
    n_close_long = settings['n_close_long']
    n_open_short = settings['n_open_short']
    n_close_short = settings['n_close_short']
    EMA_period = settings['EMA_period']

    data['IBS'] = (data.Close - data.Low) / (data.High - data.Low) # esto va a ser un float de 0 a 1
    data['EMA'] = ta.ema(data['Close'],EMA_period)

    data['trigger_long_in'] = np.select([(data.IBS <= n_open_long)&(data.Close >= data.EMA)],[1],0)
    data['trigger_long_out'] = np.select([(data.IBS >= n_close_long)],[1],0)
    data['trigger_short_in'] = np.select([(data.IBS <= n_open_short)&(data.Close <= data.EMA)],[1],0)
    data['trigger_short_out'] = np.select([(data.IBS >= n_close_short)],[1],0)

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

def add_estrategia_power_zone_rsi(data, settings):
    rsi_length = settings['rsi_length']
    pz_open_long = settings['pz_open_long']
    pz_close_long = settings['pz_close_long']
    pz_open_short = settings['pz_open_short']
    pz_close_short = settings['pz_close_short']
    EMA_period = settings['EMA_period']

    data['RSI'] = ta.rsi(data.Close,rsi_length)
    data['RSI_ol'] = data.RSI.lt(pz_open_long).mul(1).diff()
    data['RSI_cl'] = data.RSI.gt(pz_close_long).mul(1).diff()
    data['RSI_os'] = data.RSI.gt(pz_open_short).mul(1).diff()
    data['RSI_cs'] = data.RSI.lt(pz_close_short).mul(1).diff()
    data['EMA'] = ta.ema(data['Close'],EMA_period)

    data['trigger_long_in'] = np.select([(data.RSI_ol == 1)&(data.Close >= data.EMA)],[1],0)
    data['trigger_long_out'] = np.select([(data.RSI_cl == 1)],[1],0)
    data['trigger_short_in'] = np.select([(data.RSI_os == 1)&(data.Close <= data.EMA)],[1],0)
    data['trigger_short_out'] = np.select([(data.RSI_cs == 1)],[1],0)

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

def add_estrategia_connors_7(data, settings):
    n_in = settings['n_in']
    n_out = settings['n_out']
    EMA_period = settings['EMA_period']

    data['max_in'] = data.Close.rolling(n_in).max()
    data['min_in'] = data.Close.rolling(n_in).min()
    data['max_out'] = data.Close.rolling(n_out).max()
    data['min_out'] = data.Close.rolling(n_out).min()
    data['EMA'] = ta.ema(data['Close'],EMA_period)

    data['trigger_long_in'] = np.select([(data.Close == data.min_in)&(data.Close >= data.EMA)],[1],0)
    data['trigger_long_out'] = np.select([(data.Close == data.max_out)],[1],0)
    data['trigger_short_in'] = np.select([(data.Close == data.max_in)&(data.Close <= data.EMA)],[1],0)
    data['trigger_short_out'] = np.select([(data.Close == data.min_in)],[1],0)

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short


def add_estrategia_ema_play(data, settings):
    ema_length = settings['ema_length']
    EMA_filter = settings['EMA_filter']

    data['ema'] = ta.ema(data['Close'],ema_length)
    data['EMA_filter'] = ta.ema(data['Close'],EMA_filter)

    data['cross'] = data.Close.gt(data.ema).mul(1).diff()


    data['trigger_long_in'] = np.select([(data.cross == 1)&(data.Close >= data.EMA_filter)],[1],0)
    data['trigger_long_out'] = np.select([(data.Close.shift(1) < data.ema.shift(1))&(data.Low < data.Low.shift(1))],[1],0)
    data['trigger_short_in'] = np.select([(data.cross == -1)&(data.Close <= data.EMA_filter)],[1],0)
    data['trigger_short_out'] = np.select([(data.Close.shift(1) > data.ema.shift(1))&(data.High < data.High.shift(1))],[1],0)

    when_open = 'next_bar'
    price_open_long = 'Open'
    price_open_short = 'Open'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

def add_estrategia_smash_day(data, settings):
    
    EMA_period = settings['EMA_period']

    data['EMA'] = ta.ema(data['Close'],EMA_period)
    data['is_smash_long'] = np.select([(data.Close < data.Close.shift(1))&(data.Low > data.Low.shift(1))&(data.Close > data.EMA)],[1],0)
    data['is_smash_short'] = np.select([(data.Close > data.Close.shift(1))&(data.High < data.High.shift(1))&(data.Close < data.EMA)],[1],0)

    data['price_in'] = np.select([(data.is_smash_long.shift(1)==1),(data.is_smash_short.shift(1)==1)],[(data.High.shift(1)),(data.Low.shift(1))],0)

    data['trigger_long_in'] = np.select([(data.is_smash_long.shift(1) == 1)&(data.High > data.price_in)&(data.Open < data.price_in)],[1],0)
    data['trigger_long_out'] = np.select([(data.Close == 100_000)],[1],0)
    data['trigger_short_in'] = np.select([(data.is_smash_short.shift(1) == 1)&(data.Low < data.price_in)&(data.Open > data.price_in)],[1],0)
    data['trigger_short_out'] = np.select([(data.Close == 100_000)],[1],0)

    when_open = 'this_bar'
    price_open_long = 'price_in'
    price_open_short = 'price_in'
    when_close = 'next_bar'
    price_close_long = 'Open' # esto vale para los trigger_out >> para stop y tp ya elegimos seteado o al_cierre
    price_close_short = 'Open'
    order_out = ['stop_loss', 'take_profit', 'trigger_out']

    return data, order_out, when_open, when_close, price_open_long, price_open_short, price_close_long, price_close_short

###################################################################################################################
###################################################################################################################
####################### de acá para abajo es para probar que todo salga OK ########################################
#pero después dejarlo siempre comentado porque hace cagada cuando se cruza con las variables de los otros archivos#
###################################################################################################################
###################################################################################################################
# start = time.time()


# file = 'earnings_df.pickle'

# with open(file,'rb') as p:
#     df_dict = pickle.load(p)

# token_1 = 'AAPL'
# my_data = df_dict[token_1]

# my_data = my_data[-1000:-1]

# settings = {
#     'ssl_period': 10,
#     'sma_1': 200,
#     'sma_2': 150,
#     'sma_3': 100,
#     'sma_4': 50,
#     'sma_5': 30,
#     'sma_6': 30
# }

# data, _, _, _, _, _, _, _ = add_estrategia_ssl(my_data,settings)
# print(data.tail(80))
# #print(type(data.index[5]))
# data.to_excel('estrategia.xlsx')
# finish = time.time()
# print(f'Ejecutado en {round(finish-start,4)} segundos')