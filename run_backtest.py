import time
from estrategias import *
from backtesting_functions import monkey_test, mp_backtest, mp_walk_forward
from auxiliar import spx_momentum_filter, spx_filter
from rsubsets import rSubset
import pickle
import json

start = time.time()

with open('lista_acciones.pickle','rb') as p:
    lista_acciones = pickle.load(p)



source = 'df_dict.pickle'
universe_filter = None
settings = {   
    'tickers_list': [['SPY','QQQ','AAPL','NFLX','META','MELI','TSLA']],
    'fecha_desde_bt': ['2012-01-01'],
    'fecha_hasta_bt': ['2022-11-27'],
    'interval': ['1D'],
    'position_size_st': [0.50],
    'universe_size': [25], # solo sirve si universe_filter != None
    'short_selling': [False],
    'time_stop': [2],
    'stop_selection': ['seteado'],
    'trailing': [True],
    'comision': [0.0], 
    'stop_loss': [0.07],
    'take_profit': [0.10],
    'show_daily_results': [True]
}
account_sets = {
    'init_account': 100,
    'account_margin': 2, # no está andando bien el tema del margin, ojo!!
    'open_positions_long': {},
    'open_positions_short': {},
    'current_size': '',
    'available': ''
}
strategy = add_estrategia_smash_day
strategy_sets = {
    'EMA_period' : [100,50,20]
}

#sólo para walk forward:
steps = [(180,60)]

#sólo para monkey_test:
monkey_sets = {
    'random_entries': True,
    'random_exits': False,
    'pasadas': 100
}

if __name__ == '__main__':
    metricas, results = mp_backtest(
                                source,
                                universe_filter,
                                settings,
                                account_sets,
                                strategy,
                                strategy_sets,
                                #steps,
                                save_to='smash_spy'
                                )

    finish = time.time()
    print(str(round(finish-start,1)) +' segundos de ejecución')

