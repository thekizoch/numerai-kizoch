from google.cloud import bigquery
from google.cloud import storage
import os
import logging
import numerapi
import pandas as pd
import numpy as np

def trickle_in_fnc():
    # define environment variables
    PROJECT = 'numerai-kizoch'
    CURRENT_ROUND = numerapi.NumerAPI().get_current_round()

    # authenticate client
    public_id = "N6UCUI4HR6ADQHBQNUYHB5FEWE2W5WRG"
    secret_key = os.getenv('SECRET_KEY')
    napi = numerapi.NumerAPI(public_id=public_id, secret_key=secret_key)

    # get data
    # list of model names that are not 'test'
    d = napi.get_models()
    list_model_names = [x for x in d.keys() if 'test' not in x]
    # define weekly trickle amount in NMR
    weekly_trickle = float(os.getenv('WEEKLY_TRICKLE'))
    logging.info(type(weekly_trickle))
    # define smoothing
    alpha = 0.8
    # define threshold of sharpe
    threshold_sharpe = 0.2
    # input multipliers for all models
    multiplier_corr = 1
    multiplier_mmc = 2
    # use sharpe ratio to weight model
    risk_weighted = True
    # past rounds to consider
    rounds_to_use = 20

    # empty list to hold scores
    l = []
    # empty list to hold sharpe ratio of weekly returns
    l_sharpe = []

    # load each model from list
    for model_name in list_model_names:
      # load a single model
      print(f'loading {model_name}')
      dict_data = napi.round_model_performances(model_name)[0:rounds_to_use+3]
      df = pd.DataFrame.from_dict(dict_data)

      # keep essential rows for performance
      df = pd.DataFrame.from_dict(dict_data)
      df = df.loc[:,['corr','mmc','roundResolved','roundResolveTime']]
      # drop unresolved rounds, reorder
      df.drop(list(df.loc[df.roundResolved == False].index), inplace=True)
      df.sort_values(by='roundResolveTime', inplace=True)
      df.set_index('roundResolveTime', inplace=True)

      # add payout multipliers
      df['sum_corr_mmc_with_multipliers'] = df['corr']*multiplier_corr + df['mmc']*multiplier_mmc
      # checks that there are more than 5 resolved rounds
      if np.count_nonzero(~np.isnan(df.sum_corr_mmc_with_multipliers)) <= 5:
        #raise ValueError(f'There are 5 or less resolved rounds for {model_name}. Removing')
        print(f'There are 5 or less resolved rounds for {model_name}. Removing')
        continue

      # get smoothed score
      s = df.ewm(alpha=alpha).mean()['sum_corr_mmc_with_multipliers']

      # add to list of possible winners and losers
      l.append(s)
      l_sharpe.append(s.mean()/s.std())
      ###END loop

    # create frame of all smoothed scores
    ll = []
    for i, s in enumerate(l):
      s = s.to_frame().rename(columns={'sum_corr_mmc_with_multipliers':
                                       list_model_names[i]})
      ll.append(s)

    df = pd.concat(ll, axis=1)

    # gets winners above threshold
    winners = [df.columns[i] for i in range(len(l_sharpe)) if l_sharpe[i] > threshold_sharpe]
    # gets losers below threshold
    losers = list(set(df.columns) - set(winners))
    # define as frames
    df_winners = df[winners]
    df_losers = df[losers]
    # remove losers
    s_sharpe_winners = pd.Series(l_sharpe)
    s_sharpe_winners = s_sharpe_winners.loc[s_sharpe_winners > threshold_sharpe]
    s_sharpe_winners.index = df_winners.columns

    # calculate weights of trickle from performance of last round
    if risk_weighted == True:
      print('risk weighting the performance of models')
      s_weights = df_winners.iloc[-1] * s_sharpe_winners
    else:
      print('not risk weighting the performance of models')
      s_weights = df_winners.iloc[-1]

    total = s_weights.sum()

    # increase stake
    for model_name in winners:
      model = napi.get_models()[model_name]
      stake_increase = weekly_trickle * s_weights[model_name]/total
      print(f'{stake_increase} NMR stake increase for model {model_name}')
      napi.stake_increase(stake_increase, model)
