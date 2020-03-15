import pandas as pd
# import numpy as np
# import datetime
# import threading


def upi_ref_check(data):
    try:
        fault = []
        for i in range(data.shape[0] - 1):
            if data['UPI Ref no'][i] == 0:
                continue
            for j in range(i + 1, data.shape[0]):
                if data['UPI Ref no'][i] == 0:
                    continue
                if data['UPI Ref no'][i] == data['UPI Ref no'][j]:
                    if data['credit_amount'][i] != 0 or data['credit_amount'][i] != '0':
                        if data['credit_amount'][j] != 0 or data['credit_amount'][j] != '0':
                            if data['credit_amount'][i] == data['credit_amount'][j]:
                                fault.append(i)
                                break
                    elif data['debit_amount'][i] != 0 or data['debit_amount'][i] != '0':
                        if data['debit_amount'][j] != 0 or data['debit_amount'][j] != '0':
                            if data['debit_amount'][i] == data['debit_amount'][j]:
                                fault.append(i)
                                break
        fault = list(set(fault))
        data = data.drop(fault)
        data.reset_index(drop=True, inplace=True)
    except Exception as e:
        return {'status': False, 'message': e}
    return {'status': True, 'message': 'success', 'df': data}


def imps_ref_check(data):
    try:
        fault = []
        for i in range(data.shape[0] - 1):
            if data['IMPS Ref no'][i] == 0:
                continue
            for j in range(i + 1, data.shape[0]):
                if data['IMPS Ref no'][i] == 0:
                    continue
                if data['IMPS Ref no'][i] == data['IMPS Ref no'][j]:
                    if data['credit_amount'][i] != 0 or data['credit_amount'][i] != '0':
                        if data['credit_amount'][j] != 0 or data['credit_amount'][j] != '0':
                            if data['credit_amount'][i] == data['credit_amount'][j]:
                                fault.append(i)
                                break
                    elif data['debit_amount'][i] != 0 or data['debit_amount'][i] != '0':
                        if data['debit_amount'][j] != 0 or data['debit_amount'][j] != '0':
                            if data['debit_amount'][i] == data['debit_amount'][j]:
                                fault.append(i)
                                break
        fault = list(set(fault))
        data = data.drop(fault)
        data.reset_index(drop=True, inplace=True)
    except Exception as e:
        return {'status': False, 'message': e}
    return {'status': True, 'message': 'success', 'df': data}


def time_based_checking(data):
    fault = []
    try:
        for i in range(data.shape[0] - 1):
            if data['date_time'][i] == 0:
                continue
            time_i_more = pd.to_datetime(data['date_time'][i]) + pd.Timedelta('0 days 00:03:00')
            time_i_less = pd.to_datetime(data['date_time'][i]) - pd.Timedelta('0 days 00:03:00')
            k = i + 6
            if k > data.shape[0]:
                k = data.shape[0]
            for j in range(i + 1, k):
                if data['date_time'][j] == 0:
                    continue
                time_j = pd.to_datetime(data['date_time'][j])
                if time_i_more > time_j > time_i_less:
                    if data['credit_amount'][i] != 0 or data['credit_amount'][i] != '0':
                        if data['credit_amount'][j] != 0 or data['credit_amount'][j] != '0':
                            if data['credit_amount'][i] == data['credit_amount'][j]:
                                fault.append(i)
                                break
                    elif data['debit_amount'][i] != 0 or data['debit_amount'][i] != '0':
                        if data['debit_amount'][j] != 0 or data['debit_amount'][j] != '0':
                            if data['debit_amount'][i] == data['debit_amount'][j]:
                                fault.append(i)
                                break
        data.drop(fault)
        data.reset_index(drop=True, inplace=True)
        return {'df': data, 'status': True, 'message': 'success'}
    except Exception as e:
        return {'status': False, 'message': str(e)}


def time_check_dbs(data):
    fault = []
    try:
        for i in range(data.shape[0] - 1):
            if data['sender'][i] != 'DBSBNK':
                continue
            time = pd.to_datetime(data['timestamp'][i]) + pd.Timedelta('0 days 00:03:00')
            for j in range(i + 1, data.shape[0]):
                if data['sender'][j] != 'DBSBNK':
                    continue
                time_j = pd.to_datetime(data['timestamp'][j])
                if time_j < time:
                    if data['credit_amount'][i] != 0 or data['credit_amount'][i] != '0':
                        if data['credit_amount'][j] != 0 or data['credit_amount'][j] != '0':
                            if data['credit_amount'][i] == data['credit_amount'][j]:
                                fault.append(i)
                                break
                    elif data['debit_amount'][i] != 0 or data['debit_amount'][i] != '0':
                        if data['debit_amount'][j] != 0 or data['debit_amount'][j] != '0':
                            if data['debit_amount'][i] == data['debit_amount'][j]:
                                fault.append(i)
                                break
                else:
                    break
        data.drop(fault)
        data.reset_index(drop=True, inplace=True)
        return {'data': data, 'status': True, 'message': 'success', 'df': data}
    except Exception as e:
        return {'status': False, 'message': str(e)}
