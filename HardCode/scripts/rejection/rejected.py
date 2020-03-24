from HardCode.scripts.rejection.preprocessing import *
from HardCode.scripts.rejection.list_of_headers import headers
from HardCode.scripts.Util import conn
from datetime import datetime
import pandas as pd


def check_rejection(user_id):
    df_approved, df_rejected = get_data(user_id)
    status = True
    res = {
        'rejected_loan_apps': []
    }
    try:
        if not df_approved.empty and not df_rejected.empty:
            df_approved = sms_header_splitter(df_approved)
            df_rejected = sms_header_splitter(df_rejected)

            df_approved = remove_unwanted_headers_and_sort(df_approved, headers)
            df_rejected = remove_unwanted_headers_and_sort(df_rejected, headers)

            if df_rejected.shape[0] != 0 and df_approved.shape[0] != 0:

                for i in range(df_rejected.shape[0]):
                    for j in range(df_approved.shape[0]):
                        time_rejected = datetime.strptime(str(df_rejected['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
                        time_approved = datetime.strptime(str(df_approved['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                        diff = (time_approved - time_rejected).days
                        if df_rejected['sender'][i] == df_approved['sender'][j] and diff < 3:
                            df_rejected.drop([i], axis=0, inplace=True)

                df_rejected.reset_index(drop=True)

                df_rejected_grouped = df_rejected.groupby(by='sender')
                result = {}
                for app, grp in df_rejected_grouped:
                    grp.reset_index(drop=True, inplace=True)

                    count_dict = {
                        'count': 0
                    }
                    i = 0
                    while i < grp.shape[0]:
                        # print(f"I : {i}")
                        time_1 = datetime.strptime(str(grp['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
                        j = i + 1
                        count_dict['count'] += 1
                        if j < grp.shape[0]:
                            time_2 = datetime.strptime(str(grp['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                            diff = (time_2 - time_1).days
                            if diff < 7:
                                i = j
                        i += 1
                    result[app] = count_dict

                res = {
                    'rejected_loan_apps': result
                }
        elif not df_rejected.empty and df_approved.empty:

            df_rejected = sms_header_splitter(df_rejected)
            df_rejected = remove_unwanted_headers_and_sort(df_rejected, headers)
            df_rejected_grouped = df_rejected.groupby(by='sender')

            result = {}
            for app, grp in df_rejected_grouped:
                grp.reset_index(drop=True, inplace=True)

                count_dict = {
                    'count': 0
                }
                i = 0
                while i < grp.shape[0]:
                    # print(f"I : {i}")
                    time_1 = datetime.strptime(str(grp['timestamp'][i]), '%Y-%m-%d %H:%M:%S')
                    j = i + 1
                    count_dict['count'] += 1
                    if j < grp.shape[0]:
                        time_2 = datetime.strptime(str(grp['timestamp'][j]), '%Y-%m-%d %H:%M:%S')
                        diff = (time_2 - time_1).days

                        if diff < 7:
                            i = j
                    i += 1
                result[app] = count_dict
            res = {
                'rejected_loan_apps': result
            }
    except BaseException as e:
        status = False
        print(e)
    finally:
        rejection_result = {'cust_id': user_id, 'status': status, 'result': res}
        connect = conn()
        db = connect.analysis.rejection
        print()
        db.update({'cust_id': user_id}, {'$set': rejection_result}, upsert = True)
        return rejection_result
