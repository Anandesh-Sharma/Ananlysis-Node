from HardCode.scripts.Util import conn
from pprint import pprint
import pandas as pd
from HardCode.scripts.testing.gen_csv_list.loan_rejection_2 import get_rejection_count
from HardCode.scripts.testing.gen_csv_list.overdue_details_2 import get_overdue_details
from HardCode.scripts.testing.user_ids import users


def fetch_parameters(user_id):
    connect = conn()
    df = dict()
    data = connect.analysis.parameters.find_one({'cust_id': user_id})['parameters'][-1]
    report = get_overdue_details(user_id)
    due_days = report['overdue_days_segment']
    if report['overdue_days_list']:
        overdue_days = max(report['overdue_days_list'])
    else:
        overdue_days = -1
    df = {'user_id': user_id,
          'total_msgs_of_user': data['Total_msgs'],
          'overdue_ratio': data['overdue_info']['overdue_ratio'],
          'overdue_days_list': data['overdue_info']['overdue_days_list'],
          'total_loans_from_other_apps': report['total_loans'],
          'last_loan_details': data['last_loan_details'],
          'total_loan_apps': data['loan_info']['TOTAL_LOAN_APPS'],
          'current_open_loan': data['loan_info']['CURRENT_OPEN'],
          'pay_within_30_days': data['loan_info']['PAY_WITHIN_30_DAYS'],
          'overdue_days': overdue_days,
          'AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS': data['loan_info']['AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS'],
          'max_current_open_amt': data['loan_info']['CURRENT_OPEN_AMOUNT'],
          'max_loan_amt': data['loan_info']['MAX_AMOUNT'],
          'ac_no': data['available_balance']['AC_NO'],
          'bal_on_loan_date': data['available_balance']['balance_on_loan_date'],
          'last_month_bal': data['available_balance']['last_month_bal'],
          'second_last_month_bal': data['available_balance']['second_last_month_bal'],
          'third_last_month_bal': data['available_balance']['third_last_month_bal'],
          'count_creditordebit_msg': data['available_balance']['count_creditordebit_msg'],
          'no_of_accounts': data['available_balance']['no_of_accounts'],
          'avg_balance': data['avg_balance'],
          'mean_balance': data['mean_bal'],
          'salary': data['salary'],
          'quarantine_salary': data['quarantine_salary'],
          'ecs': data['ecs_bounce'],
          'chq': data['chq_bounce'],
          'legal_msg_count': data['legal_msg_count'],
          'legal_msg_ratio': data['legal_msg_ratio'],
          'overdue_msg_count': data['overdue_msg_count'],
          'overdue_msg_ratio': data['overdue_msg_ratio'],
          'account_status': data['account_status'],
          'active': data['active'],
          'closed': data['closed'],
          'age_of_oldest_trade': data['age_of_oldest_trade'],
          'age': data['age'],
          'percentage_of_loan_apps': data['percentage_of_loan_apps'],
          'payment_rating': data['payment_rating'],
          'credicxo_loan_limit': data['credicxo_loan_limit'],
          'credicxo_0-3': data['credicxo_overdue_days']['0-3_days'],
          'credicxo_3-7_days': data['credicxo_overdue_days']['3-7_days'],
          'credicxo_7-12': data['credicxo_overdue_days']['7-12_days'],
          'credicxo_12-15': data['credicxo_overdue_days']['12-15_days'],
          'credicxo_more_15': data['credicxo_overdue_days']['more_than_15'],
          'credicxo_pending_emi': data['credicxo_pending_emi'],
          'credicxo_total_loans': data['credicxo_total_loans'],
          'similarity': data['reference']['result']['similarity_score'],
          'relatives': data['no_of_relatives'],
          'secured_loans': data['secured_loans'],
          'unsecured_loans': data['unsecured_loans'],
          'username_msgs': data['username_msgs'],
          'normal_rej': data['normal_app_rejection'],
          'premium_rej': data['premium_app_rejection'],
          'overdue_0-3': due_days['0_3_days'],
          'overdue_3-7': due_days['3_7_days'],
          'overdue_7-12': due_days['7_12_days'],
          'overdue_12-15': due_days['12_15_days'],
          'overdue_more_15': due_days['more_than_15'],
          'payment_history': data['payment_history'],
          'written_amt_principal': data['written_amt_principal'],
          'written_amt_total': data['written_amt_total'],
          'no_of_contacts': data['no_of_contacts']

          }
    connect.close()
    return df


def fetch_messages(user_id):
    connect = conn()
    chq = connect.messagecluster.cheque_bounce_msgs.find_one({'cust_id': user_id})
    ecs = connect.messagecluster.ecs_msgs.find_one({'cust_id': user_id})
    legal = connect.messagecluster.legal_msgs.find_one({'cust_id': user_id})
    sal = connect.analysis.salary.find_one({'cust_id': user_id})
    message = connect.messagecluster.loanrejection.find_one({'cust_id': user_id})
    loan = connect.analysis.loan.find_one({'cust_id': user_id})
    msgs = {}
    msgs['rejection'] = message['sms']
    if chq:
        msgs['cheque_bounce'] = chq
    if ecs:
        msgs['ecs_bounce'] = ecs
    if legal:
        msgs['legal'] = legal
    if sal:
        months = list(sal['salary'].keys())[::-1]
        for i in months:
            if sal['salary'][i]['salary'] != 0:
                msgs['salary'] = sal['salary'][i]['message']['body']
    if loan:
        msgs['loan'] = loan['complete_info']

    connect.close()

    with open(f'messages_{user_id}.txt', 'wt', encoding='utf-8') as out:
        pprint(msgs, stream=out)


# cust_ids = set(client.analysis.salary.distinct("cust_id", {}))
# # print(cust_ids, len(cust_ids))


cust_ids = users
list_of_dict = []

# for id in cust_ids:
#     try:
#         list_of_dict.append(fetch_parameters(id))
#     except Exception as e:
#         print(f"{e} for userid : {id}")
#         conn()#
#     data = pd.DataFrame(list_of_dict)
#     data.to_csv(f'rule_based_parameters.csv')
# fetch_messages(id)

client = conn()
dict = {}
# for id in []:
#     len = client.analysis.parameters.find_one({'cust_id': id})['parameters'][-1]['total_msgs']
#     dict[id] = len
# dict.sort(by = len)
# sorted = {}
# for i in len(dict):
#     sorted[dict[i][id]] = i+1

for i in users:
    # len = client.analysis.parameters.find_one({'cust_id':key})['parameters'][-1]['total_msgs']
    data = client.messagecluster.disbursed.find_one({'cust_id': i})
    data1 = client.messagecluster.loanclosed.find_one({'cust_id': i})
    data2 = client.messagecluster.loandue.find_one({'cust_id': i})
    data3 = client.messagecluster.loanoverdue.find_one({'cust_id': i})
    data4 = client.messagecluster.extra.find_one({'cust_id': i})
    try:
        data = pd.DataFrame(data['sms'])
        data1 = pd.DataFrame(data1['sms'])
        data2 = pd.DataFrame(data2['sms'])
        data3 = pd.DataFrame(data3['sms'])
        data4 = pd.DataFrame(data4['sms'])
        data.to_csv(f'users_data/mongo_disbursed_{i}.csv')
        data1.to_csv(f'users_data/mongo_closed_{i}.csv')
        data2.to_csv(f'users_data/mongo_due_{i}.csv')
        data3.to_csv(f'users_data/mongo_overdue_{i}.csv')
        data4.to_csv(f'users_data/mongo_extra_{i}.csv')
    except:
        continue
