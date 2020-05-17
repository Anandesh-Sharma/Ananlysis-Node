from HardCode.scripts.Util import conn
import pandas as pd
from HardCode.scripts.testing.gen_csv_list.loan_rejection_2 import get_rejection_count
from HardCode.scripts.testing.gen_csv_list.overdue_details_2 import get_overdue_details


def fetch_parameters(user_id):
    connect = conn()
    df = dict()
    data = connect.analysis.parameters.find_one({'cust_id': user_id})
    premium_rej, normal_rej, message = get_rejection_count(user_id)
    report = get_overdue_details(user_id)
    due_days = report['overdue_days_segment']
    if report['overdue_days_list']:
        overdue_days = max(report['overdue_days_list'])
    else:
        overdue_days = -1
    df = {'user_id': user_id,
          'total_msgs_of_user': data['parameters']['Total_msg'],
          'overdue_ratio': data['parameters']['overdue_info']['overdue_ratio'],
          'overdue_days_list': data['parameters']['overdue_info']['overdue_days_list'],
          'total_loans_from_other_apps': report['total_loans'],
          'last_loan_details': data['parameters']['loan_details']['last_loan_details'],
          'total_loan_apps': data['parameters']['loan_info']['TOTAL_LOAN_APPS'],
          'current_open_loan': data['parameters']['loan_info']['CURRENT_OPEN'],
          'pay_within_30_days': data['parameters']['loan_info']['PAY_WITHIN_30_DAYS'],
          'overdue_days': overdue_days,
          'AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS': data['parameters']['loan_info']['AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS'],
          'max_current_open_amt': data['parameters']['loan_info']['CURRENT_OPEN_AMOUNT'],
          'max_loan_amt': data['parameters']['loan_info']['MAX_AMOUNT'],
          'ac_no': data['parameters']['available_balance']['AC_NO'],
          'bal_on_loan_date': data['parameters']['available_balance']['balance_on_loan_date'],
          'last_month_bal': data['parameters']['available_balance']['last_month_bal'],
          'second_last_month_bal': data['parameters']['available_balance']['second_last_month_bal'],
          'third_last_month_bal': data['parameters']['available_balance']['third_last_month_bal'],
          'count_creditordebit_msg': data['parameters']['available_balance']['count_creditordebit_msg'],
          'no_of_accounts': data['parameters']['available_balance']['no_of_accounts'],
          'avg_balance': data['parameters']['avg_balance'],
          'mean_balance': data['parameters']['mean_bal'],
          'salary': data['parameters']['salary'],
          'ecs': data['parameters']['ecs_bounce'],
          'chq': data['parameters']['chq_bounce'],
          'legal_msg_count': data['parameters']['legal_message_count'],
          'legal_msg_ratio': data['parameters']['legal_msg_ratio'],
          'overdue_msg_count': data['parameters']['overdue_msg_count'],
          'overdue_msg_ratio': data['parameters']['overdue_msg_ratio'],
          'account_status': data['parameters']['account_status'],
          'active': data['parameters']['active'],
          'closed': data['parameters']['closed'],
          'age_of_oldest_trade': data['parameters']['age_of_oldest_trade'],
          'age': data['parameters']['age'],
          'percentage_of_loan_apps': data['parameters']['percentage_of_loan_apps'],
          'payment_rating': data['parameters']['payment_rating'],
          'credicxo_loan_limit': data['parameters']['credicxo_loan_limit'],
          'credicxo_0-3': data['parameters']['credicxo_overdue_days']['0-3_days'],
          'credicxo_3-7_days': data['parameters']['credicxo_overdue_days']['3-7_days'],
          'credicxo_7-12': data['parameters']['credicxo_overdue_days']['7-12_days'],
          'credicxo_12-15': data['parameters']['credicxo_overdue_days']['12-15_days'],
          'credicxo_more_15': data['parameters']['credicxo_overdue_days']['more_than_15'],
          'credicxo_pending_emi': data['parameters']['credicxo_pending_emi'],
          'credicxo_total_loans': data['parameters']['credicxo_total_loans'],
          'similarity': data['parameters']['reference']['result']['similarity_score'],
          'relatives': data['parameters']['no_of_relatives'],
          'secured_loans': data['parameters']['secured_loans'],
          'unsecured_loans': data['parameters']['unsecured_loans'],
          'username_msgs': data['parameters']['username_msgs'],
          'normal_rej': premium_rej,
          'premium_rej': normal_rej,
          'overdue_0-3': due_days['0_3_days'],
          'overdue_3-7': due_days['3_7_days'],
          'overdue_7-12': due_days['7_12_days'],
          'overdue_12-15': due_days['12_15_days'],
          'overdue_more_15': due_days['more_than_15'],

          }
    # data = pd.DataFrame.from_dict(df)
    # data.to_csv(f'rule_based_parameters_{user_id}.csv', mode='a', header = ['user_id','total_msgs_of_user','overdue_ratio','overdue_days_list','total_loans_from_other_apps',
    #                                                        'last_loan_details','total_loan_apps','current_open_loan','pay_within_30_days',
    #                                                        'overdue_days','AVERAGE_EXCEPT_MAXIMUM_OVERDUE_DAYS','max_current_open_amt',
    #                                                        'max_loan_amt','ac_no','bal_on_loan_date','last_month_bal','second_last_month_bal',
    #                                                       'third_last_month_bal','count_creditordebit_msg','no_of_accounts','avg_balance',
    #                                                       'mean_balance','salary','ecs','chq','legal_msg_count','legal_msg_ratio',
    #                                                       'overdue_msg_count','overdue_msg_ratio','account_status','active','closed',
    #                                                       'age_of_oldest_trade','age','percentage_of_loan_apps','payment_rating','credicxo_loan_limit',
    #                                                       'credicxo_0-3','credicxo_3-7_days','credicxo_7-12','credicxo_12-15','credicxo_more_15',
    #                                                       'credicxo_pending_emi','credicxo_total_loans','similarity','relatives',
    #                                                       'secured_loans','unsecured_loans','username_msgs','normal_rej','premium_rej','overdue_0',
    #                                                                         'overdue_3','overdue_7','overdue_12-15','overdue_more_15'])
    connect.close()
    return df


# fetch_parameters(17684)
client = conn()

cust_ids = set(client.analysis.salary.distinct("cust_id", {}))
# print(cust_ids, len(cust_ids))

print(cust_ids - {21530, 22131, 37542, 146567, 163406, 167622, 185217, 208346, 224547, 240296, 244820, 254416, 262731,
                  263272, 280924, 281647, 298154, 301809, 305136, 305258, 306322, 338863, 355742})

list_of_dict = []

# for id in cust_ids:
#     try:
#         list_of_dict.append(fetch_parameters(id))
#     except Exception as e:
#         print(f"{e} for userid : {id}")
#         conn()
#     data = pd.DataFrame(list_of_dict)
#     data.to_csv(f'rule_based_parameters.csv')
