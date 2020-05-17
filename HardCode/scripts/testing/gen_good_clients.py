from HardCode.scripts.Util import conn
from pprint import pprint
from datetime import datetime
final_users_list = []

# -> get connection
client = conn()
user_ids = client.official.loans.distinct('cust_id', {})


def analyse_loan_details(loan_details):
    lockdown_date = datetime.strptime('2020-03-22 00:00:00', '%Y-%m-%d %H:%M:%S')
    # loan_date = datetime.strptime(loan_details[-1]['loan_date'], '%Y-%m-%d %H:%M:%S')
    if len(loan_details) < 2:
        return False
    for loan in reversed(loan_details):
        loan_date = datetime.strptime(loan['loan_date'][:19], '%Y-%m-%d %H:%M:%S')
        if loan_date >= lockdown_date:
            if loan['rd_emi_1'] == '' or loan['rd_emi_2'] == '':
                return False

        od = loan['od_emi_1'] + loan['od_emi_2']
        if od > 10:
            return False
    return True

# print(analyse_loan_details([{'loan_date': '2020-03-23 03:24:12.369739+00:00', 'ed_emi_1': '2020-05-05 00:00:00', 'rd_emi_1': '', 'ed_emi_2': '2020-05-23 00:00:00', 'rd_emi_2': '', 'od_emi_1': 0, 'od_emi_2': 0}]))


for user_id in user_ids:
    user_loan_details = client.official.loans.find_one({'cust_id': user_id})['loan_details']
    analysis_bool = analyse_loan_details(user_loan_details)
    if analysis_bool:
        final_users_list.append(user_id)

print(final_users_list)