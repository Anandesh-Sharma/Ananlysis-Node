from HardCode.scripts.Util import conn
import pandas as pd
from datetime import datetime

# -> convert csv to df
df = pd.DataFrame()
all_user_loan_df = pd.read_csv(
    '/home/ravan/credicxo-projects/analysisnode/HardCode/scripts/testing/completeloandetails.csv')
# droped last two columns
all_user_loan_df = all_user_loan_df.drop(['overdue_EMI1', 'overdue_EMI2'], axis=1)

all_user_loan_df['repayment_date_EMI1'] = pd.to_datetime(all_user_loan_df['repayment_date_EMI1'])
all_user_loan_df['repayment_date_EMI2'] = pd.to_datetime(all_user_loan_df['repayment_date_EMI2'])
all_user_loan_df['expected_date_1'] = pd.to_datetime(all_user_loan_df['expected_date_1'])
all_user_loan_df['expected_date_2'] = pd.to_datetime(all_user_loan_df['expected_date_2'])
all_user_loan_df['loan_date'] = pd.to_datetime(all_user_loan_df['loan_date'])

all_user_loan_df.sort_values(by=['loan_date'], inplace=True, ignore_index=True)
for loan in all_user_loan_df.itertuples(index=False):
    user_id = loan[1]
    loan_obj = dict()
    loan_obj["loan_date"] = str(loan[2])

    loan_obj["ed_emi_1"] = str(loan[3])
    if str(loan[5]) == 'NaT':
        loan_obj["rd_emi_1"] = ""
        od_emi_1 = (datetime.now() - loan[3]).days
    else:
        loan_obj["rd_emi_1"] = str(loan[5])
        od_emi_1 = (loan[5] - loan[3]).days

    loan_obj["ed_emi_2"] = str(loan[4])
    if str(loan[6]) == 'NaT':
        loan_obj["rd_emi_2"] = ""
        od_emi_2 = (datetime.now() - loan[4]).days
    else:
        loan_obj["rd_emi_2"] = str(loan[6])
        od_emi_2 = (loan[6] - loan[4]).days

    if od_emi_2 < 0:
        od_emi_2 = 0
    if od_emi_1 < 0:
        od_emi_1 = 0
    loan_obj["od_emi_1"] = od_emi_1
    loan_obj["od_emi_2"] = od_emi_2

    print(loan_obj)

    client = conn()
    client.official.loans.update({'cust_id': user_id}, {'$push': {'loan_details': loan_obj}}, upsert=True)
    client.close()

