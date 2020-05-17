import pandas as pd

df = pd.read_csv("gen_csv_list/rule_based_parameters.csv")
l = []
for i, row in df.iterrows():
    if row['overdue_days'] > 10:
        continue
    if row['salary'] == 0:
        continue
    if row['normal_rej'] > 3:
        continue
    if row['premium_rej'] > 1:
        continue
    if row['credicxo_pending_emi'] > 0:
        continue
    if row['credicxo_12-15'] > 0:
        continue
    if row['credicxo_more_15'] > 0:
        continue
    if row['ecs'] > 0:
        continue
    if row['chq'] > 0:
        continue
    if row['total_msgs_of_user'] < 100:
        continue
    if row['total_loans_from_other_apps'] < 2:
        continue
    if row['credicxo_total_loans'] < 3:
        continue
    if row['current_open_loan'] == 0:
        l.append(row['user_id'])

with open('users_list.txt', 'w') as f:
    for item in l:
        f.write("%s\n" % item)
