from HardCode.scripts.loan_analysis.preprocessing import preprocessing
from datetime import datetime

def rule_quarantine(cust_id):
    report = {
        'total_loans' : 0,
        'currently_open' : 0,
        'messages_deleted_per_loan' : 0
    }

    data, list = preprocessing(cust_id)
    try:
        for i in data.keys():
            for j in data[i].keys():
                initial_date = datetime.strptime('2020-03-01 00:00:00', '%Y-%m-%d %H:%M:%S')
                if data[i][j]['disbursed_date'] != -1:
                    disbursed_date = datetime.strptime(str(data[i][j]['disbursed_date']), '%Y-%m-%d %H:%M:%S')
                    if (disbursed_date - initial_date).days > 1:
                        report['total_loans'] += 1
                        if data[i][j]['closed_date'] == -1 and data[i][j]['overdue_check'] >= 1:
                            report['currently_open'] += 1
                        elif data[i][j]['closed_date'] == -1 and data[i][j]['overdue_check'] == 0:
                            report['messages_deleted_per_loan'] += 1
                        else:
                            pass
                else:
                    if data[i][j]['closed_date'] != -1:
                        closed_date = datetime.strptime(str(data[i][j]['closed_date']), '%Y-%m-%d %H:%M:%S')
                        if (closed_date - initial_date).days > 20:
                            report['total_loans'] += 1
    except BaseException as e:
        print(e)
    if report['currently_open'] != 0:
        return False
    return True