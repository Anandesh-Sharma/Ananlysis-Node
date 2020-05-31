from HardCode.scripts.Util import conn


def get_app_rejection_count(cust_id):
    script_status = {}
    premium_apps_rejection_count = 0
    normal_apps_rejection_count = 0
    try:
        client = conn()
        loan_details = client.analysis.loan.find_one({"cust_id": cust_id})
        rejection_count_details = loan_details['loan_rejection']['rejection_count']

        premium_apps = ['CASHBN', 'KREDTB', 'KREDTZ', 'LNFRNT', 'NIRAFN', 'SALARY']
        if rejection_count_details:
            for i in rejection_count_details.keys():
                if i in premium_apps:
                    if rejection_count_details[i] >= 1:
                        premium_apps_rejection_count += 1
                else:
                    if rejection_count_details[i] >= 1:
                        normal_apps_rejection_count += 1

        # upload rejection count detail in db here
        return normal_apps_rejection_count, premium_apps_rejection_count
    except BaseException as e:
        return normal_apps_rejection_count, premium_apps_rejection_count

