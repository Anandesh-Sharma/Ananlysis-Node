import pandas as pd
import datetime
from HardCode.scripts.cibil.xmlparser import xml_parser


def manipulatedate(date):
    date_object = None
    if date != 'None':
        date_str = str(date[4:6]) + '-' + str(date[6:]) + '-' + str(date[0:4])
        date_object = datetime.datetime.strptime(date_str, '%m-%d-%Y').date()
    return date_object


def convert_to_df(file):
    d = {'written_amt_total': [], 'written_amt_principal': [], 'credit_score': [], 'payment_rating': [],
         'payment_history': [], 'account_type': [], 'Days_Past_Due': [], 'age_of_oldest_trade': [],
         'SuitFiledWillfulDefaultWrittenOffStatus': [], 'Written_off_Settled_Status': [],
         'account_status': [], 'secured_loan': [], 'unsecured_loan': [], 'acc_type': []}
    data_dict, file_found = xml_parser(file)
    if file_found:
        try:
            score = data_dict['INProfileResponse']['SCORE']
            credit_score = score['BureauScore']
        except:
            credit_score = '0'

        try:
            loan_type = data_dict['INProfileResponse']['CAPS']['CAPS_Application_Details']
            secured_loan = 0
            unsecured_loan = 0
            if type(loan_type) is list:
                for acc in loan_type:
                    if acc['Enquiry_Reason'] == '2':
                        secured_loan += 1
                    elif acc['Enquiry_Reason'] == '4':
                        secured_loan += 1
                    elif acc['Enquiry_Reason'] == '8':
                        secured_loan += 1
                    elif acc['Enquiry_Reason'] == '10':
                        secured_loan += 1
                    elif acc['Enquiry_Reason'] == '14':
                        secured_loan += 1
                    else:
                        unsecured_loan += 1
            else:
                if loan_type['Enquiry_Reason'] == '2':
                    secured_loan += 1
                elif loan_type['Enquiry_Reason'] == '4':
                    secured_loan += 1
                elif loan_type['Enquiry_Reason'] == '8':
                    secured_loan += 1
                elif loan_type['Enquiry_Reason'] == '10':
                    secured_loan += 1
                elif loan_type['Enquiry_Reason'] == '14':
                    secured_loan += 1
                else:
                    unsecured_loan += 1

            secured_loan_count = secured_loan
            unsecured_loan_count = unsecured_loan

        except:
            secured_loan_count = '0'
            unsecured_loan_count = '0'

        try:
            acc_details = data_dict['INProfileResponse']['CAIS_Account']['CAIS_Account_DETAILS']

        except:
            d['written_amt_total'].append('0')
            d['written_amt_principal'].append('0')
            d['credit_score'].append(credit_score)
            d['payment_history'].append('0')
            d['payment_rating'].append('0')
            d['account_type'].append('0')
            d['account_status'].append('0')
            d['secured_loan'].append(str(secured_loan_count))
            d['unsecured_loan'].append(str(unsecured_loan_count))
            d['Days_Past_Due'].append('0')
            d['age_of_oldest_trade'].append('0')
            d['acc_type'].append('0')
            d['SuitFiledWillfulDefaultWrittenOffStatus'].append('0')
            d['Written_off_Settled_Status'].append('0')
            df = pd.DataFrame(d)
            message = "CAIS_Account_Details not found"
            response = {'status': True, 'data': df, 'message': message}
            return response

        if type(acc_details) is list:
            try:
                oldest_date_of_open = int(acc_details[0]['Open_Date'])
                latest_date_of_open = int(acc_details[0]['Open_Date'])
                try:
                    latest_closed_date = acc_details[0]['Date_Closed']
                except:
                    latest_closed_date = ''

            except:
                oldest_date_of_open = 99999999
                latest_date_of_open = 0
                latest_closed_date = ''
            for index in range(0, len(acc_details)):

                try:
                    open_date = int(acc_details[index]['Open_Date'])

                except:
                    continue
                closed_date = acc_details[index]['Date_Closed']
                if open_date < oldest_date_of_open:
                    oldest_date_of_open = open_date
                if open_date > latest_date_of_open:
                    latest_date_of_open = open_date
                    latest_closed_date = closed_date

            try:
                age_of_oldest_trade = manipulatedate(str(int(latest_closed_date))) - manipulatedate(str(oldest_date_of_open))
                age_of_oldest_trade = age_of_oldest_trade.days
            except:
                age_of_oldest_trade = manipulatedate(str(latest_date_of_open)) - manipulatedate(str(oldest_date_of_open))
                age_of_oldest_trade = age_of_oldest_trade.days

            for i in range(0, len(acc_details)):
                try:
                    amt_total = acc_details[i]['Written_Off_Amt_Total']
                except:
                    amt_total = '0'
                try:
                    ac_type = acc_details[i]['Account_Type']
                except:
                    ac_type = '0'
                try:
                    Written_off_Settled_Status = acc_details[i]['Written_off_Settled_Status']
                except:
                    Written_off_Settled_Status = '0'
                try:
                    SuitFiledWillfulDefaultWrittenOffStatus = acc_details[i]['SuitFiledWillfulDefaultWrittenOffStatus']
                except:
                    SuitFiledWillfulDefaultWrittenOffStatus = '0'
                try:
                    amt_principal = acc_details[i]['Written_Off_Amt_Principal']
                except:
                    amt_principal = '0'
                try:
                    pay_history = acc_details[i]['Payment_History_Profile']
                except:
                    pay_history = '0'
                try:
                    acc_type = acc_details[i]['Account_Type']
                except:
                    acc_type = '0'
                try:
                    pay_rating = acc_details[i]['Payment_Rating']
                except:
                    pay_rating = '0'
                try:
                    account_status = acc_details[i]['Account_Status']
                except:
                    account_status = '0'
                try:
                    acc_history = acc_details[i]['CAIS_Account_History']
                    max_days_pas_due = 0
                    if type(acc_history) == list:
                        for j in range(0, len(acc_history)):
                            try:
                                due_days = int(acc_history[j]['Days_Past_Due'])
                            except:
                                continue
                            if due_days > max_days_pas_due:
                                max_days_pas_due = due_days

                    else:
                        try:
                            max_days_pas_due = int(acc_history['Days_Past_Due'])
                        except:
                            max_days_pas_due = '0'

                except:
                    max_days_pas_due = '0'


                d['written_amt_total'].append(amt_total)
                d['written_amt_principal'].append(amt_principal)
                d['credit_score'].append(credit_score)
                d['payment_history'].append(pay_history)
                d['payment_rating'].append(str(pay_rating))
                d['account_type'].append(acc_type)
                d['account_status'].append(str(account_status))
                d['Days_Past_Due'].append(str(max_days_pas_due))
                d['secured_loan'].append(secured_loan_count)
                d['unsecured_loan'].append(unsecured_loan_count)
                d['age_of_oldest_trade'].append(age_of_oldest_trade)
                d['acc_type'].append(ac_type)
                d['SuitFiledWillfulDefaultWrittenOffStatus'].append(SuitFiledWillfulDefaultWrittenOffStatus)
                d['Written_off_Settled_Status'].append(Written_off_Settled_Status)

        else:
            try:
                amt_total = acc_details['Written_Off_Amt_Total']
            except:
                amt_total = '0'
            try:
                SuitFiledWillfulDefaultWrittenOffStatus = acc_details['SuitFiledWillfulDefaultWrittenOffStatus']
            except:
                SuitFiledWillfulDefaultWrittenOffStatus = '0'
            try:
                Written_off_Settled_Status = '0'
            except:
                Written_off_Settled_Status = '0'

            try:
                ac_type = acc_details['Account_Type']
            except:
                ac_type = '0'
            try:
                amt_principal = acc_details['Written_Off_Amt_Principal']
            except:
                amt_principal = '0'
            try:
                score = data_dict['INProfileResponse']['SCORE']
                credit_score = score['BureauScore']
            except:
                credit_score = '0'
            try:
                pay_history = acc_details['Payment_History_Profile']
            except:
                pay_history = '0'
            try:
                acc_type = acc_details['Account_Type']
            except:
                acc_type = '0'
            try:
                pay_rating = acc_details['Payment_Rating']
            except:
                pay_rating = '0'
            try:
                account_status = acc_details['Account_Status']
            except:
                account_status = '0'
            try:
                acc_history = acc_details['CAIS_Account_History']
                max_days_pas_due = '0'
                if type(acc_history) == list:
                    for j in range(0, len(acc_history)):
                        due_days = acc_history[j]['Days_Past_Due']
                        if due_days > max_days_pas_due:
                            max_days_pas_due = due_days
                else:
                    try:
                        max_days_pas_due = int(acc_history['Days_Past_Due'])
                    except:
                        max_days_pas_due = '0'

            except:
                max_days_pas_due = '0'
            d['written_amt_total'].append(amt_total)
            d['written_amt_principal'].append(amt_principal)
            d['Days_Past_Due'].append(max_days_pas_due)
            d['credit_score'].append(credit_score)
            d['payment_history'].append(pay_history)
            d['payment_rating'].append(str(pay_rating))
            d['account_type'].append(acc_type)
            d['account_status'].append(str(account_status))
            d['secured_loan'].append(secured_loan_count)
            d['age_of_oldest_trade'].append('0')
            d['unsecured_loan'].append(unsecured_loan_count)
            d['acc_type'].append(ac_type)
            d['SuitFiledWillfulDefaultWrittenOffStatus'].append(SuitFiledWillfulDefaultWrittenOffStatus)
            d['Written_off_Settled_Status'].append(Written_off_Settled_Status)

        df = pd.DataFrame(d)
        message = "SUCCESS"
        response = {'status': True, 'data': df, 'message': message}
        return response
    else:
        d['written_amt_total'].append('0')
        d['written_amt_principal'].append('0')
        d['credit_score'].append('0')
        d['Days_Past_Due'].append('0')
        d['payment_history'].append('0')
        d['payment_rating'].append('0')
        d['account_type'].append('0')
        d['account_status'].append('0')
        d['secured_loan'].append('0')
        d['unsecured_loan'].append('0')
        d['age_of_oldest_trade'].append('0')
        d['acc_type'].append('0')
        d['SuitFiledWillfulDefaultWrittenOffStatus'].append('0')
        d['Written_off_Settled_Status'].append('0')
        df = pd.DataFrame(d)
        response = {'status': False, 'data': df, 'message': 'File not found'}
        return response
