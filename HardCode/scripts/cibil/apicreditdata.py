import pandas as pd
from HardCode.scripts.cibil.xmlparser import xml_parser


def convert_to_df(file):
    secured_loan = 0
    unsecured_loan = 0
    d = {'written_amt_total': [], 'written_amt_principal': [], 'credit_score': [], 'payment_rating': [],
         'payment_history': [], 'account_type': [], 'account_status': [],'secured_loan':[],'unsecured_loan':[]}
    data_dict, file_found = xml_parser(file)
    if file_found:
        try:
            try:
                acc_details = data_dict['INProfileResponse']['CAIS_Account']['CAIS_Account_DETAILS']
                loan_type = data_dict['INProfileResponse']['CAPS']['CAPS_Application_Details']
                # try:
                #     score = data_dict['INProfileResponse']['SCORE']
                #     credit_score = score['BureauScore']
                # except:
                #     credit_score = '0'
            except:
                d['written_amt_total'].append('0')
                d['written_amt_principal'].append('0')
                d['credit_score'].append('0')
                d['payment_history'].append('0')
                d['payment_rating'].append('0')
                d['account_type'].append('0')
                d['account_status'].append('0')
                d['secured_loan'].append('0')
                d['unsecured'].append('0')
                df = pd.DataFrame(d)
                message = "SUCCESS"
                response = {'status': True, 'data': df, 'message': message}
                return response

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

                secured_loan_count = secured_loan
                unsecured_loan_count = unsecured_loan
            else:
                secured_loan_count = '0'
                unsecured_loan_count = '0'


            if type(acc_details) is list:
                for i in range(0, len(acc_details)):
                    try:
                        amt_total = acc_details[i]['Written_Off_Amt_Total']
                    except:
                        amt_total = '0'
                    try:
                        amt_principal = acc_details[i]['Written_Off_Amt_Principal']
                    except:
                        amt_principal = '0'
                    try:
                        score = data_dict['INProfileResponse']['SCORE']
                        credit_score = score['BureauScore']
                    except:
                        credit_score = '0'
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
                    d['written_amt_total'].append(amt_total)
                    d['written_amt_principal'].append(amt_principal)
                    d['credit_score'].append(credit_score)
                    d['payment_history'].append(pay_history)
                    d['payment_rating'].append(str(pay_rating))
                    d['account_type'].append(acc_type)
                    d['account_status'].append(str(account_status))
                    d['secured_loan'].append(secured_loan_count)
                    d['unsecured_loan'].append(unsecured_loan_count)
            else:
                try:
                    amt_total = acc_details['Written_Off_Amt_Total']
                except:
                    amt_total = '0'
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
                d['written_amt_total'].append(amt_total)
                d['written_amt_principal'].append(amt_principal)
                d['credit_score'].append(credit_score)
                d['payment_history'].append(pay_history)
                d['payment_rating'].append(str(pay_rating))
                d['account_type'].append(acc_type)
                d['account_status'].append(str(account_status))
                d['secured_loan'].append(secured_loan_count)
                d['unsecured_loan'].append(unsecured_loan_count)

                 


            df = pd.DataFrame(d)
            message = "SUCCESS"
            response = {'status': True, 'data': df, 'message': message}
        except Exception as e:
            message = e
            d['written_amt_total'].append('0')
            d['written_amt_principal'].append('0')
            d['credit_score'].append('0')
            d['payment_history'].append('0')
            d['payment_rating'].append('0')
            d['account_type'].append('0')
            d['account_status'].append('0')
            d['secured_loan'].append('0')
            d['unsecured_loan'].append('0')
            df = pd.DataFrame(d)
            response = {'status': False, 'data': df, 'message': message}
        return response
    else:
        d['written_amt_total'].append('0')
        d['written_amt_principal'].append('0')
        d['credit_score'].append('0')
        d['payment_history'].append('0')
        d['payment_rating'].append('0')
        d['account_type'].append('0')
        d['account_status'].append('0')
        d['secured_loan'].append('0')
        d['unsecured_loan'].append('0')
        df = pd.DataFrame(d)
        response = {'status': False, 'data': df, 'message': 'None'}
        return response
