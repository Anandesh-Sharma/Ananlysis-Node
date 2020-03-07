# import json
import pandas
from . import xmlparser


def convert_to_df(user_id, file):
    d = {'written_amt_total': [], 'written_amt_principal': [], 'credit_score': [], 'payment_rating': [],
         'payment_history': [], 'account_type': [], 'account_status': []}
    data_dict, file_found = xmlparser.xml_parser(file)
    if file_found:
        try:
            try:
                acc_details = data_dict['INProfileResponse']['CAIS_Account']['CAIS_Account_DETAILS']
                try:
                    score = data_dict['INProfileResponse']['SCORE']
                    credit_score = score['BureauScore']
                except:
                    credit_score = ''
            except:
                d['written_amt_total'].append('')
                d['written_amt_principal'].append('')
                d['credit_score'].append(credit_score)
                d['payment_history'].append('')
                d['payment_rating'].append('')
                d['account_type'].append('')
                d['account_status'].append('')
                df = pandas.DataFrame(d)
                message = "SUCCESS"
                response = {'status': True, 'data': df, 'message': message}
                return response

            if type(acc_details) is list:
                for i in range(0, len(acc_details)):
                    try:
                        amt_total = acc_details[i]['Written_Off_Amt_Total']
                    except:
                        amt_total = ''
                    try:
                        amt_principal = acc_details[i]['Written_Off_Amt_Principal']
                    except:
                        amt_principal = ''
                    try:
                        score = data_dict['INProfileResponse']['SCORE']
                        credit_score = score['BureauScore']
                    except:
                        credit_score = ''
                    try:
                        pay_history = acc_details[i]['Payment_History_Profile']
                    except:
                        pay_history = ''
                    try:
                        acc_type = acc_details[i]['Account_Type']
                    except:
                        acc_type = ''
                    try:
                        pay_rating = acc_details[i]['Payment_Rating']
                    except:
                        pay_rating = ''
                    try:
                        account_status = acc_details[i]['Account_Status']
                    except:
                        account_status = ''
                    d['written_amt_total'].append(amt_total)
                    d['written_amt_principal'].append(amt_principal)
                    d['credit_score'].append(credit_score)
                    d['payment_history'].append(pay_history)
                    d['payment_rating'].append(str(pay_rating))
                    d['account_type'].append(acc_type)
                    d['account_status'].append(str(account_status))
            else:
                try:
                    amt_total = acc_details['Written_Off_Amt_Total']
                except:
                    amt_total = ''
                try:
                    amt_principal = acc_details['Written_Off_Amt_Principal']
                except:
                    amt_principal = ''
                try:
                    score = data_dict['INProfileResponse']['SCORE']
                    credit_score = score['BureauScore']
                except:
                    credit_score = ''
                try:
                    pay_history = acc_details['Payment_History_Profile']
                except:
                    pay_history = ''
                try:
                    acc_type = acc_details['Account_Type']
                except:
                    acc_type = ''
                try:
                    pay_rating = acc_details['Payment_Rating']
                except:
                    pay_rating = ''
                try:
                    account_status = acc_details['Account_Status']
                except:
                    account_status = ''
                d['written_amt_total'].append(amt_total)
                d['written_amt_principal'].append(amt_principal)
                d['credit_score'].append(credit_score)
                d['payment_history'].append(pay_history)
                d['payment_rating'].append(str(pay_rating))
                d['account_type'].append(acc_type)
                d['account_status'].append(str(account_status))
            df = pandas.DataFrame(d)
            message = "SUCCESS"
            response = {'status': True, 'data': df, 'message': message}
        except Exception as e:
            message = e
            response = {'status': False, 'data': None, 'message': message}
        return response
    else:
        response = {'status': False, 'data': None, 'message': 'None'}
        return response
