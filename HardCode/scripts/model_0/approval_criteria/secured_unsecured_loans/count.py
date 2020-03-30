import pandas
from datetime import datetime
from HardCode.scripts.cibil.xmlparser import xml_parser
from collections import OrderedDict


def secure_unsecured_loan(user_id, cibil_file):
    secured_loan = 0
    unsecured_loan = 0

    data_dict, file_found = xml_parser(cibil_file)
    if file_found:
        try:
            acc_details = data_dict['INProfileResponse']['CAPS']['CAPS_Application_Details']

        except Exception as e:

            response = {'status': True, 'secured_loan': 0, 'unsecured_loan': 0, 'message': e, 'cust_id': user_id}
            return response

        if type(acc_details) is list:
            for acc in acc_details:
                try:
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
                except:
                    message = "No Enquiry reason"

        message = "SUCCESS"
        response = {'status': True, 'secured_loan': secured_loan, 'unsecured_loan': unsecured_loan, 'message': message,
                    'cust_id': user_id}

        return response
    else:
        response = {'status': False, 'secured_loan': 0, 'unsecured_loan': 0, 'message': 'None', 'cust_id': user_id}
        return response

