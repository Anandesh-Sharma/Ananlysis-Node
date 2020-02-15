from pymongo import MongoClient
from .Classifier import classifier
from .cibil_analysis import cibil_analysis
from .Cheque_Bounce import cheque_user_outer
from .Util import conn
from .Loan_Analysis import loan_analysis
from .Salary_Analysis import salary_analysis
import pandas as pd


def bl0(df_cibil, sms_json, user_id, new_user):
    '''
    Implements BL0
    
    Amount of the loan is calculated on the basis of cibil score and checks 
    the if there are any bounced check of the user and stores the user sms.

    Parameters:
    df_cibil (Data Frame)           : Containing fields of individual users with column names
        account_type(int)           : type of account
        payment_history(string)     : payment histroy of individual loan of user
        credit_score(int)           : credit score of the user
        written_amt_total(int)      : written amount total of specific loan
        written_amt_principal(int)  : written principle total of specific loan
        payment_rating(int)         : payment rating of a person
    
    sms_json(json object)   :containing the sms of the user
        timestamp(string)       :main dictionary containing keys
            body(string)            :body of message
            sender(string)          :sender's name
            read(bool)              :whether the message is seen
    
    user_id(int)            :user's specific id
    new_user(bool)          :Whether the user is new or not 
    
    Returns:
    dict    :containing follwing keys
        status(bool)    :whether the code worked correctly
        message(string) :explains the status
        onhold(bool)    :user is on hold or not
        user_id(int)    :user's specific id
        limit(int)      :limiting amount of user calculated
        logic(string)   :buissness logic of the process
    '''

    if not isinstance(user_id, int):
        return {'status': False, 'message': 'user_id not int type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}
    
    if not isinstance(new_user, bool):
        return {'status': False, 'message': 'new_user not boolean type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}


    try:  # changes to be added for updating sms
        client = conn()
        db = client.messagecluster
        file1 = db.transaction.find_one({"_id": user_id})
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    if file1 == None:
        result = classifier(sms_json, str(user_id))

        if not result['status']:
            client.close()
            result['user_id']=user_id
            return result
                         
    try:
        file1 = db.extra.find_one({"_id": user_id})
        df = pd.DataFrame(file1['sms'])
        a = cheque_user_outer(df)
        file1 = db.extra.find_one({"_id": user_id})
        df = pd.DataFrame(file1['sms'])
        a += cheque_user_outer(df)
    except Exception as e:
        client.close()
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    
    if a>0:
        return {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id, 'limit': -1,
                'logic': 'BL0'}

    try:
            loan_analysis(str(user_id))
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                        'logic': 'BL0'}
    except:
        return {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id, 'limit': None,
                        'logic': 'BL0'}
    
    try:
        salary_analysis(str(user_id))
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                        'logic': 'BL0'}
    except:
        return {'status': False, 'message': 'unhandeled error in loan_analysis', 'onhold': None, 'user_id': user_id, 'limit': None,
                        'logic': 'BL0'}
    
    if not isinstance(df_cibil, pd.DataFrame):
        return {'status': False, 'message': 'df_cibil not dataframe type', 'onhold': None, 'user_id': user_id,
                'limit': None, 'logic': 'BL0'}
    
    if df_cibil.empty:
        return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id,
                'limit': 0, 'logic': 'BL0'}

    req_col = ["account_type","payment_history","credit_score","written_amt_total","written_amt_principal","payment_rating"]
    temp_l = df_cibil.columns

    for i in req_col:
        if i not in temp_l:
            return {'status': False, 'message': "df_cibil doesn't contain required columns", 'onhold': None, 'user_id': user_id,
                'limit': None, 'logic': 'BL0'}
    
    del temp_l

    if new_user:
        try:
            result = cibil_analysis(df_cibil, 749, user_id)
            if not result['status']:
                return result
            
            ans=result['ans']

            if ans:
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id, 'limit': 3000,
                        'logic': 'BL0'}
            else:
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id, 'limit': 0,
                        'logic': 'BL0'}
        except Exception as e:
            return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                    'logic': 'BL0'}

    else:
        try:
            result = cibil_analysis(df_cibil, 649,user_id)
            if not result['status']:
                return result

            ans=result['ans']

            if ans:
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id, 'limit': 3000,
                        'logic': 'BL0'}
            else:
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id, 'limit': 0,
                        'logic': 'BL0'}
        except Exception as e:
            return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                    'logic': 'BL0'}
