from pymongo import MongoClient
from Classifier import classifier
from cibil_analysis import cibil_analysis
from Cheque_Bounce import cheque_user_outer
from Util import conn
import pandas as pd


def bl0(df_cibil, sms_json, user_id, new_user):
    '''
    Implements BL0
    
    Amount of the loan is calculated on the basis of cibil score and checks 
    the if there are any bounced check of the user and stores the user sms.
    
    Parameters:
    df_cibil (Data Frame)           : Containing fields of individual users with column names
        account_type(int)           : type of account
        pay_history-profile(string) : payment histroy of individual loan of user
        credit_score(int)           : credit score of the user
        written-of-amt-total(int)   : written amount total of specific loan
        written-amt_principal(int)  : written principle total of specific loan
        payment_rating(int)         : payment rating of a person
    
    sms_json(json object)   :containing the sms of the user
        timestamp(string)       :main dictionary containing keys
            body(string)            :body of message
            sender(string)          :sender's name
            read(bool)              :whether the message is seen
    
    user_id(string)         :user's specific id
    new_user(bool)          :Whether the user is new or not 
    
    Returns:
    dict    :containing follwing keys
        status(bool)    :whether the code worked correctly
        message(string) :explains the status
        onhold(bool)    :user is on hold or not
        user_id(string) :user's specific id
        limit(int)      :limiting amount of user calculated
        logic(string)   :buissness logic of the process
    '''

    if not isinstance(user_id, str):
        return {'status': False, 'message': 'user_id not string type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}
    if not isinstance(new_user, bool):
        return {'status': False, 'message': 'new_user not boolean type', 'onhold': None, 'user_id': user_id,
                'limit': None,
                'logic': 'BL0'}

    try:  # changes to be added for updating sms
        client = conn()
        db = client.messagecluster
        file1 = db.transaction.find_one({"_id": int(user_id)})
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    if file1 == None:
        result = classifier(sms_json, user_id)
        if not result['status']:
            client.close()
            return result
    try:
        file1 = db.extra.find_one({"_id": int(user_id)})
        df = pd.DataFrame(file1['sms'])
        a = cheque_user_outer(df)
        file1 = db.extra.find_one({"_id": int(user_id)})
        df = pd.DataFrame(file1['sms'])
        a += cheque_user_outer(df)
    except Exception as e:
        client.close()
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    client.close()
    if a > 0:
        return {'status': True, 'message': 'success', 'onhold': True, 'user_id': user_id, 'limit': -1,
                'logic': 'BL0'}
    if new_user:
        try:
            ans = cibil_analysis(df_cibil, cibil_score=749)
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
            ans = cibil_analysis(df_cibil, cibil_score=649)
            if ans:
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id, 'limit': 3000,
                        'logic': 'BL0'}
            else:
                return {'status': True, 'message': 'success', 'onhold': False, 'user_id': user_id, 'limit': 0,
                        'logic': 'BL0'}
        except Exception as e:
            return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                    'logic': 'BL0'}
