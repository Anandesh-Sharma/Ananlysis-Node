from Classifier_CreditCard import credit
from Classifier_Loan import loan
from Classifier_transaction import cleaning
import multiprocessing
from Util import conn, read_json, convert_json
import json


def extra(df, user_id, result):
    """
        Extract all extra messages for extra cluster on MongoDB.

        All messages other than important categories like loan messages, transaction messages,
        credit card messages are saved in this cluster.

        Parameters:
        df (Dataframe)
        user_id (str)
        result (Dict)

        Returns:
         dict :containing follwing keys
         status(bool) :whether the code worked correctly
         message(string) :explains the status
         onhold(bool) :user is on hold or not
         user_id(string) :user's specific id
         limit(int) :limiting amount of user calculated
         logic(string) :buissness logic of the process
        """
    for i in result.keys():
        df.drop(list(set(result[i])), inplace=True)
        df.reset_index(drop=True, inplace=True)
        data_extra = convert_json(df, user_id)
        try:
            client = conn()
            db = client.messagecluster
            db.extra.insert_one(data_extra)
        except Exception as e:
            return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                    'logic': 'BL0'}
    return {'status': True, 'message': 'success', 'onhold': None, 'user_id': user_id, 'limit': None,
            'logic': 'BL0'}


def classifier(sms_json, user_id):
    """
        Classifies all sms on basis of categories like loan, transaction, credit card.


        Parameters:
        sms_json(json object) :containing the sms of the user
             timestamp(string) :main dictionary containing keys
                 body(string) :body of message
                 sender(string) :sender's name
                 read(bool) :whether the message is seen

        user_id(string) :user's specific id

        Returns:
         dict :containing follwing keys
         status(bool) :whether the code worked correctly
         message(string) :explains the status
         onhold(bool) :user is on hold or not
         user_id(string) :user's specific id
         limit(int) :limiting amount of user calculated
         logic(string) :buissness logic of the process

        """
    manager = multiprocessing.Manager()
    result = manager.dict()
    result1 = read_json(sms_json, user_id)
    if not result1['status']:
        return result1
    df = result1['df']

    try:
        p1 = multiprocessing.Process(target=credit, args=(df, result, user_id,))
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    try:
        p2 = multiprocessing.Process(target=loan(df, result, user_id, ))
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    try:
        p3 = multiprocessing.Process(target=cleaning(df, result, user_id, ))
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    try:
        p1.start()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    try:
        p2.start()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    try:
        p3.start()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    try:
        p1.join()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    try:
        p2.join()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    try:
        p3.join()
    except Exception as e:
        return {'status': False, 'message': e, 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}

    result1 = extra(df, user_id, result)
    return result1



