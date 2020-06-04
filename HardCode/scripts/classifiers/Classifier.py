from .Classifier_CreditCard import credit
from .Classifier_Loan import loan
from .Classifier_transaction import cleaning
from .Classifier_salary import salary
from .Classifier_legal import legal_Classifier
from .Classifier_cheque_bounce import Cheque_Classifier
from .Classifier_ecs import Ecs_Classifier
from multiprocessing import Pool
from HardCode.scripts.Util import conn, read_json, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def extra(args):
    df = args[0]
    result = args[1]
    user_id = args[2]
    max_timestamp = args[3]
    new = args[4]
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
    logger = logger_1("extra", user_id)
    logger.info("Generating dictionary of extra sms")
    for i in result.keys():
        df.drop(list(set(result[i])), inplace=True)
        df.reset_index(drop=True, inplace=True)
        data_extra = convert_json(df, user_id, max_timestamp)
    logger.info("Converted extra sms into JSON obj")

    try:
        logger.info('making connection with db')
        client = conn()
        db = client.messagecluster
    except Exception as e:
        logger.critical('error in connection')
        return {'status': False, 'message': str(e), 'onhold': None, 'user_id': user_id, 'limit': None,
                'logic': 'BL0'}
    logger.info('connection success')

    if new:
        logger.info("New user checked")
        # db.extra.insert_one(data_extra)
        db.extra.update({"cust_id": int(user_id)},
                        {"cust_id": int(user_id), "sms": data_extra['sms'], "timestamp": data_extra['timestamp'],
                         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}, upsert=True)
        logger.info("Extra sms of new user inserted successfully")

    else:
        for i in range(len(data_extra['sms'])):
            logger.info("Old User checked")
            db.extra.update({"cust_id": int(user_id)}, {"$push": {"sms": data_extra['sms'][i]}})
            logger.info("Extra sms of old user updated successfully")
        db.extra.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                            upsert=True)
        logger.info("Timestamp of User updated")
    client.close()


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
    logger = logger_1("Classifier", user_id)
    logger.info("Creating Multiprocessing Manager")
    result = dict()
    logger.info("Read sms json object")
    result1 = read_json(sms_json, user_id)
    if not result1:
        logger.error("JSON not read successfully")
        return result1
    df = result1['df']
    new = result1['new']
    max_timestamp = result1['timestamp']
    try:
        logger.info("Classification start for Credit card Classifier")
        with Pool() as p:
            result = p.map(credit, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("Classification start for Loan Classifier")
        with Pool() as p:
            result = p.map(loan, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("Classification start for Transaction Classifier")
        with Pool() as p:
            result = p.map(cleaning, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("Classification start for Salary Classifier")
        with Pool() as p:
            result = p.map(salary, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("Classification start for Legal Classifier")
        with Pool() as p:
            result = p.map(legal_Classifier, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("Classification start for Ecs Classifier")
        with Pool() as p:
            result = p.map(Ecs_Classifier, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("Classification start for cheque bounce Classifier")
        with Pool() as p:
            result = p.map(Cheque_Classifier, [[df, result, user_id, max_timestamp, new]])[0]['result']

        logger.info("extra classifier called")
        with Pool() as p:
            result = p.map(extra, [[df, result, user_id, max_timestamp, new]])[0]['result']

    except BaseException as e:
        return {"status": False, "message": "Error in Classifier - " + str(e)}
    finally:
        return {"status": True, "message": "success"}
