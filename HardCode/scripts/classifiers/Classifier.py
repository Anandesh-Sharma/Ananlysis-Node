from .Classifier_CreditCard import credit
from .Classifier_Loan import loan
from .Classifier_transaction import cleaning
from .Classifier_salary import salary
import multiprocessing
from HardCode.scripts.Util import conn, read_json, convert_json, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def extra(df, user_id, result, max_timestamp, new):
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
    manager = multiprocessing.Manager()
    result = manager.dict()

    logger.info("Read sms json object")
    result1 = read_json(sms_json, user_id)
    if not result1:
        logger.error("JSON not read successfully")
        return result1
    df = result1['df']
    new = result1['new']
    max_timestamp = result1['timestamp']
    logger.info("Multiprocessing start for Credit card Classifier")
    try:
        p1 = multiprocessing.Process(target=credit, args=(df, result, user_id, max_timestamp, new,))
    except BaseException as e:
        logger.info(f"error in credit card classifier as {e}")
        return False
    logger.info("Multiprocessing start for Loan Classifier")
    try:
        p2 = multiprocessing.Process(target=loan(df, result, user_id, max_timestamp, new, ))
    except BaseException as e:
        logger.info(f"error in loan classifier as {e}")
        return False
    logger.info("Multiprocessing start for Transaction Classifier")
    try:
        p3 = multiprocessing.Process(target=cleaning(df, result, user_id, max_timestamp, new, ))
    except BaseException as e:
        logger.info(f"error in transaction classifier as {e}")
        return False
    logger.info("Multiprocessing start for Salary Classifier")
    try:
        p4 = multiprocessing.Process(target=salary(df, result, user_id, max_timestamp, new, ))
    except BaseException as e:
        logger.info(f"error in salary classifier as {e}")
        return False
    logger.info("process 1 starts")
    try:
        p1.start()
    except BaseException as e:
        logger.info(f"error in credit card classifier as {e}")
        return False
    logger.info("process 2 starts")
    try:
        p2.start()
    except BaseException as e:
        logger.info(f"error in loan classifier as {e}")
        return False
    logger.info("process 3 starts")
    try:
        p3.start()
    except BaseException as e:
        logger.info(f"error in transaction classifier {e}")
        return False
    try:
        p4.start()
    except BaseException as e:
        logger.info(f"error in salary classifier {e}")
        return False
    try:
        p1.join()
    except BaseException:
        return False
    logger.info("process 1 complete")
    try:
        p2.join()
    except BaseException as e:
        return False
    logger.info("process 2 complete")
    try:
        p3.join()
    except BaseException as e:
        return False
    logger.info("process 3 complete")
    try:
        p4.join()
    except BaseException as e:
        return False
    logger.info("process 4 complete")

    logger.info("extra classifier called")
    extra(df, user_id, result, max_timestamp, new)
    return True
