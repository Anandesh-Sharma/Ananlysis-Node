from HardCode.scripts.classifiers.Classifier import classifier
from HardCode.scripts.Util import *
import pytz


def run_classifier(**kwargs):
    sms_json = kwargs.get('sms_json')
    user_id = kwargs.get('user_id')

    logger = logger_1('run_classifer', user_id)

    if not isinstance(user_id, int):
        return exception_feeder(user_id=user_id, msg='user_id not int type', logger=logger)
    try:
        logger.info('making connection with db')
        client = conn()
    except BaseException as e:
        logger.critical('error in connection')
        return exception_feeder(user_id=user_id, msg=str(e), logger=logger)

    logger.info('connection success')
    logger.info("checking started")
    logger.info("starting classification")

    # >>=>> CLASSIFIERS
    classifier_result = classifier(sms_json, str(user_id))

    if not classifier_result: # must return bool
        return False
    else:
        return True


def exception_feeder(**kwargs):
    client = kwargs.get('client')
    logger = kwargs.get('logger')
    msg = kwargs.get('msg')
    user_id = kwargs.get('user_id')

    logger.error(msg)
    r = {'status': False, 'message': msg, 'limit': None,
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    if client:
        client.analysisresult.exception_bl0.insert_one(r)
    return r
