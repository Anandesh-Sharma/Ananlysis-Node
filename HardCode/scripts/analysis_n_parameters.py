from HardCode.scripts.update_analysis import update
from HardCode.scripts.parameters_for_bl0.parameters_updation import parameters_updation
from HardCode.scripts.Util import conn, logger_1
import warnings
from datetime import datetime
import pytz

warnings.filterwarnings("ignore")


def exception_feeder(**kwargs):
    client = kwargs.get('client')
    msg = kwargs.get('msg')
    user_id = kwargs.get('user_id')

    logger = logger_1('exception_feeder', user_id)

    logger.error(msg)
    r = {'status': False, 'message': msg,
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    if client:
        client.analysisresult.exception_bl0.insert_one(r)
    return r


def analysis_n_parameters(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')
    cibil_df = kwargs.get('cibil_xml')
    app_data = kwargs.get('app_data')
    contacts = kwargs.get('contacts')
    profile_info = kwargs.get('profile_info')
    sms_count = len(sms_json)

    # ==> creating logger and checking user_id
    logger = logger_1('bl0', user_id)
    if not isinstance(user_id, int):
        try:
            logger.info("user_id not int converting into int")
            user_id = int(user_id)
            logger.info("user_id successfully converted into int")
        except BaseException as e:
            return exception_feeder(user_id=-1, msg='user_id has a issue got id' + str(user_id))

    try:
        logger.info('making connection with db')
        client = conn()
    except:
        logger.critical('error in connection')
        return {'status': False, 'message': "Error in making connection.",
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id}
    logger.info('connection success')


    # ===> Analysis
    try:
        result = update(user_id=user_id,sms_json=sms_json)
        if not result['status']:
            msg = "sms updation failed due to some reason-" + result['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id,
                            msg=msg)

    except BaseException as e:
        msg = "sms updation failed due to some reason-"+str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id,
                        msg=msg)

    # ===> Analysis Complete

    # >>=>> Parameters Updation
    try:
        result_params = parameters_updation(user_id=user_id, cibil_xml=cibil_df, sms_count=sms_count, app_data=app_data,contacts=contacts,profile_info=profile_info)
        if not result_params['status']:
            msg = "Parameters updation check failed due to some reason-" + result_params['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
    except BaseException as e:
        msg = "Parameters updation failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
    logger.info('Parameters updation complete')

    return {'status':True,'message':'success'}
