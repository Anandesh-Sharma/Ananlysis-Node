# from HardCode.scripts.update_analysis import update
# from HardCode.scripts.parameters_for_bl0.parameters_updation import parameters_updation
# from HardCode.scripts.model_0.scoring.generate_total_score import get_score
from HardCode.scripts.rule_based_model.rule_engine import rule_engine_main
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
         'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))), 'cust_id': user_id,"result_type":"before_loan"}
    if client:
        client.analysisresult.exception_bl0.insert_one(r)
    return r


def bl0(**kwargs):
    user_id = kwargs.get('user_id')
    sms_json = kwargs.get('sms_json')
    # cibil_df = kwargs.get('cibil_xml')
    sms_count = len(sms_json)
    # ==> creating logger and checking user_id
    logger = logger_1('bl0', user_id)
    if not isinstance(user_id, int):
        try:
            logger.info("user_id not int converting into int")
            user_id = int(user_id)
            logger.info("user_+id successfully converted into int")
        except BaseException as e:
            return exception_feeder(user_id=-1, msg='user_id has a issue got id' + str(user_id))

    try:
        logger.info('making connection with db')
        client = conn()
    except:
        logger.critical('error in connection')
        return {'status': False, 'message': "Error in making connection.",
                'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata'))),"result_type":"before_loan",'cust_id': user_id}
    logger.info('connection success')

    if sms_count < 400:
        dict_update = {"status": True, "cust_id": user_id, "result": False, "reason": [f"sms_json is {sms_count}"],}
        client.analysisresult.parameters.update_one({'cust_id': user_id}, {"$push": {'parameters-3': dict_update}},
                                                    upsert=True)
        dict_update["result_type"]="before_loan"
        return dict_update

    # ===> Analysis
    # try:
    #     result = update(user_id=user_id, sms_json=sms_json)
    #     if not result['status']:
    #         msg = "sms updation failed due to some reason-" + result['message']
    #         logger.error(msg)
    #         exception_feeder(client=client, user_id=user_id,
    #                          msg=msg)
    #         return result_output_false(msg)
    # except BaseException as e:
    #     msg = "sms updation failed due to some reason-" + str(e)
    #     logger.error(msg)
    #     exception_feeder(client=client, user_id=user_id,
    #                      msg=msg)
    #     return result_output_false(msg)
    # ===> Analysis Complete

    # >>=>> Parameters Updation
    # try:
    #     result_params = parameters_updation(user_id, cibil_df, sms_count)
    #     if not result_params['status']:
    #         msg = "Parameters updation check failed due to some reason-" + result_params['message']
    #         logger.error(msg)
    #         exception_feeder(client=client, user_id=user_id, msg=msg)
    # except BaseException as e:
    #     msg = "Parameters updation failed due to some reason-" + str(e)
    #     logger.error(msg)
    #     exception_feeder(client=client, user_id=user_id, msg=msg)
    # logger.info('Parameters updation complete')

    # # >>=>> Scoring Model
    # try:
    #     result_score = get_score(user_id, sms_count)
    #     if not result_score['status']:
    #         msg = "Scoring Model failed due to some reason"
    #         logger.error(msg)
    #         exception_feeder(client=client, user_id=user_id, msg=msg)
    # except BaseException as e:
    #     msg = "Scoring Model failed due to some reason-" + str(e)
    #     logger.error(msg)
    #     exception_feeder(client=client, user_id=user_id, msg=msg)
    # logger.info('Scoring Model complete')

    # >>=>> Rule Engine
    try:
        rule_engine = rule_engine_main(user_id)
        if not rule_engine['status']:
            msg = "Rule engine failed due to some reason-" + rule_engine['message']
            logger.error(msg)
            exception_feeder(client=client, user_id=user_id, msg=msg)
            rule_engine = {"status": False, "cust_id": user_id, "result": False, "result_type": "before_loan"}
    except BaseException as e:
        msg = "Rule engine failed due to some reason-" + str(e)
        logger.error(msg)
        exception_feeder(client=client, user_id=user_id, msg=msg)
        rule_engine = {"status": False, "cust_id": user_id, "result": False, "result_type": "before_loan"}
    logger.info('Rule engine complete')
    dict_update=rule_engine['dict_update']
    del rule_engine['dict_update']
    client.analysisresult.bl0.update_one({'cust_id': user_id}, {"$push": {'parameters-3': dict_update}}, upsert=True)
    client.close()
    rule_engine[ "result_type"]="before_loan"
    return rule_engine
