import re
import pandas as pd
from HardCode.scripts.Util import conn, logger_1, convert_json
from datetime import datetime
import pytz


def legal(df, user_id, result):
    logger = logger_1("legal_classifier", user_id)
    logger.info("legal started")
    mask = []
    selected = []
    patterns = [
        r'you\shave\sdefaulted.*legal',
        r'forced.*to\sseek\slegal\scounsel',
        r'receive\slegal\s(?:notice|notifications)\s(?:at|in)\syour\sresidence',
        r'serious\snon\s?[-]?payment\sissues.*legal\saction',
        r'legal\simplications.*seriously\sin\sarrears',
        r'initiating\slegal\saction:\n\n.*served\swith.*legal\sdemand\snotice',
        r'sending\srecovery\steam\sto\syour\splace',
        r'legal\snotice.*going\sto\sbe\sdispatched',
        r'(?:taking|take|initiate[d]?|initiating|started|starting|filed|filing)\slegal\s(?:action|proceedings|case)\sagainst\s(?:you|u)',
        r'on\sthe\sverge\sof\staking\slegal\saction\s?(?:against\syou)?',
        r'loan.*is\s?(?:overdue)?\s(?:seriously|severely)\s?(?:overdue)?',
        r'recovery\sprocess.*started.*overdue\sloan',
        r'legal\sintimation.*sent\son\syour\smail',
        r'taking.*legal\saction\saccording\sby\slaw',
        r'you\sare\sintentionally\sdefaulting',
        r'case\sdetails\scopy.*couriered\sto\syou',
        r'we\sare\sabout\sto\sarrange.*field\srecovery\sagent',
        r'account.*listed\sas\spart\sof\sdefault[e]?[d]?\sbucket',
        r'arranged.*field\srecovery\s(?:agent|agency)',
        r'started.*fraud\sinvestigation\s(?:and|&)\slegal\sproceeding[s]?',
        r'legal\snotice\s?(?:is|has\sbeen)?\s(?:prepared|sent)',
        r'legal\snotice\sis\sgoing\sto\sbe\sdispatched',
        r'collection\s(?:agencies|agency)\s(?:have|has)\s(?:received|recd)\s(?:your|ur)\scase',
        r'your\scase.*(?:forwarded|escalated).*for\scollection\sof.*loan',
        r'you\shave\sbeen\sin\sarrears\swith.*loan\sfor\sa\slong\stime',
        r'started\slegal\sproceedings.*f\.?i\.?r\sis\sfiled',
        r'referred.*to\san\sagent\sto\scontinue\sproceedings',
        r'violated.*loan\scontract\sagreement.*malicious\sdefault',
        r'asset\smanagement\sdepartment\swill\sask.*contacts.*default\sreasons',
        r'no\sother\schoice.*undertake\slegal\saction',
        r'initiated\slegal\saction.*report.*all\scredit\sbureaus',
        r'situation.*critical.*requires\sserious\sattention',
        r'filing\slegal\scase\sagainst\syou.*cheating.*dishonesty',
        r'intimate\syour\sparents.*dispatch.*legal\snotice',
        r'legal\snotice.*already\sregistered\son\syour\sname',
        r'address.*shared\swith.*legal\sdepartment.*legal\snotice\salong\swith.*case\snumber',
        r'your\saccount\sis\sseverely\sdelinquent',
        r'loan\sis\snow\ssuspected\sfraud',
        r'filing.*police\scomplaint\sagainst\syou',
        r'forcing.*contact.*about\syour\snon[-]?\s?payment',
        r'legal\snotice.*case\snumber\s(is|has\sbeen)\sregistered',
        r'already\sdispatched\sthe\slegal\snotice',
        r'loan\s(?:is|has\sbeen)\smoved\sto\slegal\sbucket',
        r'case\smay\sget\sfiled.*due\sto\snon[-]?\s?payment',
        r'successfully\ssent\s?(?:the)?\slegal\snotice',
        r'issued\slegal\snotice.*proceed\sin.*court',
        r'a[a]?pka.*case\sab\sregional\scollections.*ko\sdiya\sgaya\shai',
        r'received\syour\sprofile\sin\s?(?:the)?\sdefaulter\slist',
        r'loan\sis\snow\sin\sdefault',
        r'will\sstart\sto\scall\syour\sfriends.*relatives',
        r'fake\sstatement.*fraud.*submit\syour\sloan\sfile\sto\scourt\sand\spolice\sstation',
        r'call.*reference.*legal\saction',
        r'constrained\sto\stake\s?[a]?\slegal\saction[s]?.*breach\sof\strust',
        r'fail\sto\smake\s?(?:the)\spayment.*legal\saction\swill\sfollow(?:ed)?',
        r'not\sdone.*payment.*forcefully.*take\snecessary\saction',
        r'filing\sof\spolice\scase',
        r'your\scase.*assigned\sto.*legal\sassociate',
        r'legal\saction\swill\sstart\sagains[t]?\syou',
        r'advocate.*appointed.*proceedings.*arbitration\scourt',
        r'legal\snotice\sissued\sto\sregistered\saddress',
        r'legal\saction.*(?:case|initiated)\sagainst\syou',
        r'initiating\syour\sfile\sfor\slegal\s(?:proceedings|action[s]?)',
        r'ghar\spe\slegal\snotice\sbhejunga',
        r'last\schance\sto\sstop\slegal\saction',
        r'will\shave\sto\stake\snecessary\slegal\saction[s]?',
        r'field\sagents\swill\scome.*part\sof\scollections',
        r'legal\simplications.*\n.*seriously\sin\sarrears',
        r'served\swith.*legal\sdemand\snotice',
        r'account.*blocked.*clear\syour\sdues'
    ]

    logger.info("loop started")
    for i, row in df.iterrows():
        message = row['body'].lower()
        for pattern in patterns:
            matcher = re.search(pattern, message)
            matcher1 = re.search('avoid\slegal\saction', message)
            if matcher and not matcher1:
                mask.append(True)
                selected.append(i)
                break
        else:
            mask.append(False)

    if user_id in result.keys():
        a = result[user_id]
        a.extend(list(selected))
        result[user_id] = a
    else:
        result[user_id] = list(selected)

    logger.info("loop ended")
    return df[mask].reset_index(drop=True), df.drop(selected).reset_index(drop=True)


def legal_Classifier(args):
    df = args[0]
    result = args[1]
    user_id = args[2]
    max_timestamp = args[3]
    new = args[4]

    logger = logger_1("Legal function", user_id)
    logger.info("Legal function started")

    legal_messages, df = legal(df=df, result=result, user_id=user_id)

    if legal_messages.empty:
        legal_messages = pd.DataFrame(columns=['body', 'timestamp', 'sender', 'read'])

    data = convert_json(legal_messages, user_id, max_timestamp)

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
        db.legal_msgs.update({"cust_id": int(user_id)}, {"cust_id": int(user_id), 'timestamp': data['timestamp'],
                                                         'modified_at': str(
                                                             datetime.now(pytz.timezone('Asia/Kolkata'))),
                                                         "sms": data['sms']}, upsert=True)
        logger.info("All legal messages of new user inserted successfully")
    else:
        logger.info("Old User checked")
        for i in range(len(data['sms'])):
            db.legal_msgs.update({"cust_id": int(user_id)}, {"$push": {"sms": data['sms'][i]}})
            logger.info("legal sms of old user updated successfully")
        db.legal_msgs.update_one({"cust_id": int(user_id)}, {
            "$set": {"timestamp": max_timestamp, 'modified_at': str(datetime.now(pytz.timezone('Asia/Kolkata')))}},
                                 upsert=True)
        logger.info("Timestamp of User updated")
    client.close()
    return {'status': True, 'result': result}
