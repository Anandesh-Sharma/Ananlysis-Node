import json
import os
import warnings
from concurrent.futures.thread import ThreadPoolExecutor
from time import time
import requests
from HardCode.scripts.testing.execute.data_fetch import get_sms, get_cibil
# from HardCode.scripts.testing.user_ids import *

warnings.filterwarnings('ignore')
# from tqdm import tqdm


def generate_access_token():
    # ==> this function is used to generate the access token in case if it expires

    url = 'http://localhost:8000/api/token/'

    credentials = {'username': 'iam', 'password': 'iam'}

    res = requests.post(url=url, data=credentials, verify=False)
    res = res.json()
    return res['access']


def execute_bl0(**kwargs):
    user_id = kwargs.get('user_id')
    cibil_score = kwargs.get('cibil_score')
    # cibil_xml = kwargs.get('cibil_file')

    if os.path.exists(os.path.join('../input_data', 'cibil_data_' + str(user_id) + '.xml')):
        cibil_xml = open(os.path.join('../input_data', 'cibil_data_' + str(user_id) + '.xml'))
    else:
        cibil_xml = None
    sms_json = open(os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json'), 'rb')

    new_user = 1
    all_loan_amount = [1000, 2000, 3000, 4000]
    current_loan_amount = 0

    url = 'http://localhost:8000/hard_code/bl0/'
    token = generate_access_token()
    Auth = 'Bearer ' + str(token)
    payload = {
        'user_id': user_id,
        'new_user': new_user,
        'cibil_score': cibil_score,
        'current_loan_amount': current_loan_amount,
        'all_loan_amount': all_loan_amount,

    }
    files = [('sms_json', sms_json), ('cibil_xml', cibil_xml)]
    result = requests.post(url=url, data=payload, files=files, headers={'Authorization': Auth})
    result = result.json()
    # if not os.path.exists('../result'):
    #     os.mkdir('result')

    with open(os.path.join('../result', str(user_id) + '.json'), 'w', encoding='utf-8') as fp:
        json.dump(result, fp, ensure_ascii=False, indent=4)


def testing(user_id):
    if not os.path.exists(os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json')):
        get_sms(user_id=user_id)

    if not os.path.exists(os.path.join('../input_data', 'cibil_data_' + str(user_id) + '.xml')):
        get_cibil(user_id=user_id)

    try:
        if str(user_id).isdigit():
            if os.path.exists(os.path.join('../input_data', 'sms_data_' + str(user_id) + '.json')):

                # ==> cibil score is passed 807 by default

                execute_bl0(user_id=int(user_id), cibil_score=807)

                print(f"result generated successfully : {user_id}")
            else:
                msg = 'sms json does not exists'
                raise BaseException(msg)
        else:
            msg = 'user id must contain only numbers'
            raise BaseException(msg)

    except BaseException as e:
        print(f"the following error occurred : {e}")


# user_id = input('enter user id: ')
# testing(user_id=user_id)


if __name__ == "__main__":
    t = time()
    l = [336116, 336120, 336146, 336209, 336230, 336289, 336317, 336342, 336348, 336355, 336364, 336409, 336419, 336440,
         336495, 336558, 336572, 336599, 336600, 336619, 336664, 336678, 336688, 336698, 336706, 336712, 336737, 336754,
         336799, 336831, 336837, 336848, 336871, 336878, 336885, 336891, 336896, 336899, 336942, 336958, 336971, 336987,
         336994, 337005, 337031, 337037, 337044, 337045, 337070, 337075, 337076, 337079, 337097, 337115, 337117, 337119,
         337139, 337165, 337183, 337185, 337226, 337234, 337257, 337290, 337315, 337324, 337346, 337362, 337435, 337454,
         337473, 337476, 337480, 337490, 337497, 337513, 337520, 337531, 337584, 337586, 337590, 337612, 337623, 337643,
         337651, 337681, 337713, 337729, 337734, 337748, 337752, 337762, 337790, 337791, 337840, 337845, 337852, 337892,
         337923, 337939, 337955, 338012, 338018, 338067, 338071, 338087, 338139, 338148, 338151, 338182, 338227, 338243,
         338288, 338328, 338340, 338373, 338376, 338390, 338422, 338427, 338446, 338477, 338516, 338531, 338542, 338602,
         338611, 338622, 338626, 338653, 338738, 338760, 338766, 338796, 338810, 338829, 338850, 338863, 338871, 338872,
         338890, 338899, 338933, 338941, 338942, 338946, 338963, 338973, 338992, 339016, 339073, 339149, 339159, 339170,
         339225, 339278, 339330, 339348, 339387, 339393, 339398, 339426, 339430, 339436, 339443, 339447, 339480, 339498,
         339511, 339529, 339605, 339619, 339662, 339674, 339686, 339762, 339772, 339780, 339791, 339794, 339800, 339813,
         339833, 339846, 339848, 339872, 339895, 339906, 339961, 339964, 339973, 339981, 339982, 340000, 340002, 340003,
         340022, 340026, 340053, 340054, 340186, 340204, 340226, 340230, 340256, 340375, 340387, 340465, 340526, 340578,
         340583, 340611, 340650, 340655, 340663, 340666, 340685, 340691, 340707, 340733, 340764]

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(testing, l)
    print("All tasks complete")
    # for i in nd_list_files:
    #     processes = list()
    #     for j in i:
    #         p = multiprocessing.Process(target=test_mp_bl0_request, args=(j,))
    #         processes.append(p)
    #         p.start()
    #     for process in processes:
    #         process.join()
    print(time() - t)
