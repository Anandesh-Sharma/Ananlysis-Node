import re
import threading
from Util import conn,read_json,convert_json
from tqdm import tqdm


def check_body_1(df, pattern):
    d = []
    for index, row in df.iterrows():
        matcher = re.search(pattern, row["body"].lower())
        if (matcher != None):
            d.append(index)
    return d


def check_body_2(df, pattern, required_rows):
    d = []
    for index, row in df.iterrows():
        if index not in required_rows:
            continue
        matcher = re.search(pattern, row["body"].lower())
        if matcher is not None:
            d.append(index)
    return d


def check_header(df, pattern, required_rows):
    d = []
    for index, row in df.iterrows():
        if index not in required_rows:
            continue
        if (pattern in row["sender"].lower()):
            d.append(index)
    return d


def thread_for_cleaning_1(df, pattern, result):
    result.append(check_body_1(df, pattern))


def thread_for_cleaning_2(df, pattern, result, required_rows):
    result.append(check_body_2(df, pattern, required_rows))


def thread_for_cleaning_3(df, pattern, result, required_rows):
    result.append(check_header(df, pattern, required_rows))


def cleaning(df, result, name):
    transaction_patterns = ['debited', 'credited']
    thread_list = []
    results = []
    length = set(range(df.shape[0]))
    required_rows = set(range(df.shape[0]))
    for pattern in transaction_patterns:
        thread = threading.Thread(target=thread_for_cleaning_1, args=(df, pattern, results))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in tqdm(thread_list):
        thread.join()

    for i in results:
        length = length - set(i)

    required_rows = list(set(required_rows) - set(length))

    cleaning_transaction_patterns_header = ['vfcare',
                                            'oyorms',
                                            'payzap',
                                            'rummy',
                                            'polbaz',
                                            '600010',
                                            'rummyc',
                                            'rupmax',
                                            'ftcash',
                                            'dishtv',
                                            'bigbzr',
                                            'olamny',
                                            'bigbkt',
                                            'olacab',
                                            'urclap',
                                            'ubclap',
                                            'qeedda',
                                            'myfynd',
                                            'gofynd',
                                            'paytm',
                                            'airbnk',
                                            'phonpe',
                                            'paysns',
                                            'fabhtl',
                                            'spcmak',
                                            'cuemth',
                                            'zestmn',
                                            'pcmcmh',
                                            'dlhvry',
                                            'bludrt',
                                            'airtel',
                                            'acttvi',
                                            'erecharge',
                                            'swiggy',
                                            'fpanda',
                                            'simpl',
                                            'mytsky',
                                            'vodafone',
                                            'sydost',
                                            'ipmall',
                                            'quikrr',
                                            'mytsky',
                                            'lenkrt',
                                            'flpkrt',
                                            'epfoho',
                                            'flasho',
                                            'grofrs',
                                            'hdfcsl',
                                            'idhani',
                                            'adapkr',
                                            'ipmall',
                                            'oxymny',
                                            'jionet',
                                            'kissht',
                                            '155400',  # m-pesa
                                            'kredtb',
                                            'shoekn',
                                            'lzypay',
                                            'mobikw',
                                            'notice',
                                            'payltr',
                                            'salary',
                                            'swiggy',
                                            'vishal',
                                            'qira',
                                            'domino',
                                            'dinout',
                                            'quikrd',
                                            'goibib',
                                            'cureft',
                                            'olacbs',
                                            'ryatri',
                                            'dhanip',
                                            'zestmo',
                                            'smart']
    garbage_header_rows = []
    thread_list = []
    results = []

    for pattern in cleaning_transaction_patterns_header:
        thread = threading.Thread(target=thread_for_cleaning_3, args=(df, pattern, results, required_rows))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in tqdm(thread_list):
        thread.join()

    for i in results:
        garbage_header_rows.extend(i)

    required_rows = list(set(required_rows) - set(garbage_header_rows))

    g = []
    for index, row in df.iterrows():
        if index not in required_rows:
            continue
        matcher_1 = re.search("[Rr]egards", row["body"])
        matcher_2 = re.search("[a-zA-z]{2}-\d+", row["body"])
        if matcher_1 is not None:
            if 'DHANCO' not in row["sender"]:
                g.append(index)
        elif matcher_2 is not None:
            g.append(index)

    required_rows = list(set(required_rows) - set(g))

    cleaning_transaction_patterns = ['request received to', 'received a request to add', 'premium receipt',
                                     'contribution',
                                     'data benefit', 'team hr', 'free [0-9]* ?[gm]b', ' data ', 'voucher', 'data pack',
                                     'benefit of ', 'data setting', 'added/ ?modified a beneficiary',
                                     'added to your beneficiary list', 'after activation', 'new beneficiary',
                                     'refund credited', 'return request', 'received request',
                                     'documents have been received',
                                     'last day free', 'received a refund', 'will be processed shortly',
                                     'credited a free',
                                     'request for modifying', 'free \d* [gm]b/day', 'data pack',
                                     'request for registration',
                                     'received by our company', 'month of', 'received a call', 'free data',
                                     'data benefits',
                                     'received full benefit', 'payment against', 'auto debited', 'mandates',
                                     'we apologize for the incorrect sms',
                                     'coupon', 'can be credited ', 'no hassle of adding beneficiary', 'you\'re covered',
                                     'bank will never ask you to', 'eAadhaar', 'great news!', 'your query has',
                                     'redemption request', 'number received',
                                     'your order', 'beneficiary [a-z]*? is added successfully', 'dear employee',
                                     'subscribing', 'sorry',
                                     'received \d*? enquiry', 'congratulations?', 'woohoo!', 'salary credited', 'hurry',
                                     'sign up', 'credited to your wallet', 'safe & secure!', '[gm]b is credited on',
                                     'cash reward',
                                     'remaining emi installment', 'salary amount', 'incentive amount ', 'dear investor',
                                     'verification code', 'outstanding dues', 'congrat(ulation)?s', 'available limit ',
                                     'oyo money credited',
                                     'reminder', 'card ?((holder)|(member))', 'login request', 'cashback',
                                     'electricity bill', 'data pack activation',
                                     'paytm postpaid bill', 'failed', 'declined', 'cardmember', 'credit ?card',
                                     ' porting ', 'lenskart',
                                     'activated for fund transfer', 'biocon', 'updated wallet balance', 'recharging',
                                     'assessment year', 'we wish to inform', 'refunded',
                                     'amendment', 'added/modified', 'kyc verification', 'is due', 'paytm postpaid',
                                     'please pay', 'flight booking', 'offer',
                                     '(credited)?(received)? [0-9]*[gm]b', 'payment.*failed',
                                     'uber india systems pvt ltd', 'has requested money', 'on approving',
                                     'not received', 'received your', 'brand factory has credited ', 'train ticket',
                                     'total (amt)?(amount)? due', 'redbus wallet', 'otp',
                                     'due of', 'received ?a? ?bill', 'successful payments', 'response ', 'last day',
                                     'payment confirmation', 'payment sms', 'kyc',
                                     'added beneficiary', 'received a message', ' premium ', 'claim', 'points ',
                                     'frequency monthly', 'received a pay rise', 'cheque book',
                                     'will be', 'unpaid', 'received (for|in) clearing', 'presented for clearing',
                                     'your application', 'to know', 'unpaid',
                                     'thanking you', 'redeem', 'transferred', 'available credit limit']

    garbage_rows = []
    thread_list = []
    results = []

    for pattern in cleaning_transaction_patterns:
        thread = threading.Thread(target=thread_for_cleaning_2, args=(df, pattern, results, required_rows))
        thread_list.append(thread)

    for thread in thread_list:
        thread.start()

    for thread in tqdm(thread_list):
        thread.join()

    for i in results:
        garbage_rows.extend(i)

    required_rows = list(set(required_rows) - set(garbage_rows))

    if name in result.keys():
        a = result[name]
        a.extend(list(required_rows))
        result[name] = a
    else:
        result[name] = list(required_rows)
    mask = []
    for i in range(df.shape[0]):
        if i in required_rows:
            mask.append(True)
        else:
            mask.append(False)
    df_transaction = df.copy()[mask]
    df_transaction = df_transaction.reset_index(drop=True)


    data_transaction = convert_json(df_transaction, name)
    #print('transaction')
    #print(data_transaction)
    client = conn()
    db = client.messagecluster.transaction
    db.insert_one(data_transaction)
    client.close()

