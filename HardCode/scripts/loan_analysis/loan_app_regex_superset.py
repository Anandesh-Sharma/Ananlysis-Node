loan_apps_regex = {
    'CASHBN' : {
        'disbursal' : [r'money\sis\s(?:transferred|disbursed)\sto\syour\saccount'],
        'due' : [r'apka\s(?:loan|emi|installment).*due\s(?:hai|h)',
                 r'apka\sbhugtan.*baki\shai',
                 r'(?:your|ur).*(?:loan|account|emi|installment)\sis\sdue'],
        'overdue' : [r'apka\s(?:loan|emi|installment).*([0-9]+)\s?din\sse\soverdue\shai',
                     r'apka\s(?:loan|emi|installment).*overdue\shai',
                     r'loan\s(?:is|has\sbeen)\sdue\s(?:since|for|from|by)\s([0-9]+)\s?day[s]?',
                     r'last\s(?:day|date)\sto\smake\s?(?:the|your)?\s(?:payment|re[-]?payment)',
                     r'resolve\syour\s(?:outstanding|due)\sloan',
                     r'(?:despite|instead|inspite)\s(?:of)?\s?(?:several|repeated|many|multiple)\sreminder[s]?.*(?:loan|emi)\sis\sstill\sunpaid',
                     r'your\s(?:loan|emi)\sis\soverdue',
                     r'a[a]?pki\sloan\s(?:rashi|amount)\ska\s(?:payment|bhugta[a]?n)\s(?:nahi|nai)\shua\s(?:hai|h)',
                     r'a[a]?j\sa[a]?pka\sa[a]?khri\sdin\s(?:hai|he|h)\spayment\sk[a]?rne\ska',
                     r'your\soutstanding\sloan\s(?:account)?\s(?:is|has)\sstill\snot\spaid',
                     r'payment\sis\snot\sdone\sby\smany\sdays',
                     r"we\s?(?:'ve|have)\s(?:received|got)\s(?:your|ur)\saccount\sfor\shigher\scollection\sactivities"],
        'closed' : [r'your\sloan\sis\snow\spaid\sback',
                    r"we\s?(?:'ve|have)\s(?:finished|done|completed)\sdealing\swith\syour\srepayment",
                    r'loan.*(?:is|has\sbeen)\spaid\s(?:back|off)'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #not original
    },
    'KREDTB' : {
        'disbursal' : [r'your\sloan\s(?:is|has\sbeen)\sdisbursed',
                       r'loan\sof.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\s(?:is|has\sbeen)\ssanctioned'],
        'due' : [r'(?:payment|emi).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'reminder.*regarding\syour\semi\samount',
                 r'cibil\s(?:score)?\sis\snot\simpacted\sby\spaying\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)',
                 r'repay\syour\s(?:emi|loan)\samount\sdue',
                 r'your\s(?:payment|emi)\sis\sdue',
                 r'payback\syour\sdue\samount[s]?\son\stime',
                 r'last\s(?:[0-9]+)\sdays\sto\spay.*emi\son\stime',
                 r'your\s(?:payment|emi)\sdue\sis\s(?:tomorrow|today)',
                 r'loan.*successfully\srescheduled'],
        'overdue' : [r'your\s(?:payment|loan|emi)\sis\soverdue.*amount\srepayable\sis\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r're[-]?\s?payment.*(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*(?:is|its)\sover[-]?\s?due\s?(?:by|for|since)?\s([0-9]+)\s?day[s]?',
                     r'honor\syour\scommitment.*make\s(?:the)?\spayment',
                     r'last\sday\sreminder.*make\s(?:the)?\spayment',
                     r'last\sday\sreminder.*pay.*(?:emi[s]?|loan)\stoday',
                     r'legal\snotice.*listed\sas.*loan\sdefaulter',
                     r'account\shas.*(?:pending|overdue)\samount\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'legal\snotice\salert.*loan.*of\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\sis\soverdue.*(?:since|for|from|by)\s([0-9]+)\s?days',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays',
                     r'pay\s(?:back|soon).*to\sensure\s?(?:that)?\s?[a]?\s([0-9]+)\s?days\sdelay.*not\supdated\sto\scibil',
                     r'clear.*outstanding\s(?:loan|emi|amount).*due\sdate\sis\sover',
                     r'clear.*outstanding\s(?:loan|emi|amount)\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*due\sdate\sis\sover',
                     r"we\shave\s?(?:n't|not)\sreceived.*payment",
                     r'your\sprofile\sis\smoved\sto\slegal\sdepartment',
                     r'(?:despite|instead|in\s?spite)\s(?:of)?\s?(?:several|repeated|many|multiple)\sreminder[s]?.*(?:loan|emi)\sis\sstill\s(?:unpaid|pending)',
                     r'(?:emi|payment|due).*(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*(?:unpaid|pending)',
                     r'defaulter.*pay\simmediately',
                     r'your\saccount\sis\spast\sdue',
                     r'your\saccount\sis\spast\sdue\swith\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'pay\syour\sdues\simmediately',
                     r'pay\syour\sdue\sat\sthe\searliest',
                     r'delayed.*(?:emi|loan|payment)\s(?:for|since|by).*([0-9]+)\sdays',
                     r'(?:emi|loan|payment)\s(?:is|has\sbeen)\sdelayed\s(?:for|since|by)\s([0-9]+)\sdays',
                     r"you\sdid\s?(?:n't|not)\smake.*(?:emi|loan)\spayment",
                     r'your\semi.*still\spending',
                     r'you\shave\stime\still.*make\syour\spayment.*prevent\s([0-9]+)\s?day[s]?\sdelay',
                     r'loan.*delinquency\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\s(?:for|by|since)\s([0-9]+)\sdays',
                     r'(?:loan|emi).*unpaid\sfor\s([0-9]+)\s?day[s]?\soverdue'], #KRBEEE #new
        'closed' : [r'your\sloan.*(?:is|has\sbeen)\sclosed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r"could\s?(?:n't|not)\sapprove\syour\sprofile"]
    },
    'KREDTZ' : {
        'disbursal' : [r'your\sloan\s(?:is|has\sbeen)\sdisbursed',
                       r'loan\sof.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\s(?:is|has\sbeen)\ssanctioned'],
        'due' : [r'(?:payment|emi).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'reminder.*regarding\syour\semi\samount',
                 r'cibil\s(?:score)?\sis\snot\simpacted\sby\spaying\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)',
                 r'repay\syour\s(?:emi|loan)\samount\sdue',
                 r'your\s(?:payment|emi)\sis\sdue',
                 r'payback\syour\sdue\samount[s]?\son\stime',
                 r'last\s(?:[0-9]+)\sdays\sto\spay.*emi\son\stime',
                 r'your\s(?:payment|emi)\sdue\sis\s(?:tomorrow|today)',
                 r'loan.*successfully\srescheduled'],
        'overdue' : [r'your\s(?:payment|loan|emi)\sis\soverdue.*amount\srepayable\sis\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r're[-]?\s?payment.*(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*(?:is|its)\sover[-]?\s?due\s?(?:by|for|since)?\s([0-9]+)\s?day[s]?',
                     r'honor\syour\scommitment.*make\s(?:the)?\spayment',
                     r'last\sday\sreminder.*make\s(?:the)?\spayment',
                     r'last\sday\sreminder.*pay.*(?:emi[s]?|loan)\stoday',
                     r'legal\snotice.*listed\sas.*loan\sdefaulter',
                     r'account\shas.*(?:pending|overdue)\samount\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'legal\snotice\salert.*loan.*of\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\sis\soverdue.*(?:since|for|from|by)\s([0-9]+)\s?days',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays',
                     r'pay\s(?:back|soon).*to\sensure\s?(?:that)?\s?[a]?\s([0-9]+)\s?days\sdelay.*not\supdated\sto\scibil',
                     r'clear.*outstanding\s(?:loan|emi|amount).*due\sdate\sis\sover',
                     r'clear.*outstanding\s(?:loan|emi|amount)\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*due\sdate\sis\sover',
                     r"we\shave\s?(?:n't|not)\sreceived.*payment",
                     r'your\sprofile\sis\smoved\sto\slegal\sdepartment',
                     r'(?:despite|instead|in\s?spite)\s(?:of)?\s?(?:several|repeated|many|multiple)\sreminder[s]?.*(?:loan|emi)\sis\sstill\s(?:unpaid|pending)',
                     r'(?:emi|payment|due).*(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*(?:unpaid|pending)',
                     r'defaulter.*pay\simmediately',
                     r'your\saccount\sis\spast\sdue',
                     r'your\saccount\sis\spast\sdue\swith\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'pay\syour\sdues\simmediately',
                     r'pay\syour\sdue\sat\sthe\searliest',
                     r'delayed.*(?:emi|loan|payment)\s(?:for|since|by).*([0-9]+)\sdays',
                     r'(?:emi|loan|payment)\s(?:is|has\sbeen)\sdelayed\s(?:for|since|by)\s([0-9]+)\sdays',
                     r"you\sdid\s?(?:n't|not)\smake.*(?:emi|loan)\spayment",
                     r'your\semi.*still\spending',
                     r'you\shave\stime\still.*make\syour\spayment.*prevent\s([0-9]+)\s?day[s]?\sdelay',
                     r'loan.*delinquency\sof\s(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\s(?:for|by|since)\s([0-9]+)\sdays'],
        'closed' : [r'your\sloan.*(?:is|has\sbeen)\sclosed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r"could\s?(?:n't|not)\sapprove\syour\sprofile"]
    },
    'LNFRNT' : {
        'disbursal' : [r'loan\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sready\sto\sbe\sdisbursed',
                       r'congratulations\son\syour\sloan\sfrom\sloanfront',
                       r'successfully\sdisbursed\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'loan\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\s(?:is|has\sbeen)\s(?:initiated|disbursed|transferred)'],
        'due' : [r'your\sloan\sid.*is\sdue',
                 r're[-]?payment.*loan.*is\sdue',
                 r'(?:[0-9]+)\sday[s]?\sleft\sto\sre[-]?pay\syour\sloan.*(?:rs[.]?|inr[.]?)\s?([0-9,]+[.]?[0-9]+)\son\stime',
                 r'(?:[0-9]+)\sday[s]?\sleft.*repayment\sof\s(?:rs[.]?|inr[.]?)\s?([0-9,]+[.]?[0-9]+)',
                 r'(?:rs[.]?|inr[.]?)\s?([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue' : [r'make.*(?:repayment|payment)\s\s?(?:of)?\s?(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\s(?:immediately|urgently)',
                     r're[-]?pay\s(?:rs[.]?|inr)\s(?:[0-9,]+[.]?[0-9]+)\s(?:immediately|urgently)',
                     r'due\samount\s(?:is|has)\spending\sof\s(?:rs[.]?|inr[.]?)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'your\saccount\shas.*past\sdue\sbalance\sof\s(?:rs[.]?|inr[.]?)\s?(?:[0-9,]+[.]?[0-9]+).*due\s(?:by|since|from|for)\s([0-9]+)\sday[s]?',
                     r'immediate\sre[-]?payment\sof\s(?:rs[.]?|inr[.]?)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'your\sloan.*is\soverdue\s(?:since|by|for|from)\s([0-9]+)\s?day[s]?.*(?:rs[.]?|inr[.]?)\s?(?:[0-9,]+[.]?[0-9]+)\simmediately',
                     r'due\samount\s(?:is|has)\spending\sof\s(?:rs[.]?|inr[.]?)\s?(?:[0-9,]+[.]?[0-9]+).*(?:since|for|from|by)\s([0-9]+)\sday[s]?',
                     r'loan.*repaid.*successfully\swith\soverdue\sdays\sdelay',
                     r'loan.*(?:[0-9,]+[.]?[0-9]+)\sis\soverdue\sfrom.*([0-9]+)\s?day[s]?'],  #new
        'closed' : [r'received.*(?:payment|repayment)\sof\s(?:rs[.]?|inr[.]?)?\s?([0-9,]+[.]?[0-9]+)',
                    r'loan.*(?:rs[.]?|inr[.]?)\s?([0-9,]+[.]?[0-9]+).*repaid.*successfully'], #new
        'rejection' : [r"could\s?(?:n't|not)\sfind\syou\seligible\sfor\s[a]?\s?loan",
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile"]
    },
    'RRLOAN' : {
        'disbursal' : [r'loan\samount\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\s(?:is|has\sbeen)\s(?:transferred|credited|disbursed)'],
        'due' : [r'(?:repayment|payment)\s(?:of|on).*loan.*is\sdue'],
        'overdue' : [r'missed.*payment\s(?:of|on).*loan.*outstanding\s(?:amount|payment)\sis\s(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'overdue\s(?:for|by|since|from)\s?(?:over)?\s([0-9]+)\s?days.*outstanding\s(?:amount|payment)\sis\s(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'pay\simmediately.*interest.*accrued',
                     r'missed\spayment\salert',
                     r'loan\spayment\soverdue',
                     r'urgent.*payment.*not.*paid',
                     r'loan.*pending.*since.*immediate\saction',
                     r'(?:loan|payment).*still\s(?:unpaid|pending)',
                     r'overdue\spayment\snotice',
                     r'you\shave\san\soverdue',
                     r'repay\syour\soverdue',
                     r'repay\simmediately',
                     r'(?:loam|payment).*overdue.*pay\s(?:now|immediately)'],
        'closed' : [r'(?:thank\s?you|thanks)\sfor\smaking\spayment\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\stowards\syour\sloan'],
        'rejection' : [r"your\s(?:loan|application).*could\s?(?:n't|not)\sget\sapproved"]
    },
    'LOANAP' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\sdisbursed.*amount\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)'], #new
        'due' : [r'loan\spayment.*amount\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #commmon
        'closed' : [r'your.*loan.*(?:is|has\sbeen)\spaid\ssuccessfully.*amount\sis\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)'], #new
        'rejection' : [r'loan\sapplication\sis\srejected']
    },
    'KISSHT' : {
        'disbursal' : [r'(?:amount|loan).*(?:disbursed|transferred).*successfully'],
        'due' : [r'(?:kissht)?\s?loan\sre[-]?payment\sof\s\sis\sdue',
                 r'(?:kissht)?\s?(?:instalment|loan)\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).is\sdue'],
        'overdue' : [r'payment\soverdue.*you\shave\snot\smade.*payment\sof\s(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\son.*due\sdate',
                     r'payment\soverdue.*(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)', #new
                     r'(?:payment|emi|loan)\sof\s(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\sis\soverdue\sby\s([0-9]+)\sdays', #new
                     r'([0-9]+)\sdays\spast\syour\spayment\sdue\sdate'],
        'closed' : [r'received.*payment\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)'], #new
        'rejection' : [r'your\sapplication\s(?:is|has\sbeen)\sdenied']
    },
    'GTCASH' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\sdisbursed.*amounting\sto\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+)\s?(?:rupees)?'],
        'due' : [r'due\sdate\sof\syour\sloan.*(?:payment|repayment).*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)'],
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #not original
        'closed' : [r'your\sloan.*is\spaid\s(?:off|back)'],
        'rejection' : [r"your\sloan\sapplication.*did\s?(?:n't|not)\spass.*eligibility\srequirement[s]?",
                       r'application.*rejected']
    },
    'FLASHO' : {
        'disbursal' : [r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*disbursed'],
        'due' : [r'your.*(?:loan|emi|payment|repayment).*is\s(?:due|pending)',
                 r'your.*(?:loan|emi|payment|repayment).*(?:rs[.]?|inr)\s([0-9,]+[.]?[0-9]+).*due\stomorrow'],
        'overdue' : [r'unpaid\s(?:loan|emi).*pay\simmediately',
                     r'(?:loan|emi|payment|repayment)\sis\soverdue'],
        'closed' : [r'(?:repayment|payment)\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\s(?:is|has\sbeen).*received'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #not original
    },
    'CSHMMA' : {
        'disbursal' : [r'receipt\sreminder.*loan\s(?:has\sbeen|is)\s(?:disbursed|transferred)'],
        'due' : [r'repayment\sreminder.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                 r'(?:[0-9]+)\s?days\s(?:later|after)\s(?:is|will\sbe).*(?:bill|loan|emi|payment)\sday',
                 r'expiration\sreminder.*(?:rs[.]?|inr)\s([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue' : [r'overdue\sreminder.*(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*([0-9]+)\s?day[s]?'], #new
        'closed' : [r'successful\srepayment.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r'loan\sapplication\shas\snot\spassed\sthe\sreview']
    },
    'FRLOAN' : {
        'disbursal' : [r'loan\s(?:is|has\sbeen)\s(?:disbursed|transferred).*amount.*(?:rs[.]?[:]?|inr)\s?([0-9,]+[.]?[0-9]+)'],
        'due' : [r'due\sdate\sof\syour\s(?:loan|emi|payment).*repay\son\stime',
                 r'make\s?(?:the)?\s(?:payment|repayment)\stoday'],
        'overdue' : [r'(?:loan|emi|payment)\sis\s([0-9]+)\sdays\soverdue',
                     r'payment.*(?:yet|still)?not\s?(?:yet|still)?.*received',
                     r'loan.*overdue\s(?:for|from|since|by)\s([0-9]+)\s?day[s]?'],
        'closed' : [r'loan\s?(?:application)?\sis\sclosed',
                    r'loan\s(?:is|has\sbeen)\s(?:paid|repaid)\ssuccessfully'],
        'rejection' : [r'loan\s?(?:application)?.*rejected']
    },
    'SALARY' : {
        'disbursal' : [r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:credited|transferred|disbursed)'],
        'due' : [r'pay.*due\samount.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due',
                 ],
        'overdue' : [r'(?:repayment|payment).*highly\soverdue',
                     r'pay.*overdue\s(?:payment|amount|emi).*(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'loan\saccount.*forwarded.*for\scollection.*overdue\samount',
                     r'(?:loan|emi)\s(?:repayment|payment).*(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)\s(?:is|has\sbeen)\soverdue'
                      ],
        'closed' : [r'successfully\sreceived.*payment\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                    r'(?:payment|repayment)\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\sreceived',
                    r'received\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*towards\s(?:repayment|payment)'],
        'rejection' : [r'your\sloan\snot\sapproved',
                       r'your\searlysalary\sapplication\scan\s?not\sbe\sprocessed',
                       ]
    },
    'WFCASH' :{
        'disbursal' : [r'applied\sloan\s(?:is|has\sbeen)\smade\ssuccessfully.*amount\sof\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+)'],
        'due' : [r'repayment\sdate\s(?:is)?(?:[a-z]{3}\s[0-9]+\s[0-9]+).*due\samount\sis\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+)',
                 r"today\sis.*loan(?:'s)?\sdue\sdate.*due\samount\sis\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+)"],
        'overdue' : [r'loan\sis\s(?:on)?\sdue\ssince\s(?:[0-9]+[-][0-9]+[-][0-9]+).*due\samount\sis(?:rs[.]?|inr)?\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'(?:repayment|payment)\sdate\swas\s(?:[0-9]+[-][0-9]+[-][0-9]+).*due\samount\sis\s(?:rs[.]?|inr)?\s?(?:[0-9,]+[.]?[0-9]+)'],
        'closed' : [r'successfully\srepaid\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r'application\shas\snot\sbeen\sapproved']
    },
    'NIRAFN' : {
        'disbursal' : [r'(?:would|will)\s(?:disburse|transfer).*loan',
                       r'loan.*will\sbe\sdisbursed'],
        'due' : [r'(?:emi|loan)\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\sfor\syour\snira\sloan\sis\sdue',
                 r'your\snira\s(?:loan)?\s?emi\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*is\sdue',
                 r'upcoming\semi.*due\sdate\sof\s(?:[0-9]+(?:\/|\-)[0-9]+(?:\/|\-)[0-9]+)'],
        'overdue' : [r'overdue\semi\sof\s(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'you\shave\smissed.*emi\s(?:of)?(?:rs[.]?|inr)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'not\sreceived\syour\snira\semi',
                     r'you\shave\smissed.*(?:emi|installment)\son\sdue\sdate',
                     r'your.*repayment\sis\sstill\spending',
                     r'your\spayment\sdate\salready\scrossed',
                     r'missed.*installment.*pay\sback'],
        'closed' : [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #not original
        'rejection' : [r'sorry.*we\scan\s?not\sprovide\syou.*loan\swith\snira\sthis\stime']
    },
    'QCRDIT' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\sinitiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.?]|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #not original
        'due' : [r'loan.*has\sexhausted\s(?:[0-9]+)\sdays\so\/s\s?([0-9,]+[.]?[0-9]+)',
                 r'grace\speriod.*loan\s.*going\sto\send'],
        'overdue' : [r'loan\s(?:payment|repayment)\sdeadline.*already\sexhausted',
                     r'(?:inspite|despite).*(?:several|many|repeated|multiple)\scalls.*loan.*still\sunpaid',
                     r'loan.*is\soverdue.*extension.*not\swork\sin\syour\sfavo[u]?r',
                     r'(?:inspite|despite).*(?:several|multiple|repeated|many)\sattempts.*no\sresponse\sfrom\syour\send'],
        'closed' : [r'loan\s(?:is|has\sbeen)\scleared\sfor\so\/s\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #not original
    },
    'UPWARD' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #not original
        'due' : [r'today\sis\syour\supwards\sloan\semi\sdue\sdate',
                 r'emi\spayment\sfor\supwards\sloan\sis\sdue'],
        'overdue' : [r'(?:despite|inspite).*(?:several|repeated|many|muliple)\sreminders.*you\swish\sto\scontinue\sto\sdefault\swith.*dues',
                     r'field.*recovery.*to\srecover.*over[-]?due\samount',
                     r'final\sreminder\sto\sremit\syour\sdues',
                     r'your\saccount\sis\slisted\sas\spart\sof\sdefault(?:ed|er[s]?)?\sbucket',
                     r'we\sfailed\sto\sreceive\syour\sfunds',
                     r'(?:emi|loan).*still\snot\sreceived.*make.*payment\simmediately',
                     r'(?:loan|emi|payment)\sis\soverdue'], #new
        'closed' : [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #not original
        'rejection' : [r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'loan.*(?:rejected|declined)',
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #not original
    },
    'LOANIT' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #not original
        'due' : [r'today\sis\syour\sdue\sdate',
                 r'your.*loan\shas\sexhausted\s[0-9]+\sdays',
                 r'gentle\sreminder\sto\srepay\syour\sloan',
                 r'repay\syour\sloan\simmediately'],
        'overdue' : [r'your\sloan\shas\scrossed\sthe\sdue\sdate'],
        'closed' : [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #not original
        'rejection' : [r"application\shas\s?(?:n't|not)\sbeen\sapproved",
                       r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #not original
    },
    'ICREDT' : {
        'disbursal' : [r'transferred.*loan\samount\sto\syour.*account',
                       r'loan.*sent\sto\syour.*account'],
        'due' : [r'today\sis.*\srepayment\sdate',
                 r'you\smust\srepay\stoday',
                 r'loan\swill\sbe\soverdue'],
        'overdue' : [r"we\shave\s?(?:n't|not)\sreceived.*your\srepayment",
                     r'your\sloan.*will\sgenerate\soverdue\sfee[s]?',
                     r'not\sreceived\sthe\samount\s(?:of)?(?:rs[.]?|inr)?\s?(?:[0-9,]+[.]?[0-9]+).*due\son\s(?:[0-9]{2}(?:\/|\-)[0-9]{2}(?:\/|\-)[0-9]{4})'],
        'closed' : [r'loan\s(?:is|has\sbeen)\spaid\soff'],
        'rejection' : [r"application\shas\s?(?:n't|not)\sbeen\sapproved",
                      r'loan.*not\s?(?:been)?\sapproved']
    },
    'LENDEN' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #not original
        'due' : [r'repayment\sof\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+).*due',
                 r'(?:loan|emi)\sis\sdue'],
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #not original
        'closed' : [r'your.*loan\sis\sclosed'],
        'rejection' : [r'loan\srequest\scan\s?not\sbe\sprocessed\sat\sthe\smoment']
    },
    'MNYTAP' : {
        'disbursal' : [r'money\stransfer\sof\s(?:rs[.]?|inr)?\s?([0-9,]+[.]?[0-9]+).*successful'],
        'due' : [r'(?:emi|loan|installment)\spayment\sof\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\s(?:due|upcoming)'],
        'overdue' : [r'your\s?(?:loan|emi|installment)?\spayment\s(?:is|has\sbeen)\s?(?:still)?\soverdue',
                     r'your\soverdue\s(?:loan|emi|installment)\sis\s?(?:still)?\sunpaid'],
        'closed' : [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #not original
        'rejection' : [r"your\s(?:loan|application).*could\s?(?:n't|not)\sget\sapproved",
                       r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #not original
    },
    'VIVIFI' : {
        'disbursal' : [r'money.*transfer',
                        r'disbursed'],
        'due' : [r'due.*payment.*(?:inr|rs\.?)\s([0-9,]+[.]?[0-9]+)\s',
                 r'last\sday.*make\spayment',
                 r'min\sdue\s(?:amount|amt\.?)\s(?:inr|rs\.?)\s([0-9,]+[.]?[0-9])',
                 r'due.*(?:inr|rs\.?)\s([0-9,]+[.]?[0-9])\sincluding',
                 r'(?:statement|stmnt).*is\sdue.*(?:minimum|min).*(?:inr|rs\.?)\s([0-9,]+[.]?[0-9]+)'],
        'overdue' : [r'is\soverdue',
                     r'missed.*payment\sof\s(?:inr|rs\.?)\s(?:[0-9,]+[.]?[0-9]+)'],
        'closed' : [r'thank\syou.*payment.*(?:inr|rs\.?)\s([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r'loan\s?(?:application)?.*rejected']
    },
    'SHUBLN' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due' : [r'emi\sof\s(?:inr|rs\.?)\s([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue' : [r"you\shave\s?(?:n't|not)\scleared\syour\soverdue"],
        'closed' : [r'loan\s?(?:application)?\sis\sclosed'], #new
        'rejection' : [r"unfortunately\syou\sdo\s?(?:n't|not)\squalify\sfor\s?[a]?\sloan"] #new
    },
    'PAYMIN' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due' : [r'(?:repayment|payment).*of\s(?:inr|rs\.?)\s([0-9,]+[.]?[0-9]+).*is\sdue'], #new
        'overdue' : [r'despite\s(?:several|many|repeated)\sreminders.*loan.*is\s?(?:still)?\s(?:outstanding|pending|due)', #new
                     r'reminder.*overdue\s(?:from|for|by|since)\s([0-9]+)\s?day[s]?', #new
                     r'make\s?(?:the)?\spayment\stoday.*legal\saction'],#new
        'closed' : [r'payment\sof\s(?:inr|rs\.?)\s([0-9,]+[.]?[0-9]+)\sreceived'], #new
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'HOMECR' : {
        'disbursal' : [],
        'due' : [r'loan.*(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*emi.*(?:due|dey)\shai'],
        'overdue' : [r'loan.*bakaya\shai.*late\sfee[s]?\ske\ssath\sjama\skarwaye',
                     r'(?:emi|loan).*overdue\shai'],
        'closed' : [r'loan.*kisht\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\ska\sbhugta[a]?n\skarne\ske\sliye\sdhany[a]?wa[a]?d'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'NOTICE' : {
        'disbursal' : [r'loan\sdisbursement\swas\ssuccess.*(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)'], #new
        'due' : [r'loan\s(?:will|is)\sdue',        #new
                 r'loan.*approved.*due\sdate\sis\s(?:[0-9]+[-][0-9]+[-][0-9]+).*(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)'], #new
        'overdue' : [r'loan.*overdue\s?(?:for)?\s([0-9]+)\sday[s]?'], #new
        'closed' : [r'payment\sreceived.*loan.*closed\ssuccessfully'], #new
        'rejection' : [r'loan\s?(?:application)?.*rejected'] #common
    },
    'BRANCH' : {
        'disbursal' : [r'sent\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\sto\syour\s(?:a\/c|account)',
                       r'loan\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*sent'],
        'due' : [r'(?:loan|emi|installment|payment)\sis\sdue',
                 r'loan.*(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*due\sin\s[0-9]+\s?day[s]?'],
        'overdue' : [r'(?:repayment|payment|loan|emi).*overdue\s(?:by|with|for|from)\s([0-9]+)\s?day[s]?',
                     r'([0-9]+)\s?days\soverdue.*branch\sloan',
                     r'(?:repayment|payment)\sis\snow\soverdue',
                     r'loan\spayment\soverdue',
                     r'missed\spayment\salert'],
        'closed' : [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'STHFIN' : {
        'disbursal' : [r'loan\samount\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*disbursed'],
        'due' : [r'pay\s(?:your|ur).*loan\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*avoid\slate\spayment',
                 r'(?:repayment|payment).*loan\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue' : [r'(?:immediate|urgent)\s(?:repayment|payment|pmnt)\sdue',
                     r'pay\s(?:urgently|immediately)',
                     r'loan\sof\s(?:inr|rs\.?)\s?(?:[0-9,]+[.]?[0-9]+).*still\soverdue',
                     r'waived\sall\s(?:your|ur)\soverdue\scharges'],
        'closed' : [r'received\spayment\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'BAJAJF' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*?:is|has\sbeen)\s(?:credited|transferred|disbursed)'], #common
        'due' : [r'(?:emi|payment|amount|loan|instal[l]?ment)\s?(?:of)?\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*is\sdue',
                 r'(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*is\sdue'],
        'overdue' : [r'(?:emi|installment).*(?:inr|rs\.?)\s?(?:[0-9,]+[.]?[0-9]+).*still\sunpaid',
                     r'(?:emi|installment).*still\sunpaid.*pay\soverdue',
                     r'emi.*(?:rs\.?|inr)\s?(?:[0-9,]+[.]?[0-9]+).*not\spaid.*pay\soverdue',
                     r'pay\s?(?:your)?\soverdue\s(?:emi|installment)\sof\s(?:inr|rs\.?)\s?(?:[0-9,]+[.]?[0-9]+)',
                     r'overdue\sof\s(?:inr|rs\.?)\s?(?:[0-9,]+[.]?[0-9]+).*still\sunpaid'],
        'closed' : [r'thanks\sfor\s?(?:the)?\spayment\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)',
                    r'your\sloan.*is\sclosed',
                    r'(?:repayment|payment)\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*received',
                    r'(?:received|rcvd)\spayment\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+).*loan\s(?:a\/c|account)',
                    r'loan.*stands\sclosed',
                    r'(?:thank\syou|thanks)\sfor\spayment.*(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\stowards\sloan'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'NANOCD' : {
        'disbursal' : [r'money\sis\s(?:transferred|disbursed)\sto\syour\saccount'],
        'due' : [r'(?:your|ur)\sloan\sis\sdue'],
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed' : [r'loan\sis\snow\spaid\sback'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'ZESTMN' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #common
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays', #common
                     r'settle\syour\sdues.*legal\saction',
                     r'you\shave\snot\spaid\sdues.*pay\surgently'], 
        'closed' : [r'you\sare\sthe\sbest.*(?:repayment|payment).*successful'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'LOANTP' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due' : [r'instal[l]?ment\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue' : [r'legal\s(?:intimation|notice)\sfor\snon\s?[-]?payment'],
        'closed' : [r'loan.*now\sclosed',
                    r'closure\sdetails.*sent'],
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    'MCreds' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due' : [r'loan\spayment.*amount\s([0-9,]+[.]?[0-9]+)\s?(?:inr|rs\.?)\sis\sdue'],
        'overdue' : [r'loan\spayment.*(?:is|has\sbeen)\soverdue'],
        'closed' : [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common
    },
    # 'CASHEB' : {
    #     'disbursal' : [r'loan.*(?:disbursed|transferred|sent)',
    #                    r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:initiated|disbursed|transferred|sent)',
    #                    r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
    #                    r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:credited|transferred|disbursed|sent)'], #common
    #     'due' : [r'emi\sis\sdue\sfor\spayment'],
    #     'overdue' : [r'you\shave\spassed\syour\sdue\sdate',
    #                  r'you\shave.*overdue\soutstanding'],
    #     'closed' : [r'loan.*(?:closed|repaid|paid)',
    #                 r'payment\sreceived.*loan.*closed\ssuccessfully',
    #                 r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
    #     'rejection' : [r'loan.*(?:rejected|declined)',
    #                    r"could\s?(?:n't|not)\sapprove\syour\sprofile",
    #                    r'cannot\sprovide\syou\s?[a]?\sloan',
    #                    r'unable\sto\sprocess\syour\sapplication',
    #                    r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
    #                    r'sorry.*not.*suitable\sloan\soffer'] #common
    'ABCFIN' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #common
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed' : [r'received payment\sof\s(?:inr|rs\.?)\s?([0-9,]+[.]?[0-9]+)\s'],
        'rejection' : [r'regret\sto\sinform\syou.*loan.*closed']
    },
    'PAYMEI' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'loan.*amount\s?(?:rs\.?|inr)?\s([0-9,]+[.]?[0-9]+)\sis\sdue',
                r'loan.*amount.*(?:rs\.?|inr)?\s([0-9,]+[.]?[0-9]+)\sis\sdue'],
        'overdue': [r'loan\saccount\s?(?:is)?still\sdue',
                    r'([0-9]+)\sdays.*loan\sis\sdue.*pay.*overdue\simmediately',
                    r'due\sdate\sis\sover'],
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'CFLOAN' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #common
        'overdue': [r'(?:repayment|payment).*is\soverdue\s(?:by|with|since|for|from)\s([0-9]+)\s?day[s]?'],
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'CAPFLT' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due',
                r'(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\swill\sbe\sauto\s?[-]?debited.*against\syour\sdues'], #common
        'overdue': [r'repayment\soverdue',
                    r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?'],
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'ICASHE' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed)'], #common
        'due': [r'emi\sof\s?(?:rs\.?|inr)?\s([0-9,]+[.]?[0-9]+).*is\sdue',
                r'make.*payment[s]?.*before.*due\sdate',
                r'pay\sbefore.*extended\sdue\sdate',
                r'emi\sis\sdue\sfor\spayment'],
        'overdue': [r'loan.*severely\soverdue',
                    r'you\shave\spassed\syour\sdue\sdate',
                    r'you\shave.*overdue\soutstanding'],
        'closed': [r'received.*payment\sof\s?(?:rs\.?|inr)?\s\s?([0-9,]+[.]?[0-9]+)'],
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'LOANXP' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #common
        'overdue': [r'loan.*is\soverdue\s?(?:by|since|for|from)?\s([0-9]+)\s?day[s]?',
                    r'loan.*\sis\soverdue\sseriously'],
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'PAYSNS' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'emi.*is\sdue'],
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:is|has\sbeen)\s(?:rejected|declined)']

    },
    'RAPIDR' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'[0-9]+\s?day[s]?\sto\sdue\sdate',
                r'loan\sto\sbe\srepaid\sin\s[0-9]+\s?day[s]?',
                r'[0-9]+\sday[s]?\sleft\sto\sdue\sdate',
                r'loan.*is\sdue'],
        'overdue': [r'loan\sis\s([0-9]+)\s?day[s]?\soverdue'],
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'CBTEST' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #coomon
        'overdue': [r'loan.*seriously\soverdue',
                    r'(?:loan|bill)\sis\s([0-9]+)\s?day[s]?\soverdue',
                    r'(?:loan|bill)\sis\soverdue'],
        'closed': [r'repaid\s([0-9,]+[.]?[0-9]+)\srupees.*loan.*settled'],
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'DHANII' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'emi\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*is\sdue',
                r'(?:requested|request).*payment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)',
                r'pay.*emi\son\sdue\sdate'],
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)',
                    r'(?:repayment|payment)\stowards\syour\sloan.*received'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'RSLOAN' : {
        'disbursal': [r'(?:rs\.?|inr\.?)\s([0-9,]+[.]?[0-9]+).*successfully\s(?:transferred|disbursed)'],
        'due': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #common
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan.*fully\srepaid'],
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'RUPBUS' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'loan\s(?:will\sbe|is)\sdue.*amount\sis\s?(?:rs\.?|inr)?\s([0-9,]+[.]?[0-9]+)',
                r'loan\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:will\sbe|is)\sdue'],
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan\s(?:is|has\sbeen)\spaid\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'],
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'CKCASH' : {
        'disbursal': [r'([0-9,]+[.]?[0-9]+)\sloan\s(?:is|has\sbeen)\stransferred'],
        'due': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due'], #common
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'LLNBRO' : {
        'disbursal': [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred|sent)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)'], #common
        'due': [r'loan.*is\sdue\sshortly',
                r'due\sdate.*is\snearing'],
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'CASHBS' : {
        'disbursal': [r'money\sis\s(?:transferred|disbursed)\sto\s(?:your|you)\saccount'],
        'due': [r'loan\sis\sdue'],
        'overdue': [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sover[-]?due',
                     r'(?:loan|emi|payment).*over[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays'], #common
        'closed': [r'loan.*(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'], #common
        'rejection': [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer'] #common

    },
    'OTHER' : {
        'disbursal' : [r'loan.*(?:is|has\sbeen)\s(?:disbursed|transferred)',
                       r'loan.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:initiated|disbursed|transferred|sent)',
                       r'loan.*disbursed.*(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+)',
                       r'(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*(?:is|has\sbeen)\s(?:credited|transferred|disbursed|sent)',
                       r'fund\shas\sbeen\sdisbursed'],  # common
        'due' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\sdue',
                 r'(?:payment|emi|loan).*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'repayment.*of\s(?:rs[.]?|inr)\s?([0-9,]+[.]?[0-9]+).*due',
                 r'loan\s?(?:amount)?\s([0-9,]+[.]?[0-9]+)\sis\sdue',
                 r'due\sdate\sis\s?(?:coming)?',
                 r'(?:today|tomorrow)\sis\s?(?:the)?\sdue\sdate',
                 r'loan\swill\s?(?:be)?\sdue',
                 r'(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+).*is\sdue',
                 r'loan\sis\sdue.*(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'],  # common
        'overdue' : [r'(?:loan|emi|payment).*(?:is|has\sbeen)\s?(?:in)?\sover\s?[-]?due',
                     r'(?:loan|emi|payment).*over\s?[-]?due.*(?:by|for|since|from|with)\s([0-9]+)\s?day[s]?',
                     r'loan\semi\sis\sdue\s(?:from|since)\s\s?([0-9]+)\sdays',
                     r"overdue.*has\s?(?:n't|not)?\sprocessed",
                     r'loan.*passed.*due\sdate\sof\s([0-9]+)\s?day[s]?',
                     r'loan\s(?:is|has)\sexpired',
                     r'pay\simmediately.*interest.*accrued',
                     r'missed\spayment\salert',
                     r'loan\spayment\soverdue',
                     r'urgent.*payment.*not.*paid',
                     r'loan.*pending.*since.*immediate\saction',
                     r'(?:loan|payment).*still\s(?:unpaid|pending)',
                     r'overdue\spayment\snotice',
                     r'you\shave\san\soverdue',
                     r'repay\syour\soverdue',
                     r'repay\simmediately',
                     r'(?:loam|payment).*overdue.*pay\s(?:now|immediately)',
                     r'loan\sis\s([0-9]+)\s?day[s]?.*overdue'],  # common
        'closed' : [r'loan.*(?:is|has\sbeen)\s(?:closed|repaid|paid)',
                    r'payment\sreceived.*loan.*closed\ssuccessfully',
                    r'received\spayment\sof\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)',
                    r'loan.*repaid\ssuccessfully.*repayment\samount\sis\s(?:rs\.?|inr)\s?([0-9,]+[.]?[0-9]+)'],  # common
        'rejection' : [r'loan.*(?:rejected|declined)',
                       r"could\s?(?:n't|not)\sapprove\syour\sprofile",
                       r'cannot\sprovide\syou\s?[a]?\sloan',
                       r'unable\sto\sprocess\syour\sapplication',
                       r'application\s(?:can\s?not|could\snot)\sbe\sprocessed',
                       r'sorry.*not.*suitable\sloan\soffer',
                       r'loan.*not\sapproved',
                       r'loan\sapplication\sfailed',
                       r'loan.*not\spass.*eligibility\srequirements']  # common
    }


}

bank_headers = [
    'KTKBNK' , 'KOTKBK', 'FEDBNK','ICICIB','OBCBNK','EMPBNK','INDBNK', 'QZHDFC', 'YESBNK', 'HDFCBN', 'KBLBNK', 'HDFCBK', 'CANBNK',
    'SYNBNK', 'ICICBK', 'HDFCPR', 'HDFCPL', 'ICICBK', 'AXISBK', 'KOTAKB', 'QLHDFC', 'VRHDFC','INDUSB'
]








