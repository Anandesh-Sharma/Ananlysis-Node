from HardCode.scripts.Util import conn,logger_1

def loan_app_percentage(**kwargs):
    """This code gives the loan app percentage in a userl's mobile.

    Parameters:
        cust_id(int)    : id of the user
        app_data(dict)  : a dictionary of the apps user contains and the category of the apps.

    Returns:
        dict : containing-
            status(bool)        : whether code ran successfully
            message(string)     : containing success of error.
            percentage(float)   : if successfull the percentage of app(0-1).
    """
    user_id = kwargs.get("user_id")
    app_data = kwargs.get("app_data")
    logger = logger_1('loan app count', user_id)
    logger.info("function started")
    percentage_of_loan_apps = 0
    if app_data:
        logger.info("app data function")
        try:
            count=0
            for i in app_data:
                # TODO >== prepare a list of loan apps and
                #          check from that instead of using finance keyword

                if i['app__category'] == 'FINANCE':
                    count+=1
            percentage_of_loan_apps = count / len(app_data)
            logger.info("app data function")
        except BaseException as e:
            msg = f"Error in loan app count validation : {e}"
            logger.error(msg)
            return {
            "status":False,
            "message":msg,
            "percentage":0
            }
    else:
        msg = "app data not found"
        logger.error(msg)
        return {
            "status":False,
            "message":"app data not found",
            "percentage":0
        }
    return {"status":True,"message":"success","percentage":round(percentage_of_loan_apps, 2)}