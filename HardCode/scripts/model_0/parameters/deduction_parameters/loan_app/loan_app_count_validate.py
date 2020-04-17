from HardCode.scripts.model_0.parameters.deduction_parameters.loan_app.profile_info import get_reference_details


def loan_app_count(user_id):
    """
    :returns percentage of loan apps installed
    :rtype: float
    """
    app_data = get_reference_details(user_id)
    percentage_of_loan_apps = 0
    status = False
    try:

        if app_data:
            d = []
            for i in app_data:
                # TODO >== prepare a list of loan apps and
                #          check from that instead of using finance keyword

                if i['app__category'] == 'FINANCE':
                    d.append(i)

            percentage_of_loan_apps = (len(d) / len(app_data))
            status = True

    except BaseException as e:
        print(f"Error in loan app count validation : {e}")

    finally:
        return round(percentage_of_loan_apps, 2) , status
