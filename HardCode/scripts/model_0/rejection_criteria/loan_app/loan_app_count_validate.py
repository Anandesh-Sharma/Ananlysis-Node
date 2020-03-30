from HardCode.scripts.model_0.rejection_criteria.loan_app.profile_info import get_reference_details


def loan_app_count(user_id) -> bool:
    app_data = get_reference_details(user_id)
    percentage_of_loan_apps = 200
    try:

        if app_data:
            d = []
            for i in app_data:
                if i['app__category'] == 'FINANCE':
                    d.append(i)
            percentage_of_loan_apps = (len(d) / len(app_data))

            # if percentage_of_loan_apps >= 0.70:
            #     loan_app_count_check = False

    except BaseException as e:
        print(f"Error in loan app count validation : {e}")

    finally:
        return round(percentage_of_loan_apps, 2)
