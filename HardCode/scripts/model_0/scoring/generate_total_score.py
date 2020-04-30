from HardCode.scripts.model_0.scoring.all_params import get_parameters
from HardCode.scripts.model_0.channel3.generate_additional_score import additional_score
from HardCode.scripts.model_0.channel2.generate_rejection_score import deduction_score
from HardCode.scripts.model_0.channel1.rejection_parameters import rejecting_parameters
from HardCode.scripts.Util import conn
from datetime import datetime
import pytz


timezone = pytz.timezone('Asia/Kolkata')


def get_score(user_id, cibil_df,sms_count):


    status = True
    values = {}
    try:
        # >>==>> channel 1
        rejection_reasons = rejecting_parameters(user_id,cibil_df,sms_count)
        if len(rejection_reasons) > 0:
            print("user rejected")


        variables, values = get_parameters(user_id, cibil_df)
        # >>==>> channel 2

        score1, weights1 = deduction_score(variables['deduction_variables'])

        # >>==>> channel 3
        score2 , weights2 = additional_score(variables['additional_variables'])

        score = score1 + score2
        weights1.update(weights2)

    except BaseException as e:
        print(f"Error in scoring model : {e}")
        status = False
    finally:
        model_0 = {
            'rejection_reasons': rejection_reasons,
            'parameters': values,
            'weights_of_parameters': weights1,
            'score': score,
            'modified_at': str(timezone.localize(datetime.now()))
        }

        client = conn()

        result = {'cust_id': user_id, 'Model_0': model_0, 'status': status}
        client.analysis.scoring_model.update({'cust_id': user_id}, {'$push': {'result': model_0}}, upsert=True)
        client.close()
        return result
