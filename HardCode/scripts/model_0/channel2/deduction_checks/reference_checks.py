# from HardCode.scripts.model_0.parameters.deduction_parameters.reference_verification.validation.check_reference import validate
# from HardCode.scripts.model_0.parameters.deduction_parameters.relative_verification.relative_validation import rel_validate
from HardCode.scripts.Util import conn
def reference_check(user_id):
    connect = conn()
    parameters = connect.analysis.parameters.find_one({'cust_id': user_id})
    reference = parameters['parameters']['reference']
    relative = parameters['parameters']['relatives']
    no_of_relatives = parameters['parameters']['no_of_relatives']
    # reference = validate(user_id)
    # relative = rel_validate(user_id)

    # >>==>> reference_check
    reference_check = True
    relatives_check1 = False
    relatives_check2 = False
    relatives_check3 = False

    if reference['status']:
        reference_check = reference['result']['verification']


    if relative['verification']:
        relatives_check1 = True
    else:
        if no_of_relatives ==1:
            relatives_check2 = True
        if no_of_relatives ==2:
            relatives_check3 = True




    connect.close()
    variables = {
        'reference_check':reference_check,
        'relatives_check1':relatives_check1,
        'relatives_check2':relatives_check2,
        'relatives_check3':relatives_check3
    }
    values = {
        'reference': reference,
        'relatives': relative,
        'no_of_relatives': no_of_relatives,

    }

    return variables,values