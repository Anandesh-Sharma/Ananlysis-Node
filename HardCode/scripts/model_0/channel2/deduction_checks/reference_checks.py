from HardCode.scripts.model_0.parameters.deduction_parameters.reference_verification.validation.check_reference import validate
from HardCode.scripts.model_0.parameters.deduction_parameters.relative_verification.relative_validation import rel_validate

def reference_check(user_id):

    reference = validate(user_id)
    relative = rel_validate(user_id)

    # >>==>> reference_check
    reference_check = True
    relatives_check1 = False
    relatives_check2 = False
    relatives_check3 = False

    if reference['status']:
        reference_check = reference['result']['verification']

    if relative['status']:
        if relative['result']['verification']:
            relatives_check1 = True
        else:
            if relative['length'] ==1:
                relatives_check2 = True
            if relative['length'] ==2:
                relatives_check3 = True





    variables = {
        'reference_check':reference_check,
        'relatives_check1':relatives_check1,
        'relatives_check2':relatives_check2,
        'relatives_check3':relatives_check3
    }
    values = {
        'reference': reference,
        'relatives': relative
    }

    return variables,values