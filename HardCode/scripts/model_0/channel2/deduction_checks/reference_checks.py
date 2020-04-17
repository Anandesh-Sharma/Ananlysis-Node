from HardCode.scripts.model_0.parameters.deduction_parameters.reference_verification.validation.check_reference import validate

def reference_check(user_id):

    reference = validate(user_id)

    # >>==>> reference_check
    reference_check = True
    reference_check1 = True
    if reference['status']:
        reference_check = reference['result']['verification']
    else:
        reference_check1 = True

    variables = {
        'reference_check':reference_check,
        'reference_check1': reference_check1
    }
    values = {
        'reference': reference
    }

    return variables,values