from HardCode.scripts.model_0.rejection_criteria.reference_verification.validation.check_reference import validate

def reference_check(user_id):

    reference = validate(user_id)

    # >>==>> reference_check
    reference_check = True
    if reference['status']:
        reference_check = reference['result']['verification']

    variables = {
        'reference_check':reference_check
    }
    values = {
        'reference': reference
    }

    return variables,values