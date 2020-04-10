from HardCode.scripts.model_0.scoring.parameters.approval_parameters import get_approval_parameters
from HardCode.scripts.model_0.scoring.parameters.deduction_parameters import get_deduction_parameters


def get_parameters(user_id, cibil_df):
    """
    :returns combines rejection and approval parameters into a single dictionary
    :rtype: dict
    """
    approval_variables, approval_values = get_approval_parameters(user_id)
    deduction_variables, deduction_values = get_deduction_parameters(user_id,cibil_df)

    variables = {
        'approval_variables': approval_variables,
        'deduction_variables': deduction_variables
    }
    values = {
        'approval_parameters': approval_values,
        'deduction_checks': deduction_values
    }

    return variables, values
