from HardCode.scripts.model_0.scoring.parameters.approval_parameters import get_approval_parameters
from HardCode.scripts.model_0.scoring.parameters.rejected_parameters import get_rejection_parameters


def get_parameters(user_id, cibil_df):
    """
    :returns combines rejection and approval parameters into a single dictionary
    :rtype: dict
    """
    approval_variables, approval_values = get_approval_parameters(user_id, cibil_df)
    rejection_variables, rejection_values = get_rejection_parameters(user_id,cibil_df)

    variables = {
        'approval_variables': approval_variables,
        'rejection_variables': rejection_variables
    }
    values = {
        'approval_parameters': approval_values,
        'rejection_parameters': rejection_values
    }

    return variables, values
