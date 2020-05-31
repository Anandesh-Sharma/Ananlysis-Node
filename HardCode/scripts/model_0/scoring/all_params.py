from HardCode.scripts.model_0.channel3.additional_checks import get_additional_parameters
from HardCode.scripts.model_0.channel2.deduction_variables import get_deduction_parameters


def get_parameters(user_id):
    """
    :returns combines rejection and approval parameters into a single dictionary
    :rtype: dict
    """

    additional_variables, additional_values = get_additional_parameters(user_id)

    deduction_variables, deduction_values = get_deduction_parameters(user_id)


    variables = {
        'additional_variables': additional_variables,
        'deduction_variables': deduction_variables
    }
    values = {
        'additional_parameters': additional_values,
        'deduction_parameters': deduction_values
    }

    return variables, values
