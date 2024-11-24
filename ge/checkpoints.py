

def SimpleCheckpoint(args):
    name = "SimpleCheckpoint"
    config = {
        "name": name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y_%m_%d_%H_%M_%S-"+name
    }
    return config


def SimpleResultsCheckpoint(args):
    name = "SimpleResultsCheckpoint"
    config = {
        "name": name,
        "config_version": 1,
        "class_name": "Checkpoint",
        "run_name_template": "%Y_%m_%d_%H_%M_%S-"+name,
        "action_list":[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction"
                }
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction"
                }
            }
        ],
        "runtime_configuration": {
            "result_format": {"result_format": "COMPLETE"}
        }
    }
    return config