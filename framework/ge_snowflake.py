import random
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations import util
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core import ExpectationSuite
from great_expectations.validator.validator import Validator
import json
import checkpoints as CP
import great_expectations as gx
from datetime import datetime, timedelta


class GERuntime:
    def __init__(self):
        print("GE Run Time")


    def _get_project(self):
        result = self.session.sql("select * from dq_project where name = '"+self.project_name+"'").collect()
        if len(result) > 0:
            return result[0]
        raise(Exception(self.project_name + " - project does not exists"))


    def _get_group(self):
        result = self.session.sql("select * from dq_group where name = '"+self.group_name+"'").collect()
        if len(result) > 0:
            return result[0]
        raise(Exception(self.group_name + " - group does not exists"))


    def _get_project_group(self, project, group):
        result = self.session.sql("select * from dq_project_group where project_id = "+str(project['ID'])+" and group_id = "+str(group['ID'])).collect()
        if len(result) > 0:
            return result[0]
        raise(Exception("Group is not connected to the give project"))


    def _get_rules_in_group(self, group):
        result = self.session.sql("select * from dq_group_rule where group_id="+str(group['ID'])).collect()
        if len(result) > 0:
            return result
        raise(Exception("Group does not contain any expectations"))  


    def _get_rule_configuration(self, group_rule):
        rules_config = []
        for rule in group_rule:
            rconfig = ExpectationConfiguration(
                expectation_type=rule['RULE'],
                kwargs = json.loads(rule['ARGS']),
                meta={
                    "description": rule['DESCRIPTION'],
                    "severity": rule['SEVERITY']
                }
            )
            rules_config.append(rconfig)
        return rules_config
    
    
    def validate_pandas_with_checkpoint(self, datasource, asset, asset_name, checkpoint_config=None):
        project = self._get_project()
        group = self._get_group()
        project_group = self._get_project_group(project, group)    
        group_rule = self._get_rules_in_group(group)
        context = self.pandas_in_memory_runtime_context()
        context.create_expectation_suite(self.group_name)
        suite: ExpectationSuite = context.get_expectation_suite(expectation_suite_name=self.group_name)
        rules_config = self._get_rule_configuration(group_rule)
        suite.add_expectation_configurations(rules_config)
        context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=self.group_name)
        context.add_datasource(**datasource)
        cp_config = self._get_checkpoint_config("SimpleResultsCheckpoint", {})
        cp_name = "SimpleResultsCheckpoint"

        if checkpoint_config:
            cp_config = self._get_checkpoint_config(checkpoint_config['name'], checkpoint_config['args'])
            cp_name = checkpoint_config['name']
        
        cp_config['expectation_suite_name'] = self.group_name
        context.add_checkpoint(**cp_config)
        batch_request = RuntimeBatchRequest(
            datasource_name=datasource['name'],
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name=asset_name,
            runtime_parameters={"batch_data": asset},
            batch_identifiers={"default_identifier_name": "default_identifier"}
        )
        results = context.run_checkpoint(
            checkpoint_name=cp_name,
            validations = [
                {"batch_request": batch_request}
            ],
        )
        self._write_validation_results(project, group, group_rule, results.to_json_dict(), checkpoint=True)
        return results


    def _get_checkpoint_config(self, name, args):
        func = getattr(CP, name)
        return func(args)


    def pandas_in_memory_runtime_context(self):
        from great_expectations.data_context.data_context.base_data_context import (
            BaseDataContext,
        )
        from great_expectations.data_context.types.base import (
            DataContextConfig,
            InMemoryStoreBackendDefaults,
        )

        data_context_config: DataContextConfig = DataContextConfig(
            datasources={  # type: ignore[arg-type]
                "pandas_datasource": {
                    "execution_engine": {
                        "class_name": "PandasExecutionEngine",
                        "module_name": "great_expectations.execution_engine",
                    },
                    "class_name": "Datasource",
                    "module_name": "great_expectations.datasource",
                    "data_connectors": {
                        "runtime_data_connector": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": [
                                "id_key_0",
                                "id_key_1",
                            ],
                        }
                    },
                }
            },
            expectations_store_name="expectations_store",
            validations_store_name="validations_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            checkpoint_store_name="checkpoint_store",
            store_backend_defaults=InMemoryStoreBackendDefaults(),
        )
        context: BaseDataContext = BaseDataContext(project_config=data_context_config)
        return context
    
    
    
    def validate_pandas(self, asset):
        context = self.pandas_in_memory_runtime_context()
        project = self._get_project()
        group = self._get_group()
        project_group = self._get_project_group(project, group)    
        group_rule = self._get_rules_in_group(group)
        suite = context.create_expectation_suite(self.group_name)
        rules_config = self._get_rule_configuration(group_rule)
        suite.add_expectation_configurations(rules_config)
        context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=self.group_name)

        runtime_batch_request = RuntimeBatchRequest(
            datasource_name="pandas_datasource",
            data_connector_name="runtime_data_connector",
            data_asset_name="Batch_Data",
            runtime_parameters={"batch_data": asset},
            batch_identifiers={"id_key_0": "id_key_1"}
        )

        my_validator: Validator = context.get_validator(
            batch_request=runtime_batch_request,
            expectation_suite=suite, # OR
            # expectation_suite_name=suite_name
        )

        _results = my_validator.validate()
        self._write_validation_results(project, group, group_rule, _results.to_json_dict(), checkpoint=False)
        return _results

    
    def _write_validation_results(self, project, group, rule, result, checkpoint):
        """Writing the validation results to the collection"""
        project_id = project["ID"]
        project_name = project["NAME"]
        if checkpoint:
            result = list(result["run_results"].values())[0]["validation_result"]
        else:
            result = result
        
        for _result in result["results"]:
            if 'exception_info' in _result:
                _result_exec_info = _result['exception_info']
                _result_exec_info.pop('exception_traceback')
                _result_exec_info["exception_message"] = _result_exec_info["exception_message"]

        data_to_insert = {
            "project_id": project_id, 
            "project_name": project_name,
            "group_id": group["ID"], 
            "group_name": result["meta"]["expectation_suite_name"], 
            "run_date_time": str(datetime.now()), 
            "success_percentage": result["statistics"]["success_percent"],
            "total_rules": len(rule),
            "result_json": result["results"],
            "total_records_passed": result["statistics"]["successful_expectations"],
            "total_records_failed": result["statistics"]["unsuccessful_expectations"],
            "run_name": result["meta"]["run_id"]["run_name"],
            "checkpoint_name": result["meta"]["checkpoint_name"],
            "data_asset": result["meta"]["active_batch_definition"]["data_asset_name"]
            }
        print("data",data_to_insert)
        rules_failed = []
        for rule in result["results"]:
            if not rule["success"]: # If rule `failed`
                # Extract rules data from results and append in the data_to_insert
                exec_conf = rule["expectation_config"]
                exec_type = exec_conf["expectation_type"]
                rules_failed.append({"rule": exec_type, "Count":result["statistics"]["unsuccessful_expectations"]})

        data_to_insert["rules_failed"] = rules_failed
        IST = datetime.utcnow() + timedelta(hours=5, minutes=30)
        insert_query = f"""INSERT INTO DQ_VALIDATION_RESULTS (project_name, group_name, result, 
        project_id, group_id, created_date) """
        squery = f"""SELECT '{project_name}', '{result['meta']['expectation_suite_name']}', 
        parse_json('{json.dumps(data_to_insert)}'), {project_id}, {group["ID"]}, 
        '{IST.strftime("%Y-%m-%dT%H:%M:%S")}'"""
        final_query = insert_query + squery
        
        result = self.session.sql(final_query).collect()
        return {"success" : True}