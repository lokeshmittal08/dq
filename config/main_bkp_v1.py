from connectors.database_connector import fetch_data_from_database
from connectors.file_connector import fetch_data_from_file
from connectors.elastic_connector import fetch_data_from_elastic, get_elasticsearch_client
from framework.reconciliation import perform_reconciliation
from framework.data_quality import perform_data_quality
from framework.utils import load_config, setup_logger, save_results_to_csv

def main():
    config = load_config("config/reconciliation_config.yaml")
    logger = setup_logger(config["output"]["logs"])
    results = []

    # Load sources and targets
    sources = {}
    targets = {}
    for name, source in config["sources"].items():
        if source["type"] == "sql_server":
            sources[name] = fetch_data_from_database(source["connection_string"], source["query"])
    for name, target in config["targets"].items():
        if target["type"] == "elasticsearch":
            targets[name] = fetch_data_from_elastic(
                get_elasticsearch_client(target["host"]), target["index"], target.get("query", '{"match_all": {}}'))

    # Perform Data Quality Checks
    for dq in config["data_quality"]:
        df = targets[dq["target"]]
        results.extend(perform_data_quality(df, dq["checks"]))

    # Perform Reconciliation
    for rec in config["reconciliation"]:
        source_df = sources[rec["source"]]
        target_df = fetch_data_from_elastic(
            get_elasticsearch_client(config["targets"][rec["target"]]["host"]),
            config["targets"][rec["target"]]["index"],
            rec["target_query"]
        )
        results.extend(perform_reconciliation(source_df, target_df, rec["rules"]))

    # Save results
    save_results_to_csv(results, config["output"]["results"])
    logger.info("Process completed successfully.")

if __name__ == "__main__":
    main()
