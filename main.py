from connectors.database_connector import fetch_data_from_database
from connectors.file_connector import fetch_data_from_file
from connectors.elastic_connector import fetch_data_from_elastic, get_elasticsearch_client
from framework.reconciliation import perform_reconciliation
from framework.data_quality import perform_data_quality
from framework.utils import load_config, setup_logger, save_results_to_csv

def main():
    # Load configuration
    config = load_config("config/reconciliation_config.yaml")
    logger = setup_logger(config["output"]["logs"])
    results = []
    # Initialize Elasticsearch client
    elastic_clients = {}

    # Load sources
    sources = {}
    for name, source in config["sources"].items():
        if source["type"] == "sql_server":
            sources[name] = fetch_data_from_database(source["connection_string"], source["query"])
        elif source["type"] == "csv":
            sources[name] = fetch_data_from_file(source["filepath"], source.get("delimiter", ","))
        elif source["type"] == "elasticsearch":
            if name not in elastic_clients:
                elastic_clients[name] = get_elasticsearch_client(source["host"])
            sources[name] = fetch_data_from_elastic(
                elastic_clients[name], source["index"], source.get("query", '{"match_all": {}}')
            )

    # Load targets
    targets = {}
    for name, target in config["targets"].items():
        if target["type"] == "csv":
            targets[name] = fetch_data_from_file(target["filepath"], target.get("delimiter", ","))
        elif target["type"] == "elasticsearch":
            if name not in elastic_clients:
                elastic_clients[name] = get_elasticsearch_client(target["host"])
            targets[name] = fetch_data_from_elastic(
                elastic_clients[name], target["index"], target.get("query", '{"match_all": {}}')
            )

    # Perform Data Quality Checks (if specified)
    if "data_quality" in config and config["data_quality"]:
        for dq in config["data_quality"]:
            if dq["target"] not in targets:
                logger.error(f"Target '{dq['target']}' not found for data quality checks.")
                continue

            df = targets[dq["target"]]
            results.extend(perform_data_quality(df, dq["checks"]))
    else:
        logger.info("No data quality checks specified in the configuration.")

    # Perform Reconciliation (if specified)
    if "reconciliation" in config and config["reconciliation"]:
        for rec in config["reconciliation"]:
            if rec["source"] not in sources or rec["target"] not in targets:
                logger.error(f"Invalid source/target specified for reconciliation: {rec}")
                continue

            source_df = sources[rec["source"]]
            target_df = targets[rec["target"]]
            results.extend(perform_reconciliation(source_df, target_df, rec["rules"]))
    else:
        logger.info("No reconciliation rules specified in the configuration.")

    # Save results
    print("RESULTS")
    print(results)
    save_results_to_csv(results, config["output"]["results"])
    logger.info("Process completed successfully.")

if __name__ == "__main__":
    main()
