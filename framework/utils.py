import yaml
import logging
import csv

def load_config(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

def setup_logger(log_file):
    logging.basicConfig(filename=log_file, level=logging.DEBUG, 
                        format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger()

def save_results_to_csv(results, output_file):
    # Get all unique keys from the results
    all_keys = set()
    for result in results:
        all_keys.update(result.keys())

    # Normalize results to include all keys
    normalized_results = []
    for result in results:
        normalized_results.append({key: result.get(key, "") for key in all_keys})

    # Write to CSV
    with open(output_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=all_keys)
        writer.writeheader()
        writer.writerows(normalized_results)

# def save_results_to_csv(results, output_file):
#     keys = results[0].keys()
#     with open(output_file, "w", newline="") as file:
#         writer = csv.DictWriter(file, fieldnames=keys)
#         writer.writeheader()
#         writer.writerows(results)
