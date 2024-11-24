def perform_reconciliation(source_df, target_df, rules):
    results = []
    for rule in rules:
        if rule["type"] == "aggregate":
            source_count = len(source_df)
            target_count = len(target_df)
            success = abs(source_count - target_count) <= rule["tolerance"]
            results.append({
                "rule": rule["description"],
                "success": success,
                "details": f"Source count: {source_count}, Target count: {target_count}"
            })
        elif rule["type"] == "field_comparison":
            key = rule["key_column"]
            for field in rule["fields"]:
                merged = source_df.merge(target_df, on=key, suffixes=("_source", "_target"))
                mismatched = merged[merged[f"{field}_source"] != merged[f"{field}_target"]]
                success = mismatched.empty
                results.append({
                    "rule": rule["description"],
                    "success": success,
                    "details": f"Mismatched rows: {len(mismatched)}"
                })
    return results
