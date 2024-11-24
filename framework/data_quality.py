
def perform_data_quality(df, checks):
    results = []
    for check in checks:
        if check["type"] == "non_null":
            null_count = df[check["column"]].isnull().sum()
            success = null_count == 0
            results.append({
                "check": f"Non-null check on {check['column']}",
                "success": success,
                "details": f"Null count: {null_count}"
            })
        elif check["type"] == "range":
            col = check["column"]
            min_val = df[col].min()
            max_val = df[col].max()
            success = check["min"] <= min_val and max_val <= check["max"]
            results.append({
                "check": f"Range check on {col}",
                "success": success,
                "details": f"Min: {min_val}, Max: {max_val}"
            })
    return results
