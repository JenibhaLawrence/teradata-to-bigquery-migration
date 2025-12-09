import pandas as pd
import hashlib
import sys

def md5_hash_row(row):
    """Generate MD5 for an entire row."""
    row_str = "|".join([str(x) for x in row.values])
    return hashlib.md5(row_str.encode()).hexdigest()

def validate(td_file, bq_file, key_column, tolerance=0.0001):
    print("\n===== ADVANCED TERADATA–BIGQUERY VALIDATION =====")

    td = pd.read_csv(td_file)
    bq = pd.read_csv(bq_file)

    results = {}

    # ---------------------------
    # 1. Column count validation
    # ---------------------------
    results['column_count_match'] = (td.shape[1] == bq.shape[1])
    
    # ---------------------------
    # 2. Row count validation
    # ---------------------------
    results['row_count_match'] = (td.shape[0] == bq.shape[0])

    # ---------------------------
    # 3. Data type compatibility
    # ---------------------------
    dtype_match = True
    for col in td.columns:
        if col in bq.columns:
            if td[col].dtype != bq[col].dtype:
                dtype_match = False
                break
        else:
            dtype_match = False
            break
    results['dtype_match'] = dtype_match

    # ---------------------------
    # 4. PK merge comparison
    # ---------------------------
    merged = td.merge(bq, on=key_column, suffixes=("_td", "_bq"), how="outer", indicator=True)
    pk_match = merged["_merge"].eq("both").all()
    results['primary_key_match'] = pk_match

    # ---------------------------
    # 5. Hash comparison
    # ---------------------------
    td["md5"] = td.apply(md5_hash_row, axis=1)
    bq["md5"] = bq.apply(md5_hash_row, axis=1)

    merged_hash = td[[key_column, "md5"]].merge(
        bq[[key_column, "md5"]], on=key_column, suffixes=("_td", "_bq")
    )
    merged_hash["hash_match"] = merged_hash["md5_td"] == merged_hash["md5_bq"]
    
    hash_match = merged_hash["hash_match"].all()
    results['hash_match'] = hash_match

    # Write mismatch file if needed
    mismatches = merged_hash[merged_hash["hash_match"] == False]
    if len(mismatches) > 0:
        mismatches.to_csv("row_mismatches.csv", index=False)
        print("\n⚠️ Mismatch rows saved to: row_mismatches.csv")

    # ---------------------------
    # 6. Statistical checks
    # ---------------------------
    numeric_cols = td.select_dtypes(include=["int64", "float64"]).columns

    stat_match = True
    for col in numeric_cols:
        td_sum = td[col].sum()
        bq_sum = bq[col].sum()
        diff_ratio = abs(td_sum - bq_sum) / (abs(td_sum) + 1e-9)  # avoid divide by zero
        
        if diff_ratio > tolerance:
            print(f"⚠️ Aggregation mismatch in column: {col}")
            stat_match = False
            break

    results['aggregation_match'] = stat_match

    # ---------------------------
    # 7. Final score
    # ---------------------------
    total_checks = len(results)
    passed_checks = sum(1 for v in results.values() if v)
    score = (passed_checks / total_checks) * 100
    results['final_score'] = round(score, 2)

    print("\n===== FINAL VALIDATION REPORT =====")
    for k, v in results.items():
        print(f"{k}: {v}")
    
    if score == 100:
        print("\n🎉 SUCCESS: PERFECT MATCH (100%)")
    else:
        print("\n❗ PARTIAL MATCH — CHECK ERRORS ABOVE")

    return results


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python validate_td_bq_advanced.py <td_file.csv> <bq_file.csv> <primary_key>")
        sys.exit(1)

    td_file = sys.argv[1]
    bq_file = sys.argv[2]
    key_column = sys.argv[3]

    validate(td_file, bq_file, key_column)
