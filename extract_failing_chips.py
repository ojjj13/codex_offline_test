import pandas as pd
from pathlib import Path
from typing import List


def parse_wafer_csv(file_path: str, metadata_rows: int = 29) -> pd.DataFrame:
    """Return DataFrame of failing chip coordinates for a wafer CSV."""
    df_all = pd.read_csv(file_path, header=None, skiprows=metadata_rows)
    if len(df_all) < 6:
        raise ValueError("CSV format unexpected; not enough rows after metadata")

    test_groups = df_all.iloc[0, 8:].astype(str).tolist()
    test_items = df_all.iloc[1, 8:].astype(str).tolist()
    upper_limits = pd.to_numeric(df_all.iloc[3, 8:], errors="coerce")
    lower_limits = pd.to_numeric(df_all.iloc[4, 8:], errors="coerce")

    headers = df_all.iloc[5, :8].tolist() + test_items
    units = df_all.iloc[5, 8:].astype(str).tolist()

    data_rows = df_all.iloc[6:].copy()
    data_rows = data_rows.iloc[:, :len(headers)]
    data_rows.columns = headers
    data_rows = data_rows.dropna(how="all")

    for col in ["XAdr", "YAdr"] + test_items:
        data_rows[col] = pd.to_numeric(data_rows[col], errors="coerce")

    failures = []
    for idx, item in enumerate(test_items):
        upper = upper_limits.iat[idx]
        lower = lower_limits.iat[idx]
        mask = (data_rows[item] > upper) | (data_rows[item] < lower)
        failing = data_rows.loc[mask, ["XAdr", "YAdr", item]]
        if failing.empty:
            continue
        failing = failing.assign(
            test_item=f"{test_groups[idx]}-{item}",
            unit=units[idx],
            value=failing[item],
            limit_high=upper,
            limit_low=lower,
        )
        failures.append(
            failing[["XAdr", "YAdr", "test_item", "unit", "value", "limit_high", "limit_low"]]
        )

    if failures:
        return pd.concat(failures, ignore_index=True)
    return pd.DataFrame(
        columns=["XAdr", "YAdr", "test_item", "unit", "value", "limit_high", "limit_low"]
    )


def compare_coverage(file_a: str, file_b: str) -> None:
    """Compare failing chips between two CSV files and report coverage."""
    df_a = parse_wafer_csv(file_a)
    df_b = parse_wafer_csv(file_b)

    if df_a.empty and df_b.empty:
        print("Both files have no failures to compare")
        return

    merged = pd.merge(
        df_a,
        df_b,
        on=["XAdr", "YAdr", "test_item"],
        how="outer",
        suffixes=("_a", "_b"),
    )

    def status(row: pd.Series) -> str:
        if not pd.isna(row.get("value_a")) and not pd.isna(row.get("value_b")):
            return "both_fail"
        if not pd.isna(row.get("value_a")):
            return "fail_in_a_only"
        return "fail_in_b_only"

    merged["status"] = merged.apply(status, axis=1)
    coverage = (
        len(merged[merged["status"] == "both_fail"]) / len(df_a) * 100 if len(df_a) else 0.0
    )

    merged.to_csv("coverage.csv", index=False)
    print(f"Coverage of {file_a} on {file_b}: {coverage:.2f}%")
    print("Detailed coverage written to coverage.csv")


def save_failures(path: str) -> None:
    df = parse_wafer_csv(path)
    if df.empty:
        print(f"No failures found in {path}")
        return
    out_path = f"{Path(path).stem}_failures.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved failures to {out_path}")


def main(args: List[str]):
    if args and args[0] == "--compare" and len(args) == 3:
        compare_coverage(args[1], args[2])
        return
    for path in args:
        save_failures(path)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(
            "Usage: python extract_failing_chips.py [--compare fileA.csv fileB.csv] <csv>..."
        )
        sys.exit(1)
    main(sys.argv[1:])
