"""Utility for extracting failing chips from wafer CSVs.

When run with ``--compare fileA.csv fileB.csv`` the script writes two reports:

- ``coverage.csv`` lists each failing measurement and whether it failed in
  temperature A, temperature B, or both.
- ``summary.csv`` aggregates those results per test item so you can determine
  if one temperature's test fully covers another.
"""

import argparse
from pathlib import Path
from typing import List

import pandas as pd

def _unique_test_names(groups: List[str], items: List[str]) -> List[str]:
    """Return unique column names for group/item pairs.

    Some poorly formatted CSVs may repeat the same test group and item in
    multiple columns. Pandas will then try to create multi-dimensional columns
    when reading the data which breaks later processing.  This helper assigns a
    numeric suffix when duplicates appear so every column name is distinct.
    """

    counts: dict[str, int] = {}
    names: List[str] = []
    for g, it in zip(groups, items):
        base = f"{g}-{it}"
        counts[base] = counts.get(base, 0) + 1
        suffix = f"_{counts[base]}" if counts[base] > 1 else ""
        names.append(f"{base}{suffix}")
    return names


def get_test_items(file_path: str, metadata_rows: int = 29) -> List[str]:
    """Return list of test item names with group prefix and unique suffix."""
    df_hdr = pd.read_csv(file_path, header=None, skiprows=metadata_rows, nrows=2)
    test_groups = df_hdr.iloc[0, 8:].astype(str).tolist()
    test_items = df_hdr.iloc[1, 8:].astype(str).tolist()
    return _unique_test_names(test_groups, test_items)





def parse_wafer_csv(file_path: str, metadata_rows: int = 29) -> pd.DataFrame:
    """Return DataFrame of failing chip coordinates for a wafer CSV."""
    df_all = pd.read_csv(file_path, header=None, skiprows=metadata_rows)
    df_all = df_all.dropna(axis=1, how="all")
    df_all = df_all.dropna(how="all").reset_index(drop=True)
    if len(df_all) < 6:
        raise ValueError("CSV format unexpected; not enough rows after metadata")

    test_groups = df_all.iloc[0, 8:].astype(str).tolist()
    test_items_raw = df_all.iloc[1, 8:].astype(str).tolist()
    upper_limits = pd.to_numeric(df_all.iloc[2, 8:], errors="coerce")
    lower_limits = pd.to_numeric(df_all.iloc[3, 8:], errors="coerce")


    # Build unique column names to avoid collision when test_items repeat under
    # different groups or appear multiple times.
    test_items = _unique_test_names(test_groups, test_items_raw)

    headers = df_all.iloc[4, :8].tolist() + test_items
    units = df_all.iloc[4, 8:].astype(str).tolist()

    data_rows = df_all.iloc[5:].copy()
    data_rows = data_rows.iloc[:, : len(headers)]

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
            test_item=item,
            unit=units[idx],
            value=failing[item],
            limit_high=upper,
            limit_low=lower,
        )
        failures.append(
            failing[
                [
                    "XAdr",
                    "YAdr",
                    "test_item",
                    "unit",
                    "value",
                    "limit_high",
                    "limit_low",
                ]
            ]
        )

    if failures:
        return pd.concat(failures, ignore_index=True)
    return pd.DataFrame(

        columns=[
            "XAdr",
            "YAdr",
            "test_item",
            "unit",
            "value",
            "limit_high",
            "limit_low",
        ]

    )


def compare_coverage(file_a: str, file_b: str) -> None:
    """Compare failing chips between two CSV files and report coverage."""
    df_a = parse_wafer_csv(file_a)
    df_b = parse_wafer_csv(file_b)

    tests_a = set(get_test_items(file_a))
    tests_b = set(get_test_items(file_b))

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
        len(merged[merged["status"] == "both_fail"]) / len(df_a) * 100
        if len(df_a)
        else 0.0
    )

    merged.to_csv("coverage.csv", index=False)
    print(f"Coverage of {file_a} on {file_b}: {coverage:.2f}%")
    print("Detailed coverage written to coverage.csv")

    summary = summarize_by_test_item(merged, tests_a, tests_b)
    summary.to_csv("summary.csv", index=False)
    print("Summary written to summary.csv")


def summarize_by_test_item(
    df: pd.DataFrame, tests_a: set[str], tests_b: set[str]
) -> pd.DataFrame:
    """Return summary statistics grouped by test_item."""
    grouped = df.groupby("test_item")
    summary = grouped.agg(
        fails_a=("value_a", lambda s: s.notna().sum()),
        fails_b=("value_b", lambda s: s.notna().sum()),
        both_fail=("status", lambda s: (s == "both_fail").sum()),
    ).reset_index()

    summary["coverage_a_in_b"] = (

        (summary["both_fail"] / summary["fails_a"].replace(0, pd.NA) * 100)
        .fillna(0)
        .round(2)
    )
    summary["coverage_b_in_a"] = (
        (summary["both_fail"] / summary["fails_b"].replace(0, pd.NA) * 100)
        .fillna(0)
        .round(2)
    )

    summary["a_fully_covered"] = (summary["fails_a"] > 0) & (
        summary["coverage_a_in_b"] == 100
    )
    summary["b_fully_covered"] = (summary["fails_b"] > 0) & (
        summary["coverage_b_in_a"] == 100

    )

    summary["present_in_a"] = summary["test_item"].isin(tests_a)
    summary["present_in_b"] = summary["test_item"].isin(tests_b)

    return summary


def save_failures(path: str) -> None:
    df = parse_wafer_csv(path)
    if df.empty:
        print(f"No failures found in {path}")
        return
    out_path = f"{Path(path).stem}_failures.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved failures to {out_path}")


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract failing chips and optionally compare coverage",
    )
    parser.add_argument(
        "--compare",
        nargs=2,
        metavar=("FILE_A", "FILE_B"),
        help="Compare failing chips between two CSV files",
    )
    parser.add_argument(
        "csv",
        nargs="*",
        help="CSV files to convert into *_failures.csv reports",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)

    if args.compare:
        compare_coverage(*args.compare)

    for path in args.csv:
        save_failures(path)


if __name__ == "__main__":
    main()
