import pandas as pd
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
        )
        failures.append(failing[["XAdr", "YAdr", "test_item", "unit", "value"]])

    if failures:
        return pd.concat(failures, ignore_index=True)
    return pd.DataFrame(columns=["XAdr", "YAdr", "test_item", "unit", "value"])


def main(files: List[str]):
    all_frames = []
    for path in files:
        df = parse_wafer_csv(path)
        if not df.empty:
            df["file"] = path
            all_frames.append(df)
    if all_frames:
        result = pd.concat(all_frames, ignore_index=True)
        print(result.to_csv(index=False))
    else:
        print("No failures found")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python extract_failing_chips.py <file1.csv> [file2.csv ...]")
        sys.exit(1)
    main(sys.argv[1:])
