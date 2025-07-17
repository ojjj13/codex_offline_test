# Wafer Failure Analysis

This repository contains utilities for parsing test wafer CSV files and extracting details about failing chips. The main script `extract_failing_chips.py` can produce failure reports for individual wafers and compare coverage between two datasets.

## Usage

```
python extract_failing_chips.py wafer_A.csv wafer_B.csv
```

This command creates `wafer_A_failures.csv` and `wafer_B_failures.csv` summarizing each failing chip. Use the `--compare` option to see how one wafer's failures relate to another:

```
python extract_failing_chips.py --compare wafer_A.csv wafer_B.csv
```

After running with `--compare` the script writes two reports:

* `coverage.csv` – each failing chip measurement with its status in both wafers
* `summary.csv` – per test item statistics showing coverage percentages

Sample wafer files are provided so you can try the tool immediately. A Jupyter notebook version of the workflow is also included for interactive exploration.

