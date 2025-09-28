"""Command line utility to download US macroeconomic data from FRED."""
from __future__ import annotations

import argparse
from pathlib import Path

from economics_metrics import DEFAULT_SERIES, DownloadError, download_series_collection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/us"),
        help="Directory where the CSV files will be stored.",
    )
    parser.add_argument(
        "--series",
        nargs="*",
        metavar="NAME",
        help=(
            "Specific series to download. Defaults to all known series. Choices: "
            + ", ".join(sorted(DEFAULT_SERIES))
        ),
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Skip downloading files that already exist.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    overwrite = not args.no_overwrite

    try:
        paths = download_series_collection(
            args.series,
            args.output_dir,
            overwrite=overwrite,
        )
    except DownloadError as exc:
        print(f"Error: {exc}")
        return 1
    except KeyError as exc:
        print(f"Unknown series requested: {exc}")
        return 2

    for path in paths:
        print(f"Saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
