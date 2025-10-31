#!/usr/bin/env python3
"""Utility script to export the latest commit as a patch file.

Render deployments and GitHub Codespaces sometimes need a raw patch that
can be applied manually (for example, when working outside of the
standard Git workflow).  This script wraps ``git format-patch`` so that
non-Git experts can generate the patch with a single command.
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def export_patch(output: Path) -> Path:
    """Generate a patch for ``HEAD`` and write it to *output*.

    Parameters
    ----------
    output:
        Destination path for the patch.  Parent directories are created
        automatically.

    Returns
    -------
    Path
        The path that was written, allowing callers to surface it to
        users easily.
    """

    output.parent.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        ["git", "format-patch", "-1", "HEAD", "--stdout"],
        check=True,
        capture_output=True,
        text=True,
    )
    output.write_text(result.stdout)
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export the latest commit as a patch file")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("patches/latest.patch"),
        help="Where to write the patch (default: patches/latest.patch)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dest = export_patch(args.output)
    print(f"Patch written to {dest}")


if __name__ == "__main__":
    main()
