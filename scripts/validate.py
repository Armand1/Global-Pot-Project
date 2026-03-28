#!/usr/bin/env python3
"""
validate.py — Check integrity of the Global Pot Project image archive.

Compares three sources of truth:
  1. data/master_metadata.csv       — registered pots
  2. data/manifests/originals_manifest.csv — last recorded OneDrive state
  3. OneDrive images/originals/     — actual files on disk

Reports four categories of drift:
  A. Files on OneDrive but not in manifest (new/unregistered files)
  B. Files in manifest but missing from OneDrive (lost files)
  C. Files on OneDrive whose MD5 differs from manifest (corrupted/replaced)
  D. Pots in metadata with no original_path registered (metadata gaps)

Usage:
    python scripts/validate.py             # full check
    python scripts/validate.py --no-md5    # skip checksum verification (fast)
    python scripts/validate.py --regen     # regen manifest from OneDrive, then check
"""

import argparse
import csv
import hashlib
import sys
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
ONEDRIVE_ROOT = (
    Path.home()
    / "Library/CloudStorage/OneDrive-ImperialCollegeLondon"
)
ONEDRIVE_GPP = ONEDRIVE_ROOT / "Global Pot Project  Master"
ORIGINALS_DIR = ONEDRIVE_GPP / "images" / "originals"
METADATA_CSV = REPO_ROOT / "data" / "master_metadata.csv"
MANIFEST_CSV = REPO_ROOT / "data" / "manifests" / "originals_manifest.csv"

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}


# ── Utilities ─────────────────────────────────────────────────────────────────

def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def regen_manifest() -> None:
    """Regenerate manifest from current OneDrive contents."""
    # Import and call intake's update_manifest directly
    sys.path.insert(0, str(Path(__file__).parent))
    from intake import update_manifest, MANIFESTS_DIR
    update_manifest(ORIGINALS_DIR, MANIFESTS_DIR)


# ── Checks ────────────────────────────────────────────────────────────────────

def check_onedrive_vs_manifest(verify_checksums: bool) -> tuple[list, list, list]:
    """
    Compare live OneDrive files against the manifest.

    Returns:
        unregistered: files on OneDrive not in manifest
        missing:      files in manifest not on OneDrive
        corrupted:    files present on both sides but MD5 differs
    """
    manifest_rows = load_csv(MANIFEST_CSV)
    manifest = {r["filename"]: r for r in manifest_rows}

    if not ORIGINALS_DIR.exists():
        ondrive_files = {}
    else:
        ondrive_files = {
            f.name: f
            for f in sorted(ORIGINALS_DIR.iterdir())
            if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
        }

    unregistered = [name for name in ondrive_files if name not in manifest]
    missing = [name for name in manifest if name not in ondrive_files]
    corrupted = []

    if verify_checksums:
        to_check = [name for name in ondrive_files if name in manifest]
        print(f"  Verifying checksums for {len(to_check)} file(s)...", flush=True)
        for name in to_check:
            expected = manifest[name]["md5"]
            actual = md5(ondrive_files[name])
            if actual != expected:
                corrupted.append((name, expected, actual))

    return unregistered, missing, corrupted


def check_metadata_gaps() -> list[str]:
    """Return gpp_nos in metadata that have no original_path."""
    rows = load_csv(METADATA_CSV)
    return [
        r["gpp_no"] for r in rows
        if not r.get("original_path", "").strip()
    ]


# ── Report ────────────────────────────────────────────────────────────────────

def report(unregistered, missing, corrupted, gaps) -> int:
    """Print results. Returns exit code (0 = clean, 1 = issues found)."""
    issues = 0

    print("\n── A. Files on OneDrive not in manifest (unregistered) ──")
    if unregistered:
        for name in unregistered:
            print(f"  UNREGISTERED  {name}")
        issues += len(unregistered)
    else:
        print("  OK — none")

    print("\n── B. Files in manifest missing from OneDrive ──")
    if missing:
        for name in missing:
            print(f"  MISSING       {name}")
        issues += len(missing)
    else:
        print("  OK — none")

    print("\n── C. Checksum mismatches (possible corruption) ──")
    if corrupted:
        for name, expected, actual in corrupted:
            print(f"  CORRUPT       {name}")
            print(f"                expected {expected}")
            print(f"                got      {actual}")
        issues += len(corrupted)
    else:
        print("  OK — none (or --no-md5 skipped)")

    print("\n── D. Pots in metadata with no original registered ──")
    if gaps:
        print(f"  {len(gaps)} pot(s) have no original_path in metadata")
        for gpp_no in gaps[:20]:
            print(f"  GAP           {gpp_no}")
        if len(gaps) > 20:
            print(f"  ... and {len(gaps) - 20} more")
        issues += len(gaps)
    else:
        print("  OK — none")

    print(f"\n{'CLEAN — no issues found.' if issues == 0 else f'{issues} issue(s) found.'}\n")
    return 0 if issues == 0 else 1


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate Global Pot Project archive integrity."
    )
    parser.add_argument(
        "--no-md5",
        action="store_true",
        help="Skip checksum verification (faster, won't detect corruption)",
    )
    parser.add_argument(
        "--regen",
        action="store_true",
        help="Regenerate manifest from OneDrive before validating",
    )
    args = parser.parse_args()

    if args.regen:
        print("Regenerating manifest from OneDrive...")
        regen_manifest()

    print("Validating...\n")

    unregistered, missing, corrupted = check_onedrive_vs_manifest(
        verify_checksums=not args.no_md5
    )
    gaps = check_metadata_gaps()

    exit_code = report(unregistered, missing, corrupted, gaps)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
