#!/usr/bin/env python3
"""
intake.py — Register new images into the Global Pot Project.

Usage:
    python scripts/intake.py <source_folder> [--source-desc "description"]

For each image in <source_folder>:
  - Derives gpp_no from filename stem (e.g. af_51.jpg → af_51)
  - Looks up the gpp_no in data/master_metadata.csv
  - Copies the file to OneDrive/.../images/originals/
  - Verifies the copy with MD5 checksum
  - Updates tracking columns in master_metadata.csv
  - Regenerates the originals manifest
"""

import argparse
import csv
import hashlib
import shutil
import sys
from datetime import date
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
ONEDRIVE_ROOT = (
    Path.home()
    / "Library/CloudStorage/OneDrive-ImperialCollegeLondon"
)
ONEDRIVE_GPP = ONEDRIVE_ROOT / "Global Pot Project  Master"
ORIGINALS_DIR = ONEDRIVE_GPP / "images" / "originals"
CLEAN_DIR = ONEDRIVE_GPP / "images" / "clean" / "pots_clean"
PROFILES_CSV = ONEDRIVE_GPP / "gpp_master_profiles.csv"
METADATA_CSV = REPO_ROOT / "data" / "master_metadata.csv"
MANIFESTS_DIR = REPO_ROOT / "data" / "manifests"

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}

# Tracking columns added by intake — never remove or rename these
TRACKING_COLUMNS = [
    "original_filename",
    "original_path",
    "cleaned_path",
    "date_accessioned",
    "source",
    "metadata_complete",
    "notes",
]


# ── Utilities ─────────────────────────────────────────────────────────────────

def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_metadata(csv_path: Path) -> tuple[list[dict], list[str]]:
    if not csv_path.exists():
        return [], []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def save_metadata(csv_path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_profiled_ids(profiles_csv: Path) -> set[str]:
    """Return the set of gpp_nos that have a profile in gpp_master_profiles.csv."""
    profiled = set()
    if not profiles_csv.exists():
        return profiled
    print("  Loading profiled IDs...", flush=True)
    with open(profiles_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            profiled.add(row["gpp_no"])
    return profiled


def load_metadata_ids(metadata_csv: Path) -> set[str]:
    """Return the set of gpp_nos that have a row in master_metadata.csv."""
    ids = set()
    if not metadata_csv.exists():
        return ids
    with open(metadata_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.add(row["gpp_no"])
    return ids


def load_clean_ids(clean_dir: Path) -> set[str]:
    """Return the set of gpp_nos that have a manually cleaned image."""
    if not clean_dir.exists():
        return set()
    return {f.stem for f in clean_dir.iterdir()
            if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS}


def update_manifest(originals_dir: Path, manifests_dir: Path) -> Path:
    manifests_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifests_dir / "originals_manifest.csv"
    profiled_ids = load_profiled_ids(PROFILES_CSV)
    metadata_ids = load_metadata_ids(METADATA_CSV)
    clean_ids = load_clean_ids(CLEAN_DIR)
    entries = []
    if originals_dir.exists():
        for f in sorted(originals_dir.iterdir()):
            if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS:
                gpp_no = f.stem
                entries.append({
                    "filename": f.name,
                    "path": str(f.relative_to(ONEDRIVE_ROOT)),
                    "md5": md5(f),
                    "size_bytes": f.stat().st_size,
                    "profile_present": 1 if gpp_no in profiled_ids else 0,
                    "metadata_present": 1 if gpp_no in metadata_ids else 0,
                    "clean_present": 1 if gpp_no in clean_ids else 0,
                })
    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["filename", "path", "md5", "size_bytes",
                           "profile_present", "metadata_present", "clean_present"]
        )
        writer.writeheader()
        writer.writerows(entries)
    print(f"  Manifest written: {manifest_path} ({len(entries)} files)")
    return manifest_path


# ── Main logic ────────────────────────────────────────────────────────────────

def intake(source_folder: Path, source_desc: str) -> None:
    images = sorted(
        f for f in source_folder.iterdir()
        if f.is_file() and f.suffix.lower() in IMAGE_EXTENSIONS
    )
    if not images:
        print(f"No image files found in {source_folder}")
        return

    print(f"Found {len(images)} image(s) in {source_folder}\n")

    rows, fieldnames = load_metadata(METADATA_CSV)

    # Ensure tracking columns are present
    for col in TRACKING_COLUMNS:
        if col not in fieldnames:
            fieldnames.append(col)
            for row in rows:
                row.setdefault(col, "")

    metadata_index = {row["gpp_no"]: row for row in rows}

    ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)

    today = date.today().isoformat()
    registered = 0
    skipped = 0
    errors = []

    for img in images:
        gpp_no = img.stem
        dest = ORIGINALS_DIR / img.name

        # Skip if already fully registered
        if gpp_no in metadata_index and metadata_index[gpp_no].get("original_path"):
            print(f"  SKIP   {img.name} — already registered")
            skipped += 1
            continue

        if gpp_no not in metadata_index:
            print(f"  WARN   {img.name} — gpp_no '{gpp_no}' not in metadata, registering anyway")

        # Copy to OneDrive (or verify if already there)
        if dest.exists():
            if md5(img) != md5(dest):
                errors.append(
                    f"{img.name}: file already exists on OneDrive but checksums differ — skipping"
                )
                continue
            print(f"  EXISTS {img.name} — already on OneDrive, updating metadata only")
        else:
            print(f"  COPY   {img.name} → {dest}")
            shutil.copy2(img, dest)
            if md5(img) != md5(dest):
                dest.unlink()
                errors.append(
                    f"{img.name}: checksum mismatch after copy — removed from destination"
                )
                continue

        rel_path = str(dest.relative_to(ONEDRIVE_ROOT))

        if gpp_no in metadata_index:
            row = metadata_index[gpp_no]
        else:
            row = {col: "" for col in fieldnames}
            row["gpp_no"] = gpp_no
            rows.append(row)
            metadata_index[gpp_no] = row

        row["original_filename"] = img.name
        row["original_path"] = rel_path
        row["date_accessioned"] = today
        row["source"] = source_desc
        row["metadata_complete"] = "False"

        registered += 1

    save_metadata(METADATA_CSV, rows, fieldnames)
    update_manifest(ORIGINALS_DIR, MANIFESTS_DIR)

    print(f"\nDone.  Registered: {registered}  Skipped: {skipped}  Errors: {len(errors)}")
    if errors:
        print("\nErrors:")
        for e in errors:
            print(f"  {e}")
    if registered > 0:
        print(
            "\nNext: git add data/ && git commit -m "
            f"'intake: register {registered} image(s) from {source_desc or source_folder.name}'"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Intake new images into the Global Pot Project."
    )
    parser.add_argument("source", type=Path, help="Folder containing new images")
    parser.add_argument(
        "--source-desc", default="", help="Short description of the image source"
    )
    args = parser.parse_args()

    if not args.source.is_dir():
        print(f"Error: '{args.source}' is not a directory", file=sys.stderr)
        sys.exit(1)

    intake(args.source, args.source_desc)


if __name__ == "__main__":
    main()
