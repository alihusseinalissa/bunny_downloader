#!/usr/bin/env python3
"""
Bunny.net Stream Video Bulk Downloader
======================================
Downloads all original video files from a Bunny.net Stream library.

Features:
  - Paginated API listing (handles 1500+ videos)
  - Parallel downloads (configurable concurrency)
  - Resume support (skips already-downloaded files)
  - Retry with exponential backoff
  - Progress tracking with ETA
  - Saves a manifest JSON for state tracking
  - Supports both "original" file and MP4 fallback modes

Usage:
  1. Set your credentials below or via environment variables.
  2. Run:  python3 download_bunny_videos.py
  3. Videos are saved to ./downloaded_videos/ (configurable)

Required: pip install requests
"""

import os
import sys
import json
import time
import hashlib
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ─────────────────────────── Configuration ───────────────────────────

# Set these via environment variables or edit directly here
LIBRARY_ID = os.environ.get("BUNNY_LIBRARY_ID", "")
API_KEY = os.environ.get("BUNNY_API_KEY", "")

# Storage Zone password — found in Bunny.net dashboard → Storage → Your Zone → FTP & API Access → Password
# This is DIFFERENT from the Stream API key used to list videos.
STORAGE_KEY = os.environ.get("BUNNY_STORAGE_KEY", "")

# The CDN hostname for your library (e.g., "vz-abc12345-xyz.b-cdn.net")
# Find this in your Bunny.net Stream dashboard under your video library.
CDN_HOSTNAME = os.environ.get("BUNNY_CDN_HOSTNAME", "")

# Download mode: "original" or "mp4_fallback"
#   - "original": Downloads the original uploaded file (requires "Keep Original File" enabled)
#   - "mp4_fallback": Downloads the MP4 fallback at a given resolution (requires "MP4 Fallback" enabled)
DOWNLOAD_MODE = os.environ.get("BUNNY_DOWNLOAD_MODE", "original")

# For mp4_fallback mode: which resolution to prefer (tries in order)
PREFERRED_RESOLUTIONS = ["2160", "1080", "720", "480", "360"]

# Download settings
OUTPUT_DIR = os.environ.get("BUNNY_OUTPUT_DIR", "./downloaded_videos")
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("BUNNY_CONCURRENCY", "4"))
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds
ITEMS_PER_PAGE = 100
REQUEST_TIMEOUT = 30  # seconds for API requests
DOWNLOAD_TIMEOUT = 600  # seconds per video download (10 min)
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB chunks for streaming downloads

# ─────────────────────────── Logging ─────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bunny-downloader")

# ─────────────────────────── Progress Tracker ────────────────────────

class ProgressTracker:
    def __init__(self, total: int):
        self.total = total
        self.completed = 0
        self.skipped = 0
        self.failed = 0
        self.bytes_downloaded = 0
        self.start_time = time.time()
        self._lock = Lock()

    def mark_completed(self, size_bytes: int = 0):
        with self._lock:
            self.completed += 1
            self.bytes_downloaded += size_bytes
            self._log_progress()

    def mark_skipped(self):
        with self._lock:
            self.skipped += 1
            self._log_progress()

    def mark_failed(self):
        with self._lock:
            self.failed += 1
            self._log_progress()

    def _log_progress(self):
        done = self.completed + self.skipped + self.failed
        elapsed = time.time() - self.start_time
        if self.completed > 0:
            avg_time = elapsed / (self.completed + self.skipped)
            remaining = (self.total - done) * avg_time
            eta = str(timedelta(seconds=int(remaining)))
        else:
            eta = "calculating..."

        mb = self.bytes_downloaded / (1024 * 1024)
        log.info(
            f"Progress: {done}/{self.total} "
            f"(✓ {self.completed} downloaded, ⏭ {self.skipped} skipped, ✗ {self.failed} failed) "
            f"| {mb:.1f} MB | ETA: {eta}"
        )

# ─────────────────────────── API Functions ───────────────────────────

def list_all_videos(library_id: str, api_key: str, collection_id: str = "") -> list[dict]:
    """Fetch all videos from the Bunny.net Stream library using pagination.

    Args:
        library_id:     Bunny.net Stream library ID.
        api_key:        Bunny.net Stream API key.
        collection:  Optional collection GUID.  When provided, only videos
                        belonging to that collection are returned.
    """
    base_url = f"https://video.bunnycdn.com/library/{library_id}/videos"
    headers = {
        "AccessKey": api_key,
        "Accept": "application/json",
    }

    all_videos = []
    page = 1

    if collection_id:
        log.info(f"Fetching video list from Bunny.net Stream API (collection: {collection_id})...")
    else:
        log.info("Fetching video list from Bunny.net Stream API...")

    while True:
        params = {
            "page": page,
            "itemsPerPage": ITEMS_PER_PAGE,
        }
        if collection_id:
            params["collection"] = collection_id

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    base_url, headers=headers, params=params, timeout=REQUEST_TIMEOUT
                )
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                    log.warning(f"API request failed (attempt {attempt+1}), retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    log.error(f"Failed to fetch page {page} after {MAX_RETRIES} attempts: {e}")
                    raise

        data = resp.json()

        # The response has "items" (lowercase) or "Items" - handle both
        items = data.get("items") or data.get("Items", [])

        if not items:
            break

        all_videos.extend(items)
        total_items = data.get("totalItems") or data.get("TotalItems", "?")
        log.info(f"  Page {page}: fetched {len(items)} videos (total so far: {len(all_videos)}/{total_items})")

        if len(items) < ITEMS_PER_PAGE:
            break

        page += 1
        time.sleep(0.2)  # Small delay to be nice to the API

    log.info(f"Total videos found: {len(all_videos)}")
    return all_videos


def get_video_resolutions(video: dict) -> list[str]:
    """
    Parse the 'availableResolutions' field (e.g. "360p,480p,720p,1080p") and
    return them sorted by PREFERRED_RESOLUTIONS order (highest quality first).
    Falls back to PREFERRED_RESOLUTIONS if the field is missing.
    """
    raw = video.get("availableResolutions") or video.get("AvailableResolutions") or ""
    if not raw:
        return PREFERRED_RESOLUTIONS  # fallback: try all

    # Strip trailing 'p' from each token so we can compare with PREFERRED_RESOLUTIONS
    available = set()
    for token in raw.split(","):
        token = token.strip().lower().rstrip("p")
        if token:
            available.add(token)

    # Return only resolutions that are in PREFERRED_RESOLUTIONS (preserves priority order)
    return [res for res in PREFERRED_RESOLUTIONS if res in available]


def build_download_url(video: dict, cdn_hostname: str, mode: str) -> tuple[str, str]:
    """
    Build the download URL and output filename for a video.
    Returns (url, filename).

    For "original" mode, uses the Bunny.net Storage API URL:
      https://storage.bunnycdn.com/{storage_zone}/{video_id}/
    Authentication is done via the 'AccessKey' request header (storage zone password).
    The storage zone name is derived from the CDN hostname by stripping ".b-cdn.net".

    For "mp4_fallback" mode, selects the highest resolution from the video's
    'availableResolutions' field (e.g. "360p,480p,720p,1080p").
    """
    video_id = video.get("guid") or video.get("Guid") or video.get("videoId")
    title = video.get("title") or video.get("Title") or video_id

    # Sanitize the title for use as a filename
    safe_title = sanitize_filename(title)

    if mode == "original":
        # Derive storage zone name from CDN hostname (strip ".b-cdn.net")
        storage_zone = cdn_hostname.replace(".b-cdn.net", "")
        url = f"https://storage.bunnycdn.com/{storage_zone}/{video_id}/"
        filename = f"{safe_title}___{video_id}.mp4"
    else:
        # MP4 fallback mode — pick the highest resolution actually available for this video
        resolutions = get_video_resolutions(video)
        if resolutions:
            best_res = resolutions[0]  # first = highest priority per PREFERRED_RESOLUTIONS
            url = f"https://{cdn_hostname}/{video_id}/play_{best_res}p.mp4"
        else:
            # No known resolution available — fall back to first in PREFERRED_RESOLUTIONS
            url = f"https://{cdn_hostname}/{video_id}/play_{PREFERRED_RESOLUTIONS[0]}p.mp4"
        filename = f"{safe_title}___{video_id}.mp4"

    return url, filename


def sanitize_filename(name: str) -> str:
    """Remove or replace characters that are unsafe for filenames."""
    # Replace common problematic characters
    replacements = {
        "/": "-", "\\": "-", ":": "-", "*": "", "?": "",
        '"': "", "<": "", ">": "", "|": "", "\n": " ", "\r": "",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    # Truncate to reasonable length
    name = name.strip()
    if len(name) > 150:
        name = name[:150]
    return name or "untitled"


# ─────────────────────────── Download Functions ──────────────────────

def download_video(
    video: dict,
    cdn_hostname: str,
    output_dir: Path,
    mode: str,
    tracker: ProgressTracker,
    storage_key: str = "",
) -> dict:
    """Download a single video. Returns a result dict."""
    video_id = video.get("guid") or video.get("Guid") or video.get("videoId")
    title = video.get("title") or video.get("Title") or video_id

    url, filename = build_download_url(video, cdn_hostname, mode)
    filepath = output_dir / filename

    # Skip if already downloaded
    if filepath.exists() and filepath.stat().st_size > 0:
        log.debug(f"Skipping (already exists): {filename}")
        tracker.mark_skipped()
        return {"video_id": video_id, "status": "skipped", "path": str(filepath)}

    # Try downloading with retries
    urls_to_try = [url]
    if mode == "mp4_fallback":
        # Only try resolutions that are actually available for this video
        available_res = get_video_resolutions(video)
        urls_to_try = [
            f"https://{cdn_hostname}/{video_id}/play_{res}p.mp4"
            for res in available_res
        ] or [url]  # fall back to the pre-built url if nothing resolved

    for try_url in urls_to_try:
        result = _attempt_download(try_url, filepath, video_id, title, tracker, storage_key)
        if result["status"] == "success":
            return result

    # All URLs failed
    tracker.mark_failed()
    return {"video_id": video_id, "status": "failed", "title": title, "error": "All download URLs failed"}


def _attempt_download(
    url: str, filepath: Path, video_id: str, title: str, tracker: ProgressTracker,
    storage_key: str = "",
) -> dict:
    """Attempt to download from a specific URL with retries."""
    temp_path = filepath.with_suffix(filepath.suffix + ".part")

    for attempt in range(MAX_RETRIES):
        try:
            # Support resuming partial downloads
            headers = {}
            # Authenticate with the Storage Zone password via header
            if storage_key:
                headers["AccessKey"] = storage_key
            start_byte = 0
            if temp_path.exists():
                start_byte = temp_path.stat().st_size
                headers["Range"] = f"bytes={start_byte}-"

            resp = requests.get(
                url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT
            )

            if resp.status_code == 404:
                return {"video_id": video_id, "status": "not_found"}

            if resp.status_code not in (200, 206):
                raise requests.HTTPError(f"HTTP {resp.status_code}")

            # Get total size
            content_length = resp.headers.get("Content-Length")
            total_size = int(content_length) + start_byte if content_length else None

            # Write to temp file
            mode_flag = "ab" if start_byte > 0 and resp.status_code == 206 else "wb"
            downloaded = start_byte if mode_flag == "ab" else 0

            with open(temp_path, mode_flag) as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

            # Rename temp file to final
            temp_path.rename(filepath)
            file_size = filepath.stat().st_size

            log.info(f"✓ Downloaded: {title[:60]} ({file_size / (1024*1024):.1f} MB)")
            tracker.mark_completed(file_size)

            return {
                "video_id": video_id,
                "status": "success",
                "path": str(filepath),
                "size_bytes": file_size,
            }

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                log.warning(
                    f"Download failed for '{title[:40]}' (attempt {attempt+1}/{MAX_RETRIES}), "
                    f"retrying in {wait}s: {e}"
                )
                time.sleep(wait)
            else:
                log.error(f"✗ Failed to download '{title[:60]}' after {MAX_RETRIES} attempts: {e}")
                # Clean up partial file on final failure
                if temp_path.exists():
                    pass  # Keep partial for potential resume on re-run
                return {"video_id": video_id, "status": "failed", "error": str(e)}

    return {"video_id": video_id, "status": "failed"}


# ─────────────────────────── Manifest ────────────────────────────────

def load_manifest(path: Path) -> dict:
    """Load the download manifest from disk."""
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {"videos": {}, "started_at": None, "last_updated": None}


def save_manifest(manifest: dict, path: Path):
    """Save the download manifest to disk."""
    manifest["last_updated"] = datetime.now().isoformat()
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


# ─────────────────────────── Export Links ────────────────────────────

def _export_links(videos: list[dict], cdn_hostname: str, mode: str, output_dir: Path, fmt: str):
    """
    Export download URLs for all videos to files without downloading.
    Outputs: .txt (one URL per line), .json (structured), and/or .csv.
    """
    import csv as csv_mod

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    link_entries = []

    for video in videos:
        video_id = video.get("guid") or video.get("Guid") or video.get("videoId")
        title = video.get("title") or video.get("Title") or "Untitled"
        url, filename = build_download_url(video, cdn_hostname, mode)

        entry = {
            "video_id": video_id,
            "title": title,
            "filename": filename,
            "download_url": url,
        }

        # For mp4_fallback, include only the resolutions actually available for this video
        if mode == "mp4_fallback":
            available_res = get_video_resolutions(video)
            entry["selected_resolution"] = f"{available_res[0]}p" if available_res else "unknown"
            entry["available_resolution_urls"] = {
                f"{res}p": f"https://{cdn_hostname}/{video_id}/play_{res}p.mp4"
                for res in available_res
            }

        link_entries.append(entry)

    files_written = []

    # ── TXT: one URL per line (compatible with wget -i, aria2c -i, etc.) ──
    if fmt in ("all", "txt"):
        txt_path = output_dir / f"download_links_{timestamp}.txt"
        with open(txt_path, "w") as f:
            for entry in link_entries:
                f.write(entry["download_url"] + "\n")
        files_written.append(("TXT", txt_path))

    # ── JSON: structured data with all metadata ──
    if fmt in ("all", "json"):
        json_path = output_dir / f"download_links_{timestamp}.json"
        export_data = {
            "generated_at": datetime.now().isoformat(),
            "mode": mode,
            "cdn_hostname": cdn_hostname,
            "total_videos": len(link_entries),
            "videos": link_entries,
        }
        with open(json_path, "w") as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        files_written.append(("JSON", json_path))

    # ── CSV: spreadsheet-friendly ──
    if fmt in ("all", "csv"):
        csv_path = output_dir / f"download_links_{timestamp}.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv_mod.writer(f)
            writer.writerow(["#", "Video ID", "Title", "Download URL", "Filename"])
            for i, entry in enumerate(link_entries, 1):
                writer.writerow([
                    i,
                    entry["video_id"],
                    entry["title"],
                    entry["download_url"],
                    entry["filename"],
                ])
        files_written.append(("CSV", csv_path))

    # ── Summary ──
    log.info("\n" + "=" * 60)
    log.info("DOWNLOAD LINKS EXPORTED")
    log.info("=" * 60)
    log.info(f"  Total videos: {len(link_entries)}")
    log.info(f"  Mode:         {mode}")
    log.info("")
    for label, path in files_written:
        log.info(f"  📄 {label}: {path}")
    log.info("")
    log.info("  Usage tips:")
    log.info("    • wget:   wget -i download_links_*.txt")
    log.info("    • aria2:  aria2c -i download_links_*.txt -j 4")
    log.info("    • curl:   xargs -n1 curl -O < download_links_*.txt")
    log.info("=" * 60)


# ─────────────────────────── Main ────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Bunny.net Stream Video Bulk Downloader")
    parser.add_argument("--library-id", default=LIBRARY_ID, help="Bunny.net Stream Library ID")
    parser.add_argument("--api-key", default=API_KEY, help="Bunny.net Stream API Key")
    parser.add_argument("--cdn-hostname", default=CDN_HOSTNAME, help="CDN hostname (e.g., vz-abc123.b-cdn.net)")
    parser.add_argument("--mode", default=DOWNLOAD_MODE, choices=["original", "mp4_fallback"], help="Download mode")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--storage-key", default=STORAGE_KEY, help="Bunny.net Storage Zone password (FTP/API password). Found in Storage dashboard → FTP & API Access. REQUIRED for downloading originals. Defaults to --api-key if not set.")
    parser.add_argument("--concurrency", type=int, default=MAX_CONCURRENT_DOWNLOADS, help="Max parallel downloads")
    parser.add_argument("--list-only", action="store_true", help="Only list videos, don't download")
    parser.add_argument("--links-only", action="store_true", help="Export all download URLs to .txt and .json files without downloading")
    parser.add_argument("--links-format", default="all", choices=["all", "txt", "json", "csv"], help="Output format for --links-only (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded without downloading")
    parser.add_argument("--collection", default="", help="Only download videos from this collection ID (Bunny.net collection GUID)")
    args = parser.parse_args()

    # Validate configuration
    if not args.library_id:
        log.error("BUNNY_LIBRARY_ID is required. Set it via --library-id or BUNNY_LIBRARY_ID env var.")
        sys.exit(1)
    if not args.api_key:
        log.error("BUNNY_API_KEY is required. Set it via --api-key or BUNNY_API_KEY env var.")
        sys.exit(1)
    # Fall back to api_key if storage_key not provided
    if not args.storage_key:
        args.storage_key = args.api_key
    # CDN hostname is not required for --list-only
    needs_cdn = not args.list_only
    if needs_cdn and not args.cdn_hostname:
        log.error(
            "BUNNY_CDN_HOSTNAME is required. Set it via --cdn-hostname or BUNNY_CDN_HOSTNAME env var.\n"
            "Find this in your Bunny.net dashboard → Stream → Your Library → Hostname.\n"
            "It looks like: vz-abc12345-xyz.b-cdn.net"
        )
        sys.exit(1)

    # Create output directory
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "_download_manifest.json"

    log.info("=" * 60)
    log.info("Bunny.net Stream Video Bulk Downloader")
    log.info("=" * 60)
    log.info(f"Library ID:  {args.library_id}")
    log.info(f"CDN Host:    {args.cdn_hostname}")
    log.info(f"Mode:        {args.mode}")
    log.info(f"Output:      {output_dir}")
    log.info(f"Concurrency: {args.concurrency}")
    if args.collection:
        log.info(f"Collection:  {args.collection}")
    log.info("=" * 60)

    # Step 1: List all videos
    videos = list_all_videos(args.library_id, args.api_key, collection_id=args.collection)

    if not videos:
        log.warning("No videos found in the library.")
        sys.exit(0)

    if args.list_only:
        log.info("\n--- Video List ---")
        for i, v in enumerate(videos, 1):
            vid = v.get("guid") or v.get("Guid") or v.get("videoId")
            title = v.get("title") or v.get("Title") or "Untitled"
            status = v.get("status") or v.get("Status", "?")
            log.info(f"  {i:4d}. [{vid}] {title} (status: {status})")
        log.info(f"\nTotal: {len(videos)} videos")
        sys.exit(0)

    if args.links_only:
        _export_links(videos, args.cdn_hostname, args.mode, output_dir, args.links_format)
        sys.exit(0)

    # Step 2: Load manifest for resume tracking
    manifest = load_manifest(manifest_path)
    if not manifest["started_at"]:
        manifest["started_at"] = datetime.now().isoformat()

    # Save the full video list to manifest
    for v in videos:
        vid = v.get("guid") or v.get("Guid") or v.get("videoId")
        if vid not in manifest["videos"]:
            manifest["videos"][vid] = {
                "title": v.get("title") or v.get("Title") or "Untitled",
                "status": "pending",
            }
    save_manifest(manifest, manifest_path)

    if args.dry_run:
        log.info("\n--- Dry Run ---")
        for v in videos:
            url, filename = build_download_url(v, args.cdn_hostname, args.mode)
            filepath = output_dir / filename
            exists = "✓ EXISTS" if filepath.exists() else "  NEW"
            log.info(f"  {exists} → {filename}")
            log.info(f"         URL: {url}")
        log.info(f"\nTotal: {len(videos)} videos")
        sys.exit(0)

    # Step 3: Download all videos in parallel
    tracker = ProgressTracker(total=len(videos))
    results = []

    log.info(f"\nStarting download of {len(videos)} videos with {args.concurrency} parallel workers...\n")

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(
                download_video, video, args.cdn_hostname, output_dir, args.mode, tracker, args.storage_key
            ): video
            for video in videos
        }

        for future in as_completed(futures):
            video = futures[future]
            try:
                result = future.result()
                results.append(result)

                # Update manifest
                vid = result.get("video_id")
                if vid and vid in manifest["videos"]:
                    manifest["videos"][vid]["status"] = result["status"]
                    if "path" in result:
                        manifest["videos"][vid]["path"] = result["path"]
                    if "size_bytes" in result:
                        manifest["videos"][vid]["size_bytes"] = result["size_bytes"]
                    if "error" in result:
                        manifest["videos"][vid]["error"] = result["error"]

                # Save manifest periodically (every 10 results)
                if len(results) % 10 == 0:
                    save_manifest(manifest, manifest_path)

            except Exception as e:
                vid = video.get("guid") or video.get("Guid") or "unknown"
                log.error(f"Unexpected error for video {vid}: {e}")
                results.append({"video_id": vid, "status": "error", "error": str(e)})
                tracker.mark_failed()

    # Final manifest save
    save_manifest(manifest, manifest_path)

    # Summary
    successful = sum(1 for r in results if r["status"] == "success")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed = sum(1 for r in results if r["status"] in ("failed", "error"))
    not_found = sum(1 for r in results if r["status"] == "not_found")

    log.info("\n" + "=" * 60)
    log.info("DOWNLOAD COMPLETE")
    log.info("=" * 60)
    log.info(f"  ✓ Successfully downloaded: {successful}")
    log.info(f"  ⏭ Skipped (already exist):  {skipped}")
    log.info(f"  ✗ Failed:                   {failed}")
    if not_found:
        log.info(f"  ⚠ Not found (404):          {not_found}")
    log.info(f"  Total:                      {len(results)}")
    log.info(f"\n  Manifest saved to: {manifest_path}")
    log.info(f"  Videos saved to:   {output_dir}")

    if failed > 0:
        log.info("\n  To retry failed downloads, simply run the script again.")
        log.info("  Already-downloaded files will be automatically skipped.")

        # List failed videos
        log.info("\n  Failed videos:")
        for r in results:
            if r["status"] in ("failed", "error"):
                log.info(f"    - {r.get('video_id')}: {r.get('error', 'unknown error')}")

    if not_found > 0:
        if args.mode == "original":
            log.warning(
                "\n  ⚠ Some videos returned 404. Make sure 'Keep Original File' was enabled\n"
                "    in your Bunny.net library BEFORE these videos were uploaded.\n"
                "    You can try --mode mp4_fallback to download MP4 renditions instead."
            )
        else:
            log.warning(
                "\n  ⚠ Some videos returned 404. Make sure 'MP4 Fallback' is enabled\n"
                "    in your Bunny.net library encoding settings."
            )

    log.info("=" * 60)


if __name__ == "__main__":
    main()
