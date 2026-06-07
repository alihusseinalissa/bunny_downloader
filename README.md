# Bunny.net Stream Video Bulk Downloader

A robust, multi-threaded Python command-line utility to bulk download or export original files and MP4 fallbacks from a Bunny.net Stream library.

## Features

- **Paginated Listing**: Automatically lists all videos from Bunny.net Stream API (handles 1500+ videos).
- **Parallel Downloads**: Speeds up downloads using multi-threaded concurrency (configurable).
- **Resume Support**: 
  - Automatically skips fully downloaded files.
  - Resumes partially downloaded files using HTTP Range headers.
- **Multiple Download Modes**:
  - `original`: Downloads the original uploaded file via the Bunny.net Storage API.
  - `mp4_fallback`: Downloads the MP4 fallback rendition at the highest available resolution (e.g., 2160p, 1080p, 720p).
- **Filter by Collection**: Download only videos belonging to a specific Bunny.net collection ID.
- **Link Exporting**: Export structured download URLs to `.txt` (perfect for `wget`/`aria2`), `.json`, or `.csv` without initiating local downloads.
- **Dry-Run**: Preview which videos will be downloaded and verify URLs before starting.
- **Manifest Tracking**: Saves progress and download states to `_download_manifest.json` in the output directory.
- **Resilience**: Implements exponential backoff and retries for network resilience.

---

## Prerequisites

1. **Python 3.6+**
2. **`requests` library**:
   ```bash
   pip install requests
   ```

---

## Finding Credentials in Bunny.net

To run the script, you need the following configuration values from your [Bunny.net Dashboard](https://panel.bunny.net/):

1. **Library ID**: Go to **Stream** → Click your **Video Library**. The Library ID is listed under **API / Integration** (or is the numeric ID in the URL).
2. **Stream API Key**: Go to **Stream** → Click your **Video Library** → **API / Integration** → Copy the **API Key**.
3. **CDN Hostname**: Go to **Stream** → Click your **Video Library** → **API / Integration** → Copy the **Pull Zone Hostname** (e.g., `vz-12345abc-xyz.b-cdn.net`).
4. **Storage Zone Password (Storage Key)**: Required *only* for the `original` download mode. 
   - Go to **Storage** → Click the Storage Zone corresponding to your video library (usually sharing a similar name).
   - Go to **FTP & API Access** → Copy the **Password**. *(Note: This is different from the Stream API Key)*.

---

## Usage

You can run the script by passing arguments directly or by configuring environment variables.

### Method 1: Using Command-Line Arguments

#### 1. Download MP4 Fallbacks (Recommended)
Downloads the highest resolution available (e.g., 1080p, 720p) for all videos in the library:
```bash
python3 download_bunny_videos.py \
  --library-id "YOUR_LIBRARY_ID" \
  --api-key "YOUR_STREAM_API_KEY" \
  --cdn-hostname "vz-12345abc-xyz.b-cdn.net" \
  --mode mp4_fallback
```

#### 2. Download Original Uploaded Files
*Requires "Keep Original File" to be enabled in Bunny.net library settings before video upload.*
```bash
python3 download_bunny_videos.py \
  --library-id "YOUR_LIBRARY_ID" \
  --api-key "YOUR_STREAM_API_KEY" \
  --cdn-hostname "vz-12345abc-xyz.b-cdn.net" \
  --storage-key "YOUR_STORAGE_ZONE_PASSWORD" \
  --mode original
```

#### 3. Filter by Collection ID
Only download videos from a specific collection GUID:
```bash
python3 download_bunny_videos.py \
  --library-id "YOUR_LIBRARY_ID" \
  --api-key "YOUR_STREAM_API_KEY" \
  --cdn-hostname "vz-12345abc-xyz.b-cdn.net" \
  --collection "YOUR_COLLECTION_GUID"
```

#### 4. Export Links to File (No Download)
Export all video URLs to files in the output directory.
```bash
python3 download_bunny_videos.py \
  --library-id "YOUR_LIBRARY_ID" \
  --api-key "YOUR_STREAM_API_KEY" \
  --cdn-hostname "vz-12345abc-xyz.b-cdn.net" \
  --links-only \
  --links-format all
```
*Tip: You can use `wget -i download_links_*.txt` or `aria2c -i download_links_*.txt -j 4` to download with external tools.*

---

### Method 2: Environment Variables

You can set environment variables in your terminal shell or in a `.env` file (if you wrap execution) to avoid writing API keys in your command history:

```bash
export BUNNY_LIBRARY_ID="YOUR_LIBRARY_ID"
export BUNNY_API_KEY="YOUR_STREAM_API_KEY"
export BUNNY_CDN_HOSTNAME="vz-12345abc-xyz.b-cdn.net"
export BUNNY_STORAGE_KEY="YOUR_STORAGE_ZONE_PASSWORD"
export BUNNY_DOWNLOAD_MODE="original" # or "mp4_fallback"
export BUNNY_CONCURRENCY="6"
export BUNNY_OUTPUT_DIR="./my_videos"

python3 download_bunny_videos.py
```

---

## CLI Argument Reference

| Argument | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--library-id` | `BUNNY_LIBRARY_ID` | *None* | **Required.** Bunny.net Stream Library ID. |
| `--api-key` | `BUNNY_API_KEY` | *None* | **Required.** Bunny.net Stream API Access Key. |
| `--cdn-hostname` | `BUNNY_CDN_HOSTNAME`| *None* | **Required** (except for list-only). e.g., `vz-123.b-cdn.net`. |
| `--mode` | `BUNNY_DOWNLOAD_MODE` | `original` | `original` (from storage) or `mp4_fallback` (CDN mp4 delivery). |
| `--storage-key` | `BUNNY_STORAGE_KEY` | *API Key* | Storage Zone password. Found under Storage dashboard FTP & API Access. |
| `--output-dir` | `BUNNY_OUTPUT_DIR` | `./downloaded_videos` | Directory where downloaded videos and manifests will be saved. |
| `--concurrency` | `BUNNY_CONCURRENCY` | `4` | Maximum number of concurrent download threads. |
| `--collection` | *None* | `""` | Optional collection GUID to download only a specific collection. |
| `--list-only` | *None* | `False` | Prints out all video GUIDs and titles on screen, then exits. |
| `--links-only` | *None* | `False` | Exports URLs to a file without downloading. |
| `--links-format`| *None* | `all` | Format of exported links: `all`, `txt`, `json`, or `csv`. |
| `--dry-run` | *None* | `False` | Simulates download plan and prints target filenames and URLs. |

---

## Technical Details & Troubleshooting

### Resuming & Partial Files
- When a download is interrupted, a `.part` file remains.
- On the next run, the downloader reads the size of the `.part` file, requests only the remaining bytes via an HTTP `Range` request, and appends them to the existing file.
- Once completed, the `.part` extension is removed.

### Troubleshooting 404 Errors
If you run the script and see `404 Not Found` errors:
- **In `original` mode**: Ensure that **"Keep Original File"** is enabled in your Video Library Settings *before* videos are uploaded. Videos uploaded before enabling this setting do not have original files stored on Bunny.net. If so, switch to `--mode mp4_fallback`.
- **In `mp4_fallback` mode**: Ensure that **"MP4 Fallback"** is enabled in your library's **Encoding** settings. This allows Bunny.net to compile standalone `.mp4` versions instead of just `.m3u8` streams.
