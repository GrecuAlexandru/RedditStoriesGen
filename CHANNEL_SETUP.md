# Multi-Channel Setup (YouTube Shorts + TikTok)

This project now supports a JSON-driven scheduler that can post to many channels.

## 1) Create config file

Copy `channel_schedule.example.json` to `channel_schedule.json` and add all channels.
You can add as many objects as you want under `channels`.

- `platform: "youtube"` for YouTube Shorts channels
- `platform: "tiktok"` for TikTok channels
- each channel should point to its own `assets.video_folder` (`assets/videos1`, `assets/videos2`, ...)

To control how many times per day it posts, set:

- `scheduler.daily_publish_times`: list of `HH:MM` values
- Number of entries = number of post cycles per day

Example: `"daily_publish_times": ["03:00", "09:00", "13:00", "17:00", "20:00"]` means 5 post runs/day.

Backward compatibility: `daily_publish_time` (single value) is still supported.

## 2) YouTube per-channel requirements

For each YouTube channel account:

1. Create a Google Cloud project
2. Enable **YouTube Data API v3**
3. Create OAuth Client ID credentials (`Desktop app`)
4. Download the JSON file and store it (example: `secrets/youtube/yt_channel_1_client_secret.json`)
5. Set `youtube.client_secrets_file` and `youtube.token_file` in config

First upload for each channel opens OAuth browser flow and stores the refresh token in `token_file`.
To upload as different channels, authenticate each channel separately with its own secret/token files.

## 3) TikTok per-channel requirements

This scheduler uses `tiktok-uploader` (Playwright based).

1. Install package + browser runtime:
   - `pip install tiktok-uploader`
   - `playwright install`
2. Login to each TikTok account in your browser.
3. Export cookies in Netscape format to a separate file for each account:
   - Example: `secrets/tiktok/tiktok_channel_1_cookies.txt`
4. Set each channel’s `tiktok.cookies_file` to its matching cookie file.

If `sessionid` is missing from exported cookies, add it manually as documented in `tiktok-uploader`.

## 4) Scheduler behavior

- Daily job 1: runs `fetch_and_process_posts` from `scrapper.py`
- Daily job 2: selects one top queued Reddit post, generates one shared TTS audio, then generates YouTube videos for enabled YouTube channels.
- If a TikTok channel is enabled, it uploads the **first generated YouTube video** to TikTok as well (no separate TikTok render).
- Then it continues uploading the remaining YouTube channels.

If at least one upload succeeds, that queued post is marked as used.

Fetch cooldown:

- Last fetch time is persisted in `posts.db` (`SchedulerState.last_fetch_time`).
- Default fetch interval is 24 hours.
- Configure with `scheduler.fetch_interval_hours` in `channel_schedule.json`.
- To bypass cooldown manually, run with `--force-fetch`.

## 5) Run commands

- One-time fetch only:
  - `python channel_scheduler.py --config channel_schedule.json --fetch-only`
  - force fetch: `python channel_scheduler.py --config channel_schedule.json --fetch-only --force-fetch`
- One-time full run (fetch + generate + upload):
  - `python channel_scheduler.py --config channel_schedule.json --run-once`
  - force fetch: `python channel_scheduler.py --config channel_schedule.json --run-once --force-fetch`
- 24/7 scheduler:
  - `python channel_scheduler.py --config channel_schedule.json`

## 6) Recommended folder layout

Create your secrets folders:

- `secrets/youtube/`
- `secrets/tiktok/`

and keep credentials/cookies there.

## 7) Gmail alerts (post + error notifications)

Scheduler email notifications are sent on:

- successful YouTube upload
- successful TikTok upload
- channel upload errors and fatal pipeline errors

Set these environment variables before running:

- `GMAIL_SMTP_USER` = your Gmail address used to send alerts
- `GMAIL_SMTP_APP_PASSWORD` = Gmail App Password (not your normal password)
- `EMAIL_NOTIFY_RECIPIENTS` = optional comma-separated recipients

If `EMAIL_NOTIFY_RECIPIENTS` is not set, defaults are:

- `andreizdrali@gmail.com`
- `alexandru.grecu27@gmail.com`

PowerShell example:

- `$env:GMAIL_SMTP_USER="your_sender@gmail.com"`
- `$env:GMAIL_SMTP_APP_PASSWORD="your_16_char_app_password"`
- `$env:EMAIL_NOTIFY_RECIPIENTS="andreizdrali@gmail.com,alexandru.grecu27@gmail.com"`
