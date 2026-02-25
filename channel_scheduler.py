import argparse
import datetime as dt
import importlib
import json
import os
import random
import shutil
import time
import traceback
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from scrapper import (
    fetch_and_process_posts,
    get_last_fetch_time,
    get_next_queued_post,
    mark_post_as_used,
    set_last_fetch_time,
    setup_database,
)
from ShortGen.audio.qwen3_voice_module import Qwen3VoiceModule
from ShortGen.config.languages import Language
from ShortGen.engine.reddit_short_engine import RedditShortEngine
from logger_utils import configure_logging
from notification_utils import send_gmail_notification

logger = configure_logging("ChannelScheduler")
load_dotenv()


def send_event_email(subject: str, body: str):
    send_gmail_notification(subject=subject, body=body)


def build_youtube_post_link(video_id: Optional[str]) -> Optional[str]:
    if not video_id:
        return None
    return f"https://www.youtube.com/shorts/{video_id}"


def extract_tiktok_post_link(upload_result: Any) -> Optional[str]:
    if isinstance(upload_result, str):
        normalized = upload_result.strip()
        if normalized.startswith("http://") or normalized.startswith("https://"):
            return normalized

    if isinstance(upload_result, dict):
        for key in ("url", "video_url", "share_url", "link"):
            value = upload_result.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                return value

    if isinstance(upload_result, (list, tuple)):
        for item in upload_result:
            candidate = extract_tiktok_post_link(item)
            if candidate:
                return candidate

    for attr in ("url", "video_url", "share_url", "link"):
        value = getattr(upload_result, attr, None)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return value

    return None


def safe_remove_file(file_path: Optional[str], label: str):
    if not file_path:
        return
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info("Cleanup: removed %s -> %s", label, file_path)
    except Exception as exc:
        logger.warning("Cleanup: failed removing %s (%s): %s",
                       label, file_path, exc)


def safe_remove_dir_if_empty(folder_path: Optional[str], label: str):
    if not folder_path:
        return
    try:
        if os.path.isdir(folder_path) and not os.listdir(folder_path):
            os.rmdir(folder_path)
            logger.info("Cleanup: removed empty %s -> %s", label, folder_path)
    except Exception as exc:
        logger.warning("Cleanup: failed removing empty %s (%s): %s",
                       label, folder_path, exc)


def safe_remove_dir_tree(folder_path: Optional[str], label: str):
    if not folder_path:
        return
    try:
        if os.path.isdir(folder_path):
            shutil.rmtree(folder_path)
            logger.info("Cleanup: removed %s -> %s", label, folder_path)
    except Exception as exc:
        logger.warning("Cleanup: failed removing %s (%s): %s",
                       label, folder_path, exc)


def format_elapsed(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    minutes, rem_seconds = divmod(total_seconds, 60)
    hours, rem_minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {rem_minutes}m {rem_seconds}s"
    if rem_minutes > 0:
        return f"{rem_minutes}m {rem_seconds}s"
    return f"{rem_seconds}s"


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def list_files(folder_path: str, extensions: tuple[str, ...]) -> List[str]:
    if not os.path.exists(folder_path):
        return []
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f)) and f.lower().endswith(extensions)
    ]


def parse_hhmm(value: str) -> tuple[int, int]:
    hour_str, minute_str = value.split(":")
    return int(hour_str), int(minute_str)


def parse_iso_datetime(value: Optional[str]) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def log_youtube_token_expiry(channels: List[Dict[str, Any]]):
    now_utc = dt.datetime.now(dt.timezone.utc)
    for channel in channels:
        if not channel.get("enabled", True):
            continue
        if channel.get("platform", "").lower().strip() != "youtube":
            continue

        channel_id = channel.get("id", "unknown")
        token_file = channel.get("youtube", {}).get("token_file")
        if not token_file:
            logger.warning("[%s] No token_file configured.", channel_id)
            continue

        if not os.path.exists(token_file):
            logger.warning(
                "[%s] Token file not found yet: %s (it will be created after first OAuth login)",
                channel_id,
                token_file,
            )
            continue

        try:
            with open(token_file, "r", encoding="utf-8") as token_f:
                token_data = json.load(token_f)
        except Exception as exc:
            logger.warning("[%s] Could not read token file %s: %s",
                           channel_id, token_file, exc)
            continue

        expiry_raw = token_data.get("expiry")
        has_refresh_token = bool(token_data.get("refresh_token"))
        expiry_utc = parse_iso_datetime(expiry_raw)
        if not expiry_utc:
            logger.warning(
                "[%s] Token expiry not present/parseable in %s", channel_id, token_file)
            continue

        seconds_left = (expiry_utc - now_utc).total_seconds()
        if seconds_left <= 0:
            if has_refresh_token:
                logger.info(
                    "[%s] YouTube access token expired at %s, but refresh_token exists (auto-refresh expected).",
                    channel_id,
                    expiry_utc.isoformat(),
                )
            else:
                logger.warning("[%s] YouTube access token expired at %s and no refresh_token found.",
                               channel_id, expiry_utc.isoformat())
                send_event_email(
                    subject=f"[RedditStoriesGen] YouTube token expired ({channel_id})",
                    body=(
                        f"Status: WARNING\n"
                        f"Platform: YouTube\n"
                        f"Channel: {channel_id}\n"
                        f"Token file: {token_file}\n"
                        f"Expired at (UTC): {expiry_utc.isoformat()}\n"
                        f"Detected at (UTC): {now_utc.isoformat()}\n"
                        f"refresh_token: missing"
                    ),
                )
        else:
            hours = int(seconds_left // 3600)
            minutes = int((seconds_left % 3600) // 60)
            if has_refresh_token:
                logger.info(
                    "[%s] YouTube access token expires at %s (in %dh %dm). refresh_token present (auto-refresh enabled).",
                    channel_id,
                    expiry_utc.isoformat(),
                    hours,
                    minutes,
                )
            else:
                logger.warning(
                    "[%s] YouTube access token expires at %s (in %dh %dm) and no refresh_token found.",
                    channel_id,
                    expiry_utc.isoformat(),
                    hours,
                    minutes,
                )


def choose_random_or_raise(items: List[str], label: str) -> str:
    if not items:
        raise FileNotFoundError(f"No {label} assets found")
    return random.choice(items)


def normalize_hashtags(hashtags: List[str]) -> List[str]:
    cleaned = []
    for tag in hashtags or []:
        if not isinstance(tag, str):
            continue
        tag = tag.strip()
        if not tag:
            continue
        if not tag.startswith("#"):
            tag = f"#{tag.replace(' ', '')}"
        cleaned.append(tag)
    return cleaned


def get_metadata(post: Dict[str, Any]) -> Dict[str, Any]:
    metadata = post.get("metadata") or {}
    hashtags = normalize_hashtags(metadata.get("hashtags", []))

    title = metadata.get("youtube_title") or post["title"]
    youtube_description = metadata.get("youtube_description") or post["title"]
    tiktok_description = metadata.get("tiktok_description") or post["title"]

    hash_text = " ".join(hashtags)
    return {
        "youtube_title": str(title)[:90],
        "youtube_description": f"{youtube_description}\n\n{hash_text}".strip(),
        "tiktok_description": f"{tiktok_description} {hash_text}".strip(),
        "hashtags": hashtags,
    }


def youtube_auth_service(channel_cfg: Dict[str, Any]):
    try:
        Credentials = importlib.import_module(
            "google.oauth2.credentials").Credentials
        InstalledAppFlow = importlib.import_module(
            "google_auth_oauthlib.flow").InstalledAppFlow
        Request = importlib.import_module(
            "google.auth.transport.requests").Request
        build = importlib.import_module("googleapiclient.discovery").build
    except ImportError as exc:
        raise ImportError(
            "Missing Google API dependencies. Install: google-api-python-client google-auth-oauthlib google-auth-httplib2"
        ) from exc

    scopes = ["https://www.googleapis.com/auth/youtube.upload"]
    client_secret = channel_cfg["client_secrets_file"]
    token_file = channel_cfg["token_file"]

    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, scopes)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret, scopes)
            creds = flow.run_local_server(port=0)
        with open(token_file, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    return build("youtube", "v3", credentials=creds)


def upload_to_youtube(channel: Dict[str, Any], video_path: str, metadata: Dict[str, Any]):
    MediaFileUpload = importlib.import_module(
        "googleapiclient.http").MediaFileUpload

    yt_cfg = channel["youtube"]
    youtube = youtube_auth_service(yt_cfg)

    body = {
        "snippet": {
            "title": metadata["youtube_title"],
            "description": metadata["youtube_description"],
            "tags": metadata["hashtags"],
            "categoryId": str(yt_cfg.get("category_id", "22")),
        },
        "status": {
            "privacyStatus": yt_cfg.get("privacy_status", "public"),
            "selfDeclaredMadeForKids": bool(yt_cfg.get("made_for_kids", False)),
        },
    }

    schedule_minutes = yt_cfg.get("schedule_minutes_from_now")
    if isinstance(schedule_minutes, int) and schedule_minutes > 0:
        publish_time = dt.datetime.now(
            dt.timezone.utc) + dt.timedelta(minutes=schedule_minutes)
        body["status"]["privacyStatus"] = "private"
        body["status"]["publishAt"] = publish_time.replace(
            microsecond=0).isoformat().replace("+00:00", "Z")

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=MediaFileUpload(video_path, chunksize=-1, resumable=True),
    )
    response = request.execute()
    return response.get("id")


def upload_to_tiktok(channel: Dict[str, Any], video_path: str, metadata: Dict[str, Any]):
    try:
        TikTokUploader = importlib.import_module(
            "tiktok_uploader.upload").TikTokUploader
    except ImportError as exc:
        raise ImportError("Missing dependency: tiktok-uploader") from exc

    tt_cfg = channel["tiktok"]
    uploader = TikTokUploader(
        cookies=tt_cfg["cookies_file"],
        browser=tt_cfg.get("browser", "chrome"),
        headless=False,
    )

    kwargs = {
        "description": metadata["tiktok_description"],
        "comment": bool(tt_cfg.get("comment", True)),
        "stitch": bool(tt_cfg.get("stitch", True)),
        "duet": bool(tt_cfg.get("duet", True)),
    }

    if tt_cfg.get("product_id"):
        kwargs["product_id"] = tt_cfg["product_id"]

    if tt_cfg.get("cover"):
        kwargs["cover"] = tt_cfg["cover"]

    upload_result = None
    try:
        upload_result = uploader.upload_video(video_path, **kwargs)
    finally:
        for method_name in ("close", "quit"):
            method = getattr(uploader, method_name, None)
            if callable(method):
                try:
                    method()
                except Exception:
                    pass

        for attr_name in ("browser", "driver"):
            obj = getattr(uploader, attr_name, None)
            if obj is None:
                continue
            for method_name in ("close", "quit"):
                method = getattr(obj, method_name, None)
                if callable(method):
                    try:
                        method()
                    except Exception:
                        pass

    if isinstance(upload_result, bool):
        if not upload_result:
            raise RuntimeError(
                "TikTok uploader reported upload failure (returned False). "
                "Check tiktok_uploader logs above for the exact UI step that failed."
            )
        return None

    return extract_tiktok_post_link(upload_result)


def generate_variant_video(
    voice_module: Qwen3VoiceModule,
    post: Dict[str, Any],
    channel: Dict[str, Any],
    shared_tts_audio_path: str,
    background_music_path: str,
    output_root: str,
) -> str:
    render_start = time.perf_counter()
    channel_id = channel["id"]
    video_folder = channel["assets"]["video_folder"]
    video_files = list_files(video_folder, (".mp4", ".mov", ".avi", ".mkv"))
    background_video = choose_random_or_raise(
        video_files, f"video files in {video_folder}")

    logger.info(
        "[%s] Render start (progress: video selection complete, %s candidates)",
        channel_id,
        len(video_files),
    )

    short_id = f"run_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{channel_id}"
    engine = RedditShortEngine(
        voice_module,
        background_video_name=background_video,
        background_music_name=background_music_path,
        reddit_link="IS_LOCAL",
        short_id=short_id,
        language=Language.ENGLISH,
        story_title=post["title"],
        story_content=post["content"],
        pre_generated_audio_path=shared_tts_audio_path,
    )

    for step_num, step_info in engine.makeContent():
        total_steps = engine.get_total_steps()
        progress_pct = int((step_num / total_steps) * 100)
        logger.info(
            "[%s] Step %s/%s (%s%%): %s",
            channel_id,
            step_num,
            total_steps,
            progress_pct,
            step_info,
        )

    if not os.path.exists(engine._db_video_path):
        raise FileNotFoundError(
            f"Video output not found for channel {channel_id}")

    run_folder = os.path.join(
        output_root, dt.datetime.now().strftime("%Y-%m-%d"))
    ensure_dir(run_folder)
    final_path = os.path.join(run_folder, f"{channel_id}.mp4")
    if os.path.exists(final_path):
        os.remove(final_path)
    os.replace(engine._db_video_path, final_path)

    if hasattr(engine, "dynamicAssetDir"):
        safe_remove_dir_tree(
            getattr(engine, "dynamicAssetDir", None),
            f"dynamic assets for {channel_id}",
        )

    logger.info(
        "[%s] Render done in %s -> %s",
        channel_id,
        format_elapsed(time.perf_counter() - render_start),
        final_path,
    )
    return final_path


def build_shared_audio(voice_module: Qwen3VoiceModule, post: Dict[str, Any], output_root: str) -> str:
    tts_start = time.perf_counter()
    ensure_dir(output_root)
    text = f"{post['title']}\n\n{post['content']}"
    shared_audio_path = os.path.join(output_root, "shared_tts.wav")
    generated_path = voice_module.generate_voice(text, shared_audio_path)
    logger.info(
        "Shared audio generated in %s -> %s",
        format_elapsed(time.perf_counter() - tts_start),
        generated_path,
    )
    return generated_path


def run_pipeline_once(config: Dict[str, Any], fetch_if_queue_empty: bool = True):
    pipeline_start = time.perf_counter()
    logger.info("Pipeline started")
    conn = setup_database()
    post_run_folder = None
    shared_audio_path = None
    try:
        post = get_next_queued_post(conn)
        if not post and fetch_if_queue_empty:
            logger.info("Queue empty, fetching fresh posts first...")
            refill_start = time.perf_counter()
            fetch_and_process_posts(conn)
            logger.info(
                "Queue refill fetch completed in %s",
                format_elapsed(time.perf_counter() - refill_start),
            )
            post = get_next_queued_post(conn)

        if not post:
            logger.warning("No queued posts available after fetch.")
            return

        logger.info(
            "Pipeline stage 1/4: selected post rowid=%s, title='%s'",
            post["rowid"],
            post["title"][:80],
        )

        metadata = get_metadata(post)
        enabled_channels = [
            c for c in config.get("channels", []) if c.get("enabled", True)
        ]
        if not enabled_channels:
            logger.warning("No enabled channels in config.")
            return

        youtube_channels = [
            c
            for c in enabled_channels
            if c.get("platform", "").lower().strip() == "youtube"
        ]
        tiktok_channels = [
            c
            for c in enabled_channels
            if c.get("platform", "").lower().strip() == "tiktok"
        ]

        if len(tiktok_channels) > 1:
            logger.warning(
                "Multiple TikTok channels are enabled. Only the first one (%s) will be used.",
                tiktok_channels[0].get("id", "unknown"),
            )

        if not youtube_channels and not tiktok_channels:
            logger.warning(
                "No supported platforms enabled. Use youtube and/or tiktok.")
            return

        if not youtube_channels and tiktok_channels:
            logger.warning(
                "TikTok reuse mode needs at least one YouTube channel/video. "
                "Enable one YouTube channel so TikTok can reuse its first rendered video."
            )
            return

        logger.info(
            "Pipeline stage 2/4: channels ready (youtube=%s, tiktok=%s)",
            len(youtube_channels),
            len(tiktok_channels),
        )

        shared_cfg = config.get("shared", {})
        output_root = shared_cfg.get("output_root", "output/scheduled")
        audio_folder = shared_cfg.get("audio_folder", "assets/audios")

        background_music_files = list_files(audio_folder, (".mp3", ".wav"))
        background_music = choose_random_or_raise(
            background_music_files, f"audio files in {audio_folder}")

        voice_module = Qwen3VoiceModule()
        post_run_folder = os.path.join(
            output_root,
            dt.datetime.now().strftime("%Y-%m-%d"),
            f"post_{post['rowid']}"
        )
        ensure_dir(post_run_folder)

        shared_audio_path = build_shared_audio(
            voice_module, post, post_run_folder)

        logger.info("Pipeline stage 3/4: starting channel render/upload loop")

        success_count = 0
        tiktok_uploaded = False
        tiktok_channel = tiktok_channels[0] if tiktok_channels else None

        for channel_index, channel in enumerate(youtube_channels):
            channel_id = channel.get("id", "unknown")
            channel_start = time.perf_counter()
            variant_video = None
            try:
                logger.info(
                    "Channel progress %s/%s: %s",
                    channel_index + 1,
                    len(youtube_channels),
                    channel_id,
                )
                variant_video = generate_variant_video(
                    voice_module=voice_module,
                    post=post,
                    channel=channel,
                    shared_tts_audio_path=shared_audio_path,
                    background_music_path=background_music,
                    output_root=post_run_folder,
                )

                upload_start = time.perf_counter()
                video_id = upload_to_youtube(channel, variant_video, metadata)
                logger.info(
                    "Uploaded YouTube channel %s, videoId=%s in %s",
                    channel_id,
                    video_id,
                    format_elapsed(time.perf_counter() - upload_start),
                )
                youtube_link = build_youtube_post_link(video_id)
                send_event_email(
                    subject=f"[RedditStoriesGen] Posted to YouTube ({channel_id})",
                    body=(
                        f"Status: SUCCESS\n"
                        f"Platform: YouTube\n"
                        f"Channel: {channel_id}\n"
                        f"Video ID: {video_id}\n"
                        f"Link: {youtube_link or 'Unavailable'}\n"
                        f"Post rowid: {post.get('rowid')}\n"
                        f"Title: {post.get('title', '')}\n"
                        f"Generated video: {variant_video}\n"
                        f"Time (UTC): {dt.datetime.now(dt.timezone.utc).isoformat()}"
                    ),
                )
                success_count += 1

                if channel_index == 0 and tiktok_channel and not tiktok_uploaded:
                    tiktok_channel_id = tiktok_channel.get("id", "unknown")
                    try:
                        tiktok_start = time.perf_counter()
                        tiktok_link = upload_to_tiktok(
                            tiktok_channel, variant_video, metadata)
                        logger.info(
                            "Uploaded TikTok channel %s using first YouTube video in %s",
                            tiktok_channel_id,
                            format_elapsed(time.perf_counter() - tiktok_start),
                        )
                        send_event_email(
                            subject=f"[RedditStoriesGen] Posted to TikTok ({tiktok_channel_id})",
                            body=(
                                f"Status: SUCCESS\n"
                                f"Platform: TikTok\n"
                                f"Channel: {tiktok_channel_id}\n"
                                f"Link: {tiktok_link or 'Unavailable (TikTok uploader did not return a public URL)'}\n"
                                f"Post rowid: {post.get('rowid')}\n"
                                f"Title: {post.get('title', '')}\n"
                                f"Reused video: {variant_video}\n"
                                f"Time (UTC): {dt.datetime.now(dt.timezone.utc).isoformat()}"
                            ),
                        )
                        success_count += 1
                        tiktok_uploaded = True
                    except Exception as tiktok_exc:
                        logger.exception(
                            "TikTok channel %s failed while reusing first YouTube video: %s",
                            tiktok_channel_id,
                            tiktok_exc,
                        )
                        send_event_email(
                            subject=f"[RedditStoriesGen] ERROR on TikTok ({tiktok_channel_id})",
                            body=(
                                f"Status: ERROR\n"
                                f"Platform: TikTok\n"
                                f"Channel: {tiktok_channel_id}\n"
                                f"Post rowid: {post.get('rowid')}\n"
                                f"Title: {post.get('title', '')}\n"
                                f"Error: {tiktok_exc}\n\n"
                                f"Traceback:\n{traceback.format_exc()}"
                            ),
                        )
            except Exception as exc:
                logger.exception("Channel %s failed: %s", channel_id, exc)
                send_event_email(
                    subject=f"[RedditStoriesGen] ERROR on YouTube ({channel_id})",
                    body=(
                        f"Status: ERROR\n"
                        f"Platform: YouTube\n"
                        f"Channel: {channel_id}\n"
                        f"Post rowid: {post.get('rowid')}\n"
                        f"Title: {post.get('title', '')}\n"
                        f"Error: {exc}\n\n"
                        f"Traceback:\n{traceback.format_exc()}"
                    ),
                )
            finally:
                logger.info(
                    "Channel %s finished in %s",
                    channel_id,
                    format_elapsed(time.perf_counter() - channel_start),
                )
                safe_remove_file(
                    variant_video, f"generated video for {channel_id}")

        if success_count > 0:
            mark_post_as_used(conn, post["rowid"])
            logger.info("Marked rowid=%s as used after %s successful upload(s)",
                        post["rowid"], success_count)
        else:
            logger.warning(
                "No successful uploads for rowid=%s; keeping it unused for retry.", post["rowid"])
    except Exception as pipeline_exc:
        logger.exception(
            "Pipeline failed with an unhandled error: %s", pipeline_exc)
        send_event_email(
            subject="[RedditStoriesGen] ERROR in pipeline",
            body=(
                f"Status: ERROR\n"
                f"Context: run_pipeline_once\n"
                f"Error: {pipeline_exc}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            ),
        )
        raise
    finally:
        safe_remove_file(shared_audio_path, "shared TTS audio")

        nested_day_folder = None
        if post_run_folder:
            nested_day_folder = os.path.join(
                post_run_folder, dt.datetime.now().strftime("%Y-%m-%d")
            )

        safe_remove_dir_if_empty(nested_day_folder, "nested post date folder")
        safe_remove_dir_if_empty(post_run_folder, "post run folder")

        conn.close()
        logger.info(
            "Pipeline stage 4/4: complete in %s",
            format_elapsed(time.perf_counter() - pipeline_start),
        )


def run_fetch_job(force: bool = False, fetch_interval_hours: int = 24) -> bool:
    fetch_start = time.perf_counter()
    conn = setup_database()
    try:
        now_utc = dt.datetime.now(dt.timezone.utc)
        last_fetch_time = get_last_fetch_time(conn)

        if not force and last_fetch_time is not None:
            elapsed = now_utc - last_fetch_time
            cooldown = dt.timedelta(hours=fetch_interval_hours)
            if elapsed < cooldown:
                remaining = cooldown - elapsed
                remaining_hours = int(remaining.total_seconds() // 3600)
                remaining_minutes = int(
                    (remaining.total_seconds() % 3600) // 60)
                logger.info(
                    "Skipping fetch: last fetch was at %s UTC. Next allowed fetch in %dh %dm.",
                    last_fetch_time.isoformat(),
                    remaining_hours,
                    remaining_minutes,
                )
                logger.info(
                    "Fetch stage finished in %s (skipped)",
                    format_elapsed(time.perf_counter() - fetch_start),
                )
                return False

        fetch_and_process_posts(conn)
        set_last_fetch_time(conn, now_utc)
        logger.info("Fetch completed and timestamp updated: %s",
                    now_utc.isoformat())
        logger.info(
            "Fetch stage finished in %s",
            format_elapsed(time.perf_counter() - fetch_start),
        )
        return True
    finally:
        conn.close()


def start_scheduler(config: Dict[str, Any]):
    BlockingScheduler = importlib.import_module(
        "apscheduler.schedulers.blocking").BlockingScheduler
    CronTrigger = importlib.import_module(
        "apscheduler.triggers.cron").CronTrigger

    scheduler_cfg = config.get("scheduler", {})

    timezone = scheduler_cfg.get("timezone", "UTC")
    fetch_time = scheduler_cfg.get("daily_fetch_time", "00:10")
    fetch_interval_hours = int(scheduler_cfg.get("fetch_interval_hours", 24))
    publish_times = scheduler_cfg.get("daily_publish_times")
    if not publish_times:
        publish_times = [scheduler_cfg.get("daily_publish_time", "00:30")]

    if not isinstance(publish_times, list) or not publish_times:
        raise ValueError(
            "scheduler.daily_publish_times must be a non-empty list of HH:MM strings")

    fetch_hour, fetch_minute = parse_hhmm(fetch_time)

    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        lambda: run_fetch_job(
            force=False, fetch_interval_hours=fetch_interval_hours),
        trigger=CronTrigger(
            hour=fetch_hour, minute=fetch_minute, timezone=timezone),
        id="daily_fetch_posts",
        replace_existing=True,
    )
    for index, publish_time in enumerate(publish_times):
        publish_hour, publish_minute = parse_hhmm(publish_time)
        scheduler.add_job(
            lambda: run_pipeline_once(config),
            trigger=CronTrigger(hour=publish_hour,
                                minute=publish_minute, timezone=timezone),
            id=f"daily_generate_and_upload_{index + 1}",
            replace_existing=True,
        )

    logger.info(
        "Scheduler started. Fetch job at %s, upload jobs at %s (%s)",
        fetch_time,
        ", ".join(publish_times),
        timezone,
    )
    scheduler.start()


def main():
    parser = argparse.ArgumentParser(
        description="24/7 multi-channel scheduler for RedditStoriesGen")
    parser.add_argument("--config", default="channel_schedule.json",
                        help="Path to channel schedule JSON")
    parser.add_argument("--run-once", action="store_true",
                        help="Run fetch+generation+upload once and exit")
    parser.add_argument("--fetch-only", action="store_true",
                        help="Only run fetch_and_process_posts once and exit")
    parser.add_argument("--force-fetch", action="store_true",
                        help="Force fetch even if last fetch was less than the fetch interval")
    args = parser.parse_args()

    cfg = load_config(args.config)
    scheduler_cfg = cfg.get("scheduler", {})
    fetch_interval_hours = int(scheduler_cfg.get("fetch_interval_hours", 24))

    if args.fetch_only:
        run_fetch_job(force=args.force_fetch,
                      fetch_interval_hours=fetch_interval_hours)
        return

    log_youtube_token_expiry(cfg.get("channels", []))

    if args.run_once:
        run_once_start = time.perf_counter()
        run_fetch_job(force=args.force_fetch,
                      fetch_interval_hours=fetch_interval_hours)
        run_pipeline_once(cfg, fetch_if_queue_empty=False)
        logger.info(
            "Run-once finished in %s",
            format_elapsed(time.perf_counter() - run_once_start),
        )
        return

    start_scheduler(cfg)


if __name__ == "__main__":
    try:
        main()
    except Exception as main_exc:
        send_event_email(
            subject="[RedditStoriesGen] FATAL scheduler error",
            body=(
                f"Status: FATAL\n"
                f"Error: {main_exc}\n\n"
                f"Traceback:\n{traceback.format_exc()}"
            ),
        )
        raise
