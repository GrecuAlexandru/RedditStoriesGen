import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv"}
DEFAULT_VIDEO_FOLDERS = ["assets/videos1",
                         "assets/videos2", "assets/videos3", "assets/videos4"]


def get_video_dimensions(video_path: Path) -> tuple[int, int]:
    probe_cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_streams",
        str(video_path),
    ]
    result = subprocess.run(
        probe_cmd, capture_output=True, text=True, check=True)
    probe_data = json.loads(result.stdout)

    video_stream = None
    for stream in probe_data.get("streams", []):
        if stream.get("codec_type") == "video":
            video_stream = stream
            break

    if not video_stream:
        raise ValueError(f"No video stream found in {video_path}")

    return int(video_stream["width"]), int(video_stream["height"])


def get_video_duration(video_path: Path) -> float:
    probe_cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_entries",
        "format=duration",
        str(video_path),
    ]
    result = subprocess.run(
        probe_cmd, capture_output=True, text=True, check=True)
    probe_data = json.loads(result.stdout)
    duration_str = (probe_data.get("format") or {}).get("duration")
    if not duration_str:
        return 0.0
    try:
        return float(duration_str)
    except Exception:
        return 0.0


def build_crop_filter(width: int, height: int) -> str:
    target_width = int(height * 9 / 16)

    if target_width > width:
        target_height = int(width * 16 / 9)
        target_width = width
        crop_filter = f"crop={target_width}:{target_height}:0:{(height - target_height) // 2}"
    else:
        target_height = height
        crop_x = (width - target_width) // 2
        crop_filter = f"crop={target_width}:{target_height}:{crop_x}:0"

    return f"{crop_filter},scale=1080:1920"


def _run_ffmpeg_with_progress(ffmpeg_cmd: list[str], total_duration_seconds: float):
    process = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    last_percent = -1
    assert process.stdout is not None
    for line in process.stdout:
        line = line.strip()
        if not line.startswith("out_time_ms="):
            continue

        out_time_ms_str = line.split("=", 1)[1].strip()
        if not out_time_ms_str.isdigit():
            continue

        out_time_seconds = int(out_time_ms_str) / 1_000_000.0
        if total_duration_seconds <= 0:
            continue

        percent = int(
            min(100, (out_time_seconds / total_duration_seconds) * 100))
        if percent != last_percent:
            sys.stdout.write(f"\r     progress: {percent:3d}%")
            sys.stdout.flush()
            last_percent = percent

    return_code = process.wait()
    if last_percent >= 0:
        sys.stdout.write("\r     progress: 100%\n")
        sys.stdout.flush()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, ffmpeg_cmd)


def process_video(video_path: Path, overwrite: bool = True, threads: int = 0) -> bool:
    width, height = get_video_dimensions(video_path)
    duration_seconds = get_video_duration(video_path)
    filter_chain = build_crop_filter(width, height)

    temp_output = video_path.with_name(
        f"{video_path.stem}.__crop_tmp__{video_path.suffix}")
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-progress",
        "pipe:1",
        "-nostats",
        "-loglevel",
        "error",
        "-i",
        str(video_path),
        "-vf",
        filter_chain,
        "-c:v",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "23",
        "-c:a",
        "copy",
        str(temp_output),
    ]

    if threads and threads > 0:
        ffmpeg_cmd[2:2] = ["-threads", str(threads)]

    _run_ffmpeg_with_progress(ffmpeg_cmd, duration_seconds)

    out_w, out_h = get_video_dimensions(temp_output)
    if out_w != 1080 or out_h != 1920:
        raise RuntimeError(
            f"Output dimensions incorrect for {video_path}: {out_w}x{out_h}, expected 1080x1920"
        )

    if overwrite:
        os.replace(temp_output, video_path)
    return True


def iter_videos(folder: Path):
    if not folder.exists() or not folder.is_dir():
        return
    for file_path in sorted(folder.iterdir()):
        if file_path.is_file() and file_path.suffix.lower() in VIDEO_EXTENSIONS:
            yield file_path


def main():
    parser = argparse.ArgumentParser(
        description="Apply the same 9:16 crop+scale algorithm used by the generator to all videos in assets/videos1..4"
    )
    parser.add_argument(
        "--folders",
        nargs="*",
        default=DEFAULT_VIDEO_FOLDERS,
        help="Video folders to process (default: assets/videos1 assets/videos2 assets/videos3 assets/videos4)",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Do not overwrite originals (keeps temporary cropped output files)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=0,
        help="Limit FFmpeg threads per file (0 = ffmpeg default)",
    )
    args = parser.parse_args()

    total = 0
    succeeded = 0
    failed = 0

    for folder_str in args.folders:
        folder = Path(folder_str)
        print(f"\nProcessing folder: {folder}")
        if not folder.exists():
            print(f"  Skipping (folder not found): {folder}")
            continue

        for video_path in iter_videos(folder):
            total += 1
            print(f"  -> {video_path.name}")
            try:
                process_video(
                    video_path,
                    overwrite=not args.keep_temp,
                    threads=args.threads,
                )
                succeeded += 1
                print("     ✓ done")
            except Exception as exc:
                failed += 1
                print(f"     ✗ failed: {exc}")

    print("\nFinished")
    print(f"  Total: {total}")
    print(f"  Succeeded: {succeeded}")
    print(f"  Failed: {failed}")


if __name__ == "__main__":
    main()
