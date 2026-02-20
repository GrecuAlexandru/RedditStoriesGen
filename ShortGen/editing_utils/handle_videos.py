import os
import random

import subprocess
import json


def extract_random_clip_from_video(
    video_url, video_duration, clip_duration, output_file
):
    """Extracts a clip from a video using a signed URL.
    Args:
        video_url (str): The signed URL of the video.
        video_url (int): Duration of the video.
        start_time (int): The start time of the clip in seconds.
        clip_duration (int): The duration of the clip in seconds.
        output_file (str): The output file path for the extracted clip.
    """
    if not video_duration:
        raise Exception("Could not get video duration")
    if not video_duration * 0.7 > 120:
        raise Exception("Video too short")
    start_time = video_duration * 0.15 + random.random() * (
        0.7 * video_duration - clip_duration
    )

    command = [
        "ffmpeg",
        "-loglevel",
        "error",
        "-ss",
        str(start_time),
        "-t",
        str(clip_duration),
        "-i",
        video_url,
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        output_file,
    ]

    subprocess.run(command, check=True)

    if not os.path.exists(output_file):
        raise Exception("Random clip failed to be written")
    return output_file


def get_aspect_ratio(video_file):
    cmd = (
        'ffprobe -i "{}" -v quiet -print_format json -show_format -show_streams'.format(
            video_file
        )
    )
    #     jsonstr = subprocess.getoutput(cmd)
    jsonstr = subprocess.check_output(cmd, shell=True, encoding="utf-8")
    r = json.loads(jsonstr)
    # look for "codec_type": "video". take the 1st one if there are mulitple
    video_stream_info = [x for x in r["streams"] if x["codec_type"] == "video"][0]
    if (
        "display_aspect_ratio" in video_stream_info
        and video_stream_info["display_aspect_ratio"] != "0:1"
    ):
        a, b = video_stream_info["display_aspect_ratio"].split(":")
        dar = int(a) / int(b)
    else:
        # some video do not have the info of 'display_aspect_ratio'
        w, h = video_stream_info["width"], video_stream_info["height"]
        dar = int(w) / int(h)
        # not sure if we should use this
        # cw,ch = video_stream_info['coded_width'], video_stream_info['coded_height']
        # sar = int(cw)/int(ch)
    if (
        "sample_aspect_ratio" in video_stream_info
        and video_stream_info["sample_aspect_ratio"] != "0:1"
    ):
        # some video do not have the info of 'sample_aspect_ratio'
        a, b = video_stream_info["sample_aspect_ratio"].split(":")
        sar = int(a) / int(b)
    else:
        sar = dar
    par = dar / sar
    return dar
