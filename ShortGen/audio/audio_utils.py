import os
import subprocess
import time


# Arrived to this result after whispering a ton of shorts and calculating the average number of characters per second of speech.
CONST_CHARS_PER_SEC = 20.5

WHISPER_MODEL = None


def get_asset_duration(asset_path, is_video=True):
    if is_video:
        command = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            asset_path,
        ]
    else:
        command = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            asset_path,
        ]

    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    duration = float(result.stdout.decode("utf-8").strip())
    return asset_path, duration


def ChunkForAudio(alltext, chunk_size=2500):
    alltext_list = alltext.split(".")
    chunks = []
    curr_chunk = ""
    for text in alltext_list:
        if len(curr_chunk) + len(text) <= chunk_size:
            curr_chunk += text + "."
        else:
            chunks.append(curr_chunk)
            curr_chunk = text + "."
    if curr_chunk:
        chunks.append(curr_chunk)
    return chunks


def audioToText(filename, model_size="base"):
    from whisper_timestamped import load_model, transcribe_timestamped

    global WHISPER_MODEL
    if WHISPER_MODEL == None:
        WHISPER_MODEL = load_model(model_size)
    gen = transcribe_timestamped(WHISPER_MODEL, filename, verbose=False, fp16=False)
    return gen


def getWordsPerSec(filename):
    a = audioToText(filename)
    return len(a["text"].split()) / a["segments"][-1]["end"]


def getCharactersPerSec(filename):
    a = audioToText(filename)
    return len(a["text"]) / a["segments"][-1]["end"]


def run_background_audio_split(sound_file_path):
    try:
        # Run spleeter command
        # Get absolute path of sound file
        output_dir = os.path.dirname(sound_file_path)
        command = f"spleeter separate -p spleeter:2stems -o '{output_dir}' '{sound_file_path}'"

        process = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # If spleeter runs successfully, return the path to the background music file
        if process.returncode == 0:
            return os.path.join(
                output_dir,
                sound_file_path.split("/")[-1].split(".")[0],
                "accompaniment.wav",
            )
        else:
            return None
    except Exception:
        # If spleeter crashes, return None
        return None
