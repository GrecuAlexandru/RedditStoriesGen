import os
import argparse
import random
import shutil
from typing import List, Dict

# --- MOVIEPY WINERROR 6 FIX FOR PYTHON 3.13 ---
import subprocess
from logger_utils import configure_logging

try:
    from moviepy.audio.io.readers import FFMPEG_AudioReader

    original_del = FFMPEG_AudioReader.__del__

    def safe_del(self):
        try:
            original_del(self)
        except OSError:
            pass

    FFMPEG_AudioReader.__del__ = safe_del
except Exception:
    pass
# ----------------------------------------------

try:
    from ShortGen.audio.qwen3_voice_module import Qwen3VoiceModule
except ImportError:
    Qwen3VoiceModule = None
from ShortGen.engine.reddit_short_engine import RedditShortEngine
from ShortGen.config.languages import Language

# Configure logging
logger = configure_logging("LocalGen")

QWEN_TTS = "Qwen3-TTS (Local Model High Quality)"


def load_stories(file_path: str) -> List[Dict[str, str]]:
    """
    Load stories from a text file.
    Format is expected to be:
    TITLE: <title>
    SCRIPT: <script>
    ---
    """
    stories = []
    if not os.path.exists(file_path):
        logger.error(f"Stories file not found: {file_path}")
        return stories

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Split by delimiter
    raw_stories = content.split("---")

    for raw_story in raw_stories:
        if not raw_story.strip():
            continue

        lines = raw_story.strip().split("\n")
        title = ""
        script = ""

        current_section = None

        for line in lines:
            line = line.strip()
            if line.startswith("TITLE:"):
                title = line[6:].strip()
                current_section = "TITLE"
            elif line.startswith("SCRIPT:"):
                script = line[7:].strip()
                current_section = "SCRIPT"
            elif current_section == "SCRIPT":
                script += "\n" + line

        if title and script:
            stories.append({"title": title, "content": script})

    return stories


def get_files_from_folder(folder_path: str, extensions: tuple) -> List[str]:
    if not os.path.exists(folder_path):
        return []
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(extensions)
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Generate Reddit videos locally")
    parser.add_argument(
        "--video_folder",
        type=str,
        required=True,
        help="Folder containing background videos",
    )
    parser.add_argument(
        "--audio_folder",
        type=str,
        required=True,
        help="Folder containing background audio",
    )
    parser.add_argument(
        "--stories_file", type=str, required=True, help="Text file containing stories"
    )
    parser.add_argument(
        "--output_folder",
        type=str,
        default="output",
        help="Output folder for generated videos",
    )
    parser.add_argument("-N", type=int, default=1,
                        help="Number of videos to generate")
    parser.add_argument(
        "--tts_engine",
        type=str,
        choices=[QWEN_TTS],
        default=QWEN_TTS,
        help="TTS Engine to use",
    )

    args = parser.parse_args()

    # Setup output folder
    if not os.path.exists(args.output_folder):
        os.makedirs(args.output_folder)

    # Load stories
    stories = load_stories(args.stories_file)
    if not stories:
        logger.error("No stories found in the provided file.")
        return

    # Check assets
    video_files = get_files_from_folder(
        args.video_folder, (".mp4", ".mov", ".avi", ".mkv")
    )
    audio_files = get_files_from_folder(args.audio_folder, (".mp3", ".wav"))

    if not video_files:
        logger.error(f"No videos found in {args.video_folder}")
        return
    if not audio_files:
        logger.error(f"No audio files found in {args.audio_folder}")
        return

    # Initialize TTS
    if args.tts_engine == QWEN_TTS:
        if Qwen3VoiceModule is None:
            logger.error(
                "Qwen3-TTS module not found. Please install required packages (e.g. 'qwen-tts', 'torch', 'soundfile')."
            )
            return

        voice_module = Qwen3VoiceModule()

    # Generate videos
    for i in range(args.N):
        logger.info(f"=== Generating Video {i + 1}/{args.N} ===")

        # Select story (cycle if needed)
        story = stories[i % len(stories)]

        # Select random assets
        video_path = random.choice(video_files)
        audio_path = random.choice(audio_files)

        # We need to pass just the filename/relative path if the engine expects it,
        # or absolute path. The engine seems to prepend "assets/audios/" in some places.
        # Let's check RedditShortEngine._chooseBackgroundMusic:
        # self._db_background_music_url = "assets/audios/" + self._db_background_music_name
        # This implies the engine is coupled to the assets directory structure.
        # We should modify the engine to handle absolute paths or we need to be careful.

        # Actually, looking at the engine code I modified:
        # It takes background_video_name and background_music_name.
        # _chooseBackgroundVideo: self._db_background_video_url = self._db_background_video_name
        # _chooseBackgroundMusic: self._db_background_music_url = "assets/audios/" + self._db_background_music_name

        # We need to fix _chooseBackgroundMusic in RedditShortEngine to support absolute paths or
        # we have to hack it here.
        # Better to fix RedditShortEngine to check if path exists before prepending assets.

        # For now, to avoid modifying engine too much, I will pass the absolute path and I will
        # need to patch RedditShortEngine to not prepend "assets/audios/" if it's an absolute path.

        # Let's assume we will patch RedditShortEngine further.

        short_id = f"video_{i + 1}"

        engine = RedditShortEngine(
            voice_module,
            background_video_name=video_path,  # We will pass full path
            background_music_name=audio_path,  # We will pass full path
            reddit_link="IS_LOCAL",  # Dummy link
            short_id=short_id,
            language=Language.ENGLISH,
            story_title=story["title"],
            story_content=story["content"],
        )

        try:
            for step_num, step_info in engine.makeContent():
                logger.info(f"Step {step_num}: {step_info}")

            # Move result to output
            if os.path.exists(engine._db_video_path):
                target_dir = os.path.join(args.output_folder, short_id)
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                shutil.move(
                    engine._db_video_path, os.path.join(
                        target_dir, "video.mp4")
                )
                logger.info(
                    f"Video saved to {os.path.join(target_dir, 'video.mp4')}")
            else:
                logger.error("Video generation failed, output not found.")

        except Exception as e:
            logger.error(f"Error generating video {i + 1}: {e}")
            import traceback

            traceback.print_exc()
            try:
                import logger_utils

                logger_utils.log_error(e)
            except:
                pass


if __name__ == "__main__":
    main()
