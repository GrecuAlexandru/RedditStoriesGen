import datetime
import datetime
import os
import re
import shutil
import random
import subprocess
import gc
import asyncio
from ShortGen.audio.voice_module import VoiceModule
from ShortGen.config.languages import Language
from ShortGen.editing_framework.editing_engine import EditingEngine, EditingStep, Flow

# from ShortGen.gpt import gpt_yt
from ShortGen.audio import audio_utils
from ShortGen.editing_utils import captions, editing_images
from ShortGen.editing_utils.handle_videos import extract_random_clip_from_video
from ShortGen.reddit_content import reddit_story_api

from moviepy import VideoFileClip, AudioFileClip
import cv2


class RedditShortEngine:
    def __init__(
        self,
        voiceModule: VoiceModule,
        background_video_name: str,
        background_music_name: str,
        reddit_link: str,
        short_id="",
        language: Language = Language.ENGLISH,
        story_title: str = None,
        story_content: str = None,
    ):
        self.voiceModule = voiceModule
        self.short_id = short_id
        self.language = language.value if isinstance(language, Language) else language

        # Database attributes with prefix _db_ to store state
        self._db_background_video_name = background_video_name
        self._db_background_music_name = background_music_name
        self._db_reddit_link = reddit_link
        self._db_language = self.language
        self._db_story_title = story_title
        self._db_story_content = story_content

        # Initialize other attributes
        self._db_script = ""
        self._db_temp_audio_path = ""
        self._db_audio_path = ""
        self._db_voiceover_duration = None
        self._db_background_video_url = None
        self._db_background_music_url = None
        self._db_background_video_duration = None
        self._db_background_trimmed = None
        self._db_timed_captions = []
        self._db_video_path = ""
        self._db_reddit_thread_image = ""
        self._db_reddit_question = ""
        self._db_yt_title = ""
        self._db_yt_description = ""
        self._db_ready_to_upload = False
        self._db_last_completed_step = 0
        self._db_reddit_author = ""

        # Create dynamic asset directory
        self.dynamicAssetDir = f"dynamic_assets/{short_id or 'temp'}/"
        if not os.path.exists(self.dynamicAssetDir):
            os.makedirs(self.dynamicAssetDir)

        # Define the processing steps
        self.stepDict = {
            1: self._generateScript,
            2: self._generateTempAudio,
            3: self._timeCaptions,
            4: self._chooseBackgroundMusic,
            5: self._chooseBackgroundVideo,
            6: self._prepareBackgroundAssets,
            7: self._prepareCustomAssets,
            8: self._editAndRenderShort,
            9: self._saveVideo,
        }

    def _generateScript(self):
        print("Generating reddit question & entertaining story")
        if self._db_story_title and self._db_story_content:
            self._db_reddit_question = self._db_story_title
            self._db_reddit_author = (
                "Anonymous"  # Placeholder since we don't have author
            )
            story_content = self._db_story_content
        else:
            self._db_reddit_question, self._db_reddit_author, story_content = (
                reddit_story_api.get_reddit_post_content(self._db_reddit_link)
            )

        # Remove emojis from title and content
        def remove_emojis(text):
            """Remove emoji characters from text"""
            if not text:
                return text
            # Unicode ranges for emoji characters
            emoji_pattern = re.compile(
                "["
                "\U0001f600-\U0001f64f"  # emoticons
                "\U0001f300-\U0001f5ff"  # symbols & pictographs
                "\U0001f680-\U0001f6ff"  # transport & map symbols
                "\U0001f1e0-\U0001f1ff"  # flags (iOS)
                "\U00002500-\U00002bef"  # chinese char
                "\U00002702-\U000027b0"
                "\U00002702-\U000027b0"
                "\U000024c2-\U0001f251"
                "\U0001f926-\U0001f937"
                "\U00010000-\U0010ffff"
                "\u2640-\u2642"
                "\u2600-\u2b55"
                "\u200d"
                "\u23cf"
                "\u23e9"
                "\u231a"
                "\ufe0f"  # dingbats
                "\u3030"
                "]+",
                flags=re.UNICODE,
            )
            return emoji_pattern.sub(r"", text).strip()

        # Clean the title and content
        self._db_reddit_question = remove_emojis(self._db_reddit_question)
        story_content = remove_emojis(story_content)

        print(f"Reddit question: {self._db_reddit_question}")
        print(f"Reddit author: {self._db_reddit_author}")
        print(f"Story content length: {len(story_content)} characters")
        print("Story content:")

        # Safe printing to handle any remaining Unicode issues
        try:
            print(story_content)
        except UnicodeEncodeError:
            safe_content = story_content.encode("ascii", errors="ignore").decode(
                "ascii"
            )
            print(f"[Content with Unicode characters filtered]: {safe_content}")

        # Combine question and story content
        raw_script = f"{self._db_reddit_question}\n\n{story_content}"

        # Process the script to improve readability and speech patterns
        if not self._db_story_content:
            from ShortGen.gpt.gpt_utils import process_script_for_voice

            print("Processing script to improve readability and speech patterns...")
            try:
                processed_script = process_script_for_voice(raw_script)
                self._db_script = processed_script
                print("Script processed successfully")
            except Exception as e:
                print(f"Error processing script: {e}. Using raw script instead.")
                self._db_script = raw_script
        else:
            print("Using manual script, skipping GPT processing.")
            self._db_script = raw_script

    def _generateTempAudio(self):
        if not self._db_script:
            raise ValueError("generateScript method must set self._db_script.")
        if self._db_temp_audio_path:
            return
        self.verifyParameters(text=self._db_script)
        script = self._db_script
        self._db_temp_audio_path = self.voiceModule.generate_voice(
            script, self.dynamicAssetDir + "temp_audio_path.wav"
        )
        self._db_audio_path = self._db_temp_audio_path

    def _timeCaptions(self):
        """
        Generate timed captions from audio file using Whisper
        with improved error handling and resource management
        """
        print("Generating timed captions...")
        self.verifyParameters(audioPath=self._db_audio_path)

        import gc
        import asyncio

        try:
            # First verify the audio file exists
            if not os.path.exists(self._db_audio_path):
                raise FileNotFoundError(f"Audio file not found: {self._db_audio_path}")

            # Reset asyncio event loop if it's closed
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    print("Event loop was closed, creating new one...")
                    asyncio.set_event_loop(asyncio.new_event_loop())
            except Exception as e:
                print(f"Asyncio setup error: {str(e)}, creating new event loop")
                asyncio.set_event_loop(asyncio.new_event_loop())

            # Process the audio using Whisper
            print("Processing audio with Whisper...")
            whisper_analysis = audio_utils.audioToText(self._db_audio_path)

            # Check if analysis was successful
            if not whisper_analysis or not isinstance(whisper_analysis, dict):
                print("Warning: Whisper analysis returned invalid data")
                raise ValueError("Invalid Whisper analysis result")

            # Process the whisper output to get word-by-word captions
            self._db_timed_captions = captions.getWordByWordCaptionsWithTime(
                whisper_analysis
            )

            # Verify captions were generated properly
            if not self._db_timed_captions or len(self._db_timed_captions) < 1:
                print("Warning: No captions were generated")
                # Create a placeholder caption if needed
                audio_duration = self.getAudioDuration(self._db_audio_path)[1]
                self._db_timed_captions = [
                    ((0, audio_duration), "NO CAPTIONS AVAILABLE")
                ]

            print(
                f"Successfully generated {len(self._db_timed_captions)} caption segments"
            )

        except Exception as e:
            print(f"Error in caption generation: {str(e)}")
            print("Creating fallback captions...")

            # Create fallback captions so the process can continue
            try:
                # Get audio duration for fallback timing
                _, audio_duration = self.getAudioDuration(self._db_audio_path)

                # Create basic caption spanning the audio duration
                self._db_timed_captions = [
                    ((0, audio_duration), "CAPTIONS UNAVAILABLE")
                ]
                print(f"Created fallback caption with duration {audio_duration}s")
            except Exception as e2:
                print(f"Error creating fallback captions: {str(e2)}")
                # Last resort - create a generic 30-second caption
                self._db_timed_captions = [((0, 30), "CAPTIONS UNAVAILABLE")]

        finally:
            # Force garbage collection to clean up resources
            gc.collect()

            # Try to explicitly close any event loops
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                    loop.close()
            except Exception:
                pass  # Ignore errors during cleanup

    def _chooseBackgroundMusic(self):
        if os.path.exists(self._db_background_music_name):
            self._db_background_music_url = self._db_background_music_name
        else:
            self._db_background_music_url = (
                "assets/audios/" + self._db_background_music_name
            )

    def _chooseBackgroundVideo(self):
        print(f"getting video: {self._db_background_video_name}")
        self._db_background_video_url = self._db_background_video_name
        self._db_background_video_duration = self.getVideoDuration(
            self._db_background_video_url
        )

    def _prepareBackgroundAssets(self):
        self.verifyParameters(
            voiceover_audio_url=self._db_audio_path,
            video_duration=self._db_background_video_duration,
            background_video_url=self._db_background_video_url,
            music_url=self._db_background_music_url,
        )
        # Step 1: Get the voice-over duration first
        if not self._db_voiceover_duration:
            print("Rendering short: (1/4) preparing voice asset...")
            print(self._db_audio_path)
            self._db_audio_path, self._db_voiceover_duration = self.getAudioDuration(
                self._db_audio_path
            )
            print(f"Voice-over duration: {self._db_voiceover_duration:.2f} seconds")

        # Step 2: Check if background video has enough duration
        print(f"Total video duration: {self._db_background_video_duration:.2f} seconds")
        if self._db_background_video_duration < self._db_voiceover_duration * 1.2:
            raise Exception(
                f"Background video is too short! Need at least {self._db_voiceover_duration * 1.2:.2f} seconds, but video is {self._db_background_video_duration:.2f} seconds"
            )

        # Step 3: Check if background music has enough duration
        music_path, music_duration = self.getAudioDuration(
            self._db_background_music_url
        )
        print(f"Background music duration: {music_duration:.2f} seconds")
        if music_duration < self._db_voiceover_duration * 1.2:
            raise Exception(
                f"Background music is too short! Need at least {self._db_voiceover_duration * 1.2:.2f} seconds, but music is {music_duration:.2f} seconds"
            )

        # Step 4: Prepare the video clip with proper random starting point
        if not self._db_background_trimmed:
            print("Rendering short: (2/4) preparing background video asset...")
            output_path = self.dynamicAssetDir + "clipped_background.mp4"
            if os.path.exists(output_path):
                os.remove(output_path)
                print(f"Deleted existing background clip: {output_path}")

            # Store the video starting point for later reference (will be used for music synchronization)
            max_start_time = (
                self._db_background_video_duration - self._db_voiceover_duration * 1.1
            )
            min_start_time = (
                self._db_background_video_duration * 0.15
            )  # Skip first 15% to avoid intros
            if max_start_time <= min_start_time:
                start_time = min_start_time
            else:
                start_time = min_start_time + random.random() * (
                    max_start_time - min_start_time
                )

            self._db_video_start_time = start_time
            print(
                f"Selected video starting point: {start_time:.2f}s ({(start_time / self._db_background_video_duration) * 100:.1f}% of video)"
            )

            command = [
                "ffmpeg",
                "-loglevel",
                "error",
                "-ss",
                str(start_time),
                # Add 1 second buffer
                "-t",
                str(self._db_voiceover_duration + 1),
                "-i",
                self._db_background_video_url,
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                output_path,
            ]

            subprocess.run(command, check=True)

            if not os.path.exists(output_path):
                raise Exception("Video clip failed to be written")

            self._db_background_trimmed = output_path

            cropped_output_path = self.dynamicAssetDir + "clipped_background_9_16.mp4"
            self._db_background_trimmed = self.crop_to_9_16(
                output_path, cropped_output_path
            )

            # Step 5: Save the music starting point for use in EditingEngine
            max_music_start = music_duration - self._db_voiceover_duration * 1.1
            min_music_start = music_duration * 0.15  # Skip first 15% to avoid intros
            if max_music_start <= min_music_start:
                music_start_time = min_music_start
            else:
                music_start_time = min_music_start + random.random() * (
                    max_music_start - min_music_start
                )

            self._db_music_start_time = music_start_time
            print(
                f"Selected music starting point: {music_start_time:.2f}s ({(music_start_time / music_duration) * 100:.1f}% of track)"
            )

            # Store the music subclip for later use
            music_output_path = self.dynamicAssetDir + "music_clip.mp3"
            if os.path.exists(music_output_path):
                os.remove(music_output_path)

            music_command = [
                "ffmpeg",
                "-loglevel",
                "error",
                "-ss",
                str(music_start_time),
                "-t",
                str(self._db_voiceover_duration),
                "-i",
                self._db_background_music_url,
                "-c:a",
                "libmp3lame",
                "-q:a",
                "4",
                music_output_path,
            ]

            subprocess.run(music_command, check=True)

            if not os.path.exists(music_output_path):
                raise Exception("Music clip failed to be written")

            self._db_background_music_clip = music_output_path

    def _prepareCustomAssets(self):
        """
        Generate custom reddit image asset
        """
        print("Rendering short: (3/4) preparing custom reddit image...")
        self.verifyParameters(
            question=self._db_reddit_question,
        )
        title, header = self._db_reddit_question, self._db_reddit_author

        wrapped_title = self._wrap_text(title, max_chars_per_line=90)

        imageEditingEngine = EditingEngine()
        imageEditingEngine.ingestFlow(
            Flow.WHITE_REDDIT_IMAGE_FLOW,
            {
                "username_text": header,
                "question_text": wrapped_title,
            },
        )
        imageEditingEngine.renderImage(self.dynamicAssetDir + "redditThreadImage.png")
        self._db_reddit_thread_image = self.dynamicAssetDir + "redditThreadImage.png"

    # Reduced from 60 to avoid edge cropping
    def _wrap_text(self, text, max_chars_per_line=90):
        """Helper function to wrap text at specific character count with proper word breaks"""
        words = text.split()
        lines = []
        current_line = []
        current_length = 0

        for word in words:
            # If this is the first word on the line, just add it regardless of length
            if not current_line:
                current_line.append(word)
                current_length = len(word)
                continue

            # Calculate length if we add this word (including a space)
            word_length = len(word)
            new_length = current_length + 1 + word_length  # +1 for the space

            # If adding this word would exceed the line length, start a new line
            if new_length > max_chars_per_line:
                # Complete the current line
                lines.append(" ".join(current_line))
                # Start a new line with the current word
                current_line = [word]
                current_length = word_length
            else:
                # Add the word to the current line
                current_line.append(word)
                current_length = new_length

        # Add the last line if it has content
        if current_line:
            lines.append(" ".join(current_line))

        return "\n".join(lines)

    def crop_to_9_16(self, video_path, output_path, center_x=None):
        """
        Crop video to 9:16 aspect ratio using FFmpeg for better reliability
        """
        try:
            # First get video properties using FFmpeg
            probe_cmd = [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_streams",
                video_path,
            ]

            import json

            result = subprocess.run(
                probe_cmd, capture_output=True, text=True, check=True
            )
            probe_data = json.loads(result.stdout)

            # Find video stream
            video_stream = None
            for stream in probe_data["streams"]:
                if stream["codec_type"] == "video":
                    video_stream = stream
                    break

            if not video_stream:
                raise ValueError("No video stream found")

            width = int(video_stream["width"])
            height = int(video_stream["height"])

            # Calculate target dimensions for 9:16 aspect ratio
            print(f"Original video dimensions: {width}x{height}")
            target_width = int(height * 9 / 16)

            if target_width > width:
                # If video is too narrow, we need to crop height instead
                target_height = int(width * 16 / 9)
                target_width = width
                crop_filter = f"crop={target_width}:{target_height}:0:{(height - target_height) // 2}"
            else:
                # Crop width to get 9:16 ratio
                target_height = height
                crop_x = (
                    (width - target_width) // 2
                    if center_x is None
                    else max(0, min(center_x - target_width // 2, width - target_width))
                )
                crop_filter = f"crop={target_width}:{target_height}:{crop_x}:0"

            print(f"Target dimensions after crop: {target_width}x{target_height}")

            # Always scale to standard 1080x1920 for consistency
            final_width = 1080
            final_height = 1920

            # Create filter chain: crop first, then scale to standard size
            filter_chain = f"{crop_filter},scale={final_width}:{final_height}"

            print(f"Final standardized dimensions: {final_width}x{final_height}")

            # Use FFmpeg to crop and scale the video
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-i",
                video_path,
                "-vf",
                filter_chain,
                "-c:v",
                "libx264",
                "-preset",
                "fast",
                "-crf",
                "23",
                "-c:a",
                "copy",  # Copy audio without re-encoding
                output_path,
            ]

            print(f"Running FFmpeg crop command...")
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True)

            # Verify the output
            if not os.path.exists(output_path):
                raise FileNotFoundError(f"Cropped video was not created: {output_path}")

            # Verify dimensions of output
            verify_cmd = [
                "ffprobe",
                "-v",
                "quiet",
                "-print_format",
                "json",
                "-show_streams",
                output_path,
            ]
            verify_result = subprocess.run(
                verify_cmd, capture_output=True, text=True, check=True
            )
            verify_data = json.loads(verify_result.stdout)

            for stream in verify_data["streams"]:
                if stream["codec_type"] == "video":
                    final_width = int(stream["width"])
                    final_height = int(stream["height"])
                    print(f"Final video dimensions: {final_width}x{final_height}")

                    # Check if dimensions match our standard 1080x1920
                    if final_width != 1080 or final_height != 1920:
                        print(
                            f"Warning: Final dimensions {final_width}x{final_height} do not match expected 1080x1920"
                        )
                    else:
                        print(
                            "✓ Video dimensions are correctly standardized to 1080x1920"
                        )
                    break

            print(f"Successfully cropped video to 9:16 aspect ratio: {output_path}")
            return output_path

        except subprocess.CalledProcessError as e:
            print(f"FFmpeg error: {e}")
            print(f"FFmpeg stderr: {e.stderr}")
            raise
        except Exception as e:
            print(f"Error cropping video: {str(e)}")
            # Fallback to original OpenCV method if FFmpeg fails
            return self._crop_to_9_16_opencv_fallback(video_path, output_path, center_x)

    def _crop_to_9_16_opencv_fallback(self, video_path, output_path, center_x=None):
        """
        Fallback OpenCV cropping method
        """
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            raise IOError(f"Cannot open video file: {video_path}")

        # Retrieve video properties
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = cap.get(cv2.CAP_PROP_FPS)

        print(f"Fallback: Original dimensions {width}x{height}")

        # Calculate target width for 9:16 aspect ratio
        target_width = int(height * 9 / 16)
        if target_width > width:
            cap.release()
            raise ValueError(
                "Original video is too narrow to crop to 9:16 aspect ratio."
            )

        # Determine horizontal center for cropping
        if center_x is None:
            center_x = width // 2
        x1 = max(0, center_x - target_width // 2)
        x2 = min(width, x1 + target_width)

        # Adjust x1 and x2 if they exceed boundaries
        if x2 - x1 < target_width:
            x1 = max(0, width - target_width)
            x2 = x1 + target_width

        print(
            f"Fallback: Target dimensions {target_width}x{height}, crop from x={x1} to x={x2}"
        )

        # Always output to standard 1080x1920 dimensions
        final_width = 1080
        final_height = 1920
        print(
            f"Fallback: Will scale to standard dimensions {final_width}x{final_height}"
        )

        # Try different codecs in order of preference
        codecs_to_try = ["mp4v", "XVID", "avc1", "H264"]

        for codec in codecs_to_try:
            try:
                # Try current codec - output at standard resolution
                fourcc = cv2.VideoWriter_fourcc(*codec)
                out = cv2.VideoWriter(
                    output_path, fourcc, fps, (final_width, final_height)
                )

                # Check if VideoWriter was initialized properly
                if not out.isOpened():
                    print(
                        f"Failed to initialize VideoWriter with codec {codec}, trying next..."
                    )
                    continue

                # Reset video capture to beginning
                # Process the video
                cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                frame_count = 0
                while True:
                    ret, frame = cap.read()
                    if not ret:
                        break
                    # Crop the frame
                    cropped_frame = frame[0:height, x1:x2]
                    # Resize to standard dimensions
                    resized_frame = cv2.resize(
                        cropped_frame, (final_width, final_height)
                    )
                    out.write(resized_frame)
                    frame_count += 1

                out.release()
                print(
                    f"Fallback: Successfully cropped {frame_count} frames using {codec} codec"
                )
                break  # Exit the loop if successful

            except Exception as e:
                print(f"Fallback: Error with codec {codec}: {str(e)}")
                if codec == codecs_to_try[-1]:
                    cap.release()
                    raise  # Re-raise the exception if we've tried all codecs

        cap.release()
        print(f"Fallback: Cropped video saved to: {output_path}")
        return output_path

    def _editAndRenderShort(self):
        """
        Customize video rendering sequence by adding a Reddit image
        """
        self.verifyParameters(
            voiceover_audio_url=self._db_audio_path,
            video_duration=self._db_background_video_duration,
            music_url=self._db_background_music_url,
        )

        # Verify audio files exist and are valid before proceeding
        try:
            print(f"Verifying voiceover audio: {self._db_audio_path}")
            if not os.path.exists(self._db_audio_path):
                raise ValueError(
                    f"Voiceover audio file not found: {self._db_audio_path}"
                )

            voice_audio = AudioFileClip(self._db_audio_path)
            voice_duration = voice_audio.duration
            voice_audio.close()
            print(f"Voiceover audio verified: {voice_duration} seconds")

            # Check for our pre-cut music clip
            music_to_use = (
                self._db_background_music_clip
                if hasattr(self, "_db_background_music_clip")
                else self._db_background_music_url
            )
            print(f"Using music: {music_to_use}")
            if not os.path.exists(music_to_use):
                raise ValueError(f"Background music file not found: {music_to_use}")

            music_audio = AudioFileClip(music_to_use)
            music_duration = music_audio.duration
            music_audio.close()
            print(f"Background music verified: {music_duration} seconds")
        except Exception as e:
            print(f"Audio verification failed: {str(e)}")
            print("Attempting to continue with video only...")

        outputPath = self.dynamicAssetDir + "rendered_video.mp4"
        if not (os.path.exists(outputPath)):
            print("Rendering short: Starting automated editing...")
            videoEditor = EditingEngine()

            # Only add audio steps if files are valid
            try:
                voice_audio = AudioFileClip(self._db_audio_path)
                voice_audio.close()
                videoEditor.addEditingStep(
                    EditingStep.ADD_VOICEOVER_AUDIO, {"url": self._db_audio_path}
                )
            except Exception as e:
                print(f"Skipping voiceover audio due to error: {str(e)}")

            try:
                # Use our pre-cut music clip that already has the correct random starting point
                music_to_use = (
                    self._db_background_music_clip
                    if (
                        hasattr(self, "_db_background_music_clip")
                        and self._db_background_music_clip
                    )
                    else self._db_background_music_url
                )
                music_audio = AudioFileClip(music_to_use)
                music_audio.close()

                music_params = {
                    "url": music_to_use,
                    "volume_percentage": 0.11,
                    # Always include this parameter
                    "loop_background_music": self._db_voiceover_duration,
                }  # For pre-cut clips, also add timing parameters
                if music_to_use != self._db_background_music_url:
                    music_params["set_time_start"] = 0
                    music_params["set_time_end"] = self._db_voiceover_duration

                videoEditor.addEditingStep(
                    EditingStep.ADD_BACKGROUND_MUSIC, music_params
                )
            except Exception as e:
                print(f"Skipping background music due to error: {str(e)}")

            videoEditor.addEditingStep(
                EditingStep.ADD_BACKGROUND_VIDEO,
                {
                    "url": self._db_background_trimmed,
                    "set_time_start": 0,  # Start at the beginning
                    "set_time_end": self._db_voiceover_duration,  # End when the voiceover ends
                    "loop_background_video": True,  # Ensure video loops if needed
                },
            )

            # Check if subscribe animation exists and is valid
            subscribe_animation_path = "assets/extra/subscribe_animation.mp4"
            try:
                if os.path.exists(subscribe_animation_path):
                    # Try to verify the file
                    test_clip = VideoFileClip(subscribe_animation_path)
                    if test_clip.duration > 0:
                        test_clip.close()
                        videoEditor.addEditingStep(
                            EditingStep.ADD_SUBSCRIBE_ANIMATION,
                            {"url": subscribe_animation_path},
                        )
            except Exception as e:
                print(f"Skipping subscribe animation due to error: {str(e)}")

            videoEditor.addEditingStep(
                EditingStep.ADD_REDDIT_IMAGE,
                {
                    "url": self._db_reddit_thread_image,
                    "set_time_start": 0,  # Start showing at beginning
                    "set_time_end": self._db_voiceover_duration,  # Show for entire duration
                },
            )

            print(f"DEBUG: Voiceover duration: {self._db_voiceover_duration}")
            print(
                f"DEBUG: Video parameters: start=0, end={self._db_voiceover_duration}"
            )
            caption_type = (
                EditingStep.ADD_CAPTION_SHORT_ARABIC
                if self._db_language == Language.ARABIC.value
                else EditingStep.ADD_CAPTION_SHORT
            )

            for timing, text in self._db_timed_captions:
                videoEditor.addEditingStep(
                    caption_type,
                    {
                        "text": text.upper(),
                        "set_time_start": timing[0],
                        "set_time_end": timing[1],
                        "animate_words": True,
                    },
                )

            # Render the video with error handling
            try:
                videoEditor.renderVideo(outputPath)
            except ValueError as e:
                if "operands could not be broadcast together with shapes" in str(e):
                    print("Handling MoviePy compositing error...")
                    # Create a simpler version without some overlays
                    simpleEditor = EditingEngine()

                    # Add essential components only
                    simpleEditor.addEditingStep(
                        EditingStep.ADD_VOICEOVER_AUDIO, {"url": self._db_audio_path}
                    )

                    simpleEditor.addEditingStep(
                        EditingStep.ADD_BACKGROUND_VIDEO,
                        {
                            "url": self._db_background_trimmed,
                            "set_time_start": 0,
                            "set_time_end": self._db_voiceover_duration,
                        },
                    )

                    # Add captions but not other visual elements
                    caption_type = (
                        EditingStep.ADD_CAPTION_SHORT_ARABIC
                        if self._db_language == Language.ARABIC.value
                        else EditingStep.ADD_CAPTION_SHORT
                    )
                    for timing, text in self._db_timed_captions:
                        simpleEditor.addEditingStep(
                            caption_type,
                            {
                                "text": text.upper(),
                                "set_time_start": timing[0],
                                "set_time_end": timing[1],
                                "animate_words": False,  # Disable animation for stability
                            },
                        )

                    # Try rendering with simplified composition
                    print("Attempting simplified rendering...")
                    simpleEditor.renderVideo(outputPath)
                else:
                    raise  # Re-raise if it's a different ValueError

        # Always set the video path regardless of whether we rendered or file existed
        self._db_video_path = outputPath

    def _saveVideo(self):
        # Create timestamped filename and save the video
        now = datetime.datetime.now()
        date_str = now.strftime("%Y-%m-%d_%H-%M-%S")

        # Create the output directory if it doesn't exist
        output_dir = (
            f"channel{self.short_id}_output"
            if self.short_id.startswith("channel")
            else "videos"
        )
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Create the new filename with timestamp
        new_filename = f"{date_str}.mp4"
        new_video_path = os.path.join(output_dir, new_filename)

        # Move the rendered video to the final location
        if os.path.exists(self._db_video_path):
            shutil.move(self._db_video_path, new_video_path)
            self._db_video_path = new_video_path
            print(f"Video saved with timestamp: {new_video_path}")
        else:
            print(f"Warning: Rendered video not found at {self._db_video_path}")

        self._db_ready_to_upload = True

    def verifyParameters(self, **kwargs):
        """Utility method to verify that required parameters are not None"""
        for key, value in kwargs.items():
            if value is None:
                raise ValueError(f"Parameter {key} is required but was not provided")

    def get_total_steps(self):
        return len(self.stepDict)

    def isShortDone(self):
        return self._db_ready_to_upload

    def makeContent(self):
        while not self.isShortDone():
            currentStep = self._db_last_completed_step + 1
            if currentStep not in self.stepDict:
                raise Exception(f"Incorrect step {currentStep}")
            if self.stepDict[currentStep].__name__ == "_editAndRenderShort":
                yield (
                    currentStep,
                    f"Current step ({currentStep} / {self.get_total_steps()}) : "
                    + "Preparing rendering assets...",
                )
            else:
                yield (
                    currentStep,
                    f"Current step ({currentStep} / {self.get_total_steps()}) : "
                    + self.stepDict[currentStep].__name__,
                )
            print(f"Step {currentStep} {self.stepDict[currentStep].__name__}")
            self.stepDict[currentStep]()
            self._db_last_completed_step = currentStep

    def getVideoDuration(self, video_path):
        clip = VideoFileClip(video_path)
        duration = clip.duration
        clip.close()
        return duration

    def getAudioDuration(self, audio_path):
        audio = AudioFileClip(audio_path)
        duration = audio.duration
        audio.close()
        return audio_path, duration
