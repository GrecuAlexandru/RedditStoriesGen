import os
import random
import shutil
import asyncio
import logging
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks, HTTPException, status
from pydantic import BaseModel
from dotenv import load_dotenv
import httpx

# --- MOVIEPY WINERROR 6 FIX FOR PYTHON 3.13 ---
import subprocess

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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("FastAPI-Gen")

# Environment constraints
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "output")
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Global variables for the background worker
job_queue = asyncio.Queue()
voice_module = None

class GenerateRequest(BaseModel):
    title: str
    content: str
    callback_url: str
    short_id: str

def get_files_from_folder(folder_path: str, extensions: tuple) -> list[str]:
    if not os.path.exists(folder_path):
        return []
    return [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(extensions)
    ]

# The actual synchronous generation logic
def generate_video_sync(job: GenerateRequest) -> str:
    """
    Synchronous function that runs the generation pipeline.
    Returns the path to the completed video, or raises an Exception.
    """
    logger.info(f"Starting generation for short_id: {job.short_id}")

    # Check assets
    video_folder = os.path.abspath(os.path.join("assets", "videos"))
    audio_folder = os.path.abspath(os.path.join("assets", "audios"))

    video_files = get_files_from_folder(video_folder, (".mp4", ".mov", ".avi", ".mkv"))
    audio_files = get_files_from_folder(audio_folder, (".mp3", ".wav"))

    if not video_files:
        raise FileNotFoundError(f"No videos found in {video_folder}")
    if not audio_files:
        raise FileNotFoundError(f"No audio files found in {audio_folder}")

    # Select random assets (ensure absolute paths)
    video_path = os.path.abspath(random.choice(video_files))
    audio_path = os.path.abspath(random.choice(audio_files))

    # Initialize Engine
    engine = RedditShortEngine(
        voice_module,
        background_video_name=video_path,
        background_music_name=audio_path,
        reddit_link="IS_LOCAL",  # Dummy link
        short_id=job.short_id,
        language=Language.ENGLISH,
        story_title=job.title,
        story_content=job.content,
    )

    # Run the generation process
    for step_num, step_info in engine.makeContent():
        logger.info(f"[{job.short_id}] Step {step_num}: {step_info}")

    # Move result to output
    if hasattr(engine, "_db_video_path") and os.path.exists(engine._db_video_path):
        target_dir = os.path.abspath(os.path.join(OUTPUT_FOLDER, job.short_id))
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        final_video_path = os.path.join(target_dir, "video.mp4")
        shutil.move(engine._db_video_path, final_video_path)
        
        # Cleanup dynamic assets
        if hasattr(engine, 'dynamicAssetDir') and os.path.exists(engine.dynamicAssetDir):
            try:
                shutil.rmtree(engine.dynamicAssetDir)
            except Exception as e:
                logger.warning(f"Could not clean dynamic directory {engine.dynamicAssetDir}: {e}")
                
        logger.info(f"[{job.short_id}] Video saved to {final_video_path}")
        return final_video_path
    else:
        raise FileNotFoundError("Video generation finished, but output file not found.")

async def send_webhook(url: str, payload: dict):
    """Sends a POST request to the callback URL."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=60.0)
            response.raise_for_status()
            logger.info(f"Webhook sent successfully to {url}")
    except Exception as e:
        logger.error(f"Failed to send webhook to {url}: {e}")

async def process_queue():
    """Background worker that processes jobs ONE at a time."""
    logger.info("Background worker started. Waiting for jobs...")
    while True:
        job: GenerateRequest = await job_queue.get()
        try:
            logger.info(f"Processing job {job.short_id} from queue...")
            
            # Run the heavy synchronous workload in a separate thread
            # so it doesn't block the FastAPI event loop
            final_video_path = await asyncio.to_thread(generate_video_sync, job)
            
            # Send success webhook
            await send_webhook(job.callback_url, {
                "status": "success",
                "short_id": job.short_id,
                "video_path": final_video_path
            })
            
        except Exception as e:
            logger.error(f"Job {job.short_id} failed: {e}")
            import traceback
            traceback.print_exc()
            try:
                import logger_utils
                logger_utils.log_error(e)
            except:
                pass
            
            # Send failure webhook
            await send_webhook(job.callback_url, {
                "status": "failed",
                "short_id": job.short_id,
                "error": str(e)
            })
        finally:
            job_queue.task_done()
            logger.info(f"Finished processing job {job.short_id}. Waiting for next...")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize TTS and start the background worker
    global voice_module
    
    if Qwen3VoiceModule is None:
        logger.error("Qwen3-TTS module not found. Generation will fail.")
    else:
        logger.info("Initializing Qwen3-TTS Module...")
        # Initialize it once at startup
        voice_module = Qwen3VoiceModule()
        logger.info("Qwen3-TTS Module initialized.")

    # Start the worker task
    task = asyncio.create_task(process_queue())
    
    yield
    
    # Shutdown
    task.cancel()
    logger.info("Shutting down background worker.")

app = FastAPI(title="Reddit Video Gen API", lifespan=lifespan)

@app.post("/generate", status_code=status.HTTP_202_ACCEPTED)
async def generate_video(request: GenerateRequest):
    """
    Accepts a video generation request and adds it to the rendering queue.
    Ensures single concurrency.
    """
    await job_queue.put(request)
    logger.info(f"Job {request.short_id} added to the queue. Current queue size: {job_queue.qsize()}")
    return {
        "status": "accepted",
        "short_id": request.short_id,
        "message": "Job queued for processing",
        "queue_position": job_queue.qsize()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
