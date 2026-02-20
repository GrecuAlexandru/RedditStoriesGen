import argparse
import os
import sys
from pytube import YouTube
from moviepy import VideoFileClip


def download_video(url, output_path):
    """Download a YouTube video at the highest resolution"""
    print(f"Downloading video from {url}...")
    try:
        yt = YouTube(url)
        video = yt.streams.get_highest_resolution()

        # Creating output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Download the video
        video_path = video.download(output_path)
        print(f"Downloaded to {video_path}")
        return video_path

    except Exception as e:
        print(f"Error downloading video: {str(e)}")
        sys.exit(1)


def crop_to_9_16(video_path, output_path, center_x=None):
    """Crop a video to 9:16 aspect ratio"""
    print("Cropping video to 9:16 aspect ratio...")

    # Load the video
    clip = VideoFileClip(video_path)

    # Get current dimensions
    width, height = clip.w, clip.h
    current_ratio = width / height
    target_ratio = 9 / 16

    print(
        f"Original dimensions: {width}x{height} (ratio: {current_ratio:.4f})")
    print(f"Target ratio: {target_ratio:.4f} (9:16)")

    # Determine if we need to crop width or height to achieve 9:16
    if current_ratio > target_ratio:
        # Video is too wide, need to crop the width
        new_width = height * target_ratio

        # Determine crop position
        if center_x is None:
            # Default to center
            x1 = max(0, (width - new_width) / 2)
        else:
            # Use specified center point
            x1 = max(0, min(center_x - new_width / 2, width - new_width))

        print(f"Cropping width to {new_width:.1f} pixels (x1={x1:.1f})")
        cropped_clip = clip.crop(x1=x1, y1=0, width=new_width, height=height)

    else:
        # Video is too tall or already at right ratio, need to crop the height
        new_height = width / target_ratio
        y1 = max(0, (height - new_height) / 2)
        print(f"Cropping height to {new_height:.1f} pixels (y1={y1:.1f})")
        cropped_clip = clip.crop(x1=0, y1=y1, width=width, height=new_height)

    # Create output filename
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    output_filename = os.path.join(output_path, f"{base_name}_9x16.mp4")

    # Save the cropped video
    print(f"Saving cropped video (this may take a while)...")
    cropped_clip.write_videofile(
        output_filename,
        codec="h264_amf",
        audio_codec="aac",
        fps=30,
        preset="medium",
        bitrate="8000k")

    # Close clips
    clip.close()
    cropped_clip.close()

    print(f"Cropped video saved to {output_filename}")
    return output_filename


def main():
    parser = argparse.ArgumentParser(
        description="Download a YouTube video and crop it to 9:16 aspect ratio")
    parser.add_argument("url", help="YouTube video URL")
    parser.add_argument("-o", "--output", default="downloads",
                        help="Output directory (default: downloads)")
    parser.add_argument("-c", "--center", type=float, default=0.5,
                        help="Center position for horizontal cropping (0.0-1.0, where 0.5 is center)")
    args = parser.parse_args()

    try:
        # Validate center value
        if not 0 <= args.center <= 1:
            print("Center value must be between 0.0 and 1.0")
            sys.exit(1)

        # Download the video
        video_path = download_video(args.url, args.output)

        # Calculate center_x pixel position
        clip = VideoFileClip(video_path)
        center_x = clip.w * args.center
        clip.close()

        # Crop the video
        cropped_path = crop_to_9_16(video_path, args.output, center_x)

        print("Process completed successfully!")

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
