import json
import os
import random
import datetime
import os
import json
import datetime
import random
from typing import Dict, List, Tuple, Optional


class SelectorEngine:
    def __init__(self, database_path="databse.json", links_path="links.txt"):
        self.database_path = database_path
        self.links_path = links_path
        self.database = self._load_database()

    def _load_database(self) -> Dict:
        """Load the database from the JSON file or create it if it doesn't exist"""
        if os.path.exists(self.database_path):
            try:
                with open(self.database_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                print(f"Error reading database file. Creating new database.")

        # Create a new database structure if file doesn't exist or is invalid
        db = {
            "videos": [],  # Keep for backwards compatibility
            "audios": [],
            "reddit_links": []
        }

        # Add channel-specific video sections
        for i in range(1, 5):
            db[f"videos_channel{i}"] = []

        return db

    def _save_database(self):
        """Save the database back to the JSON file"""
        with open(self.database_path, 'w') as f:
            json.dump(self.database, f, indent=2)

    def scan_assets(self):
        """Scan asset directories for new files and add them to the database"""
        # Scan channel-specific video directories
        for i in range(1, 5):
            video_dir = f"assets/videos{i}"
            self._scan_asset_directory(video_dir, f"videos_channel{i}")

        # Scan audios directory
        self._scan_asset_directory("assets/audios", "audios")

        # Save changes to database
        self._save_database()

    def _scan_asset_directory(self, directory: str, asset_type: str):
        """Scan a directory and add new files to the database"""
        if not os.path.exists(directory):
            print(f"Warning: Directory {directory} doesn't exist.")
            return

        # Create the database section if it doesn't exist
        if asset_type not in self.database:
            self.database[asset_type] = []

        # Get existing filenames in the database
        existing_files = {item["filename"]
                          for item in self.database[asset_type]}

        # Scan directory for new files
        for filename in os.listdir(directory):
            if filename in existing_files:
                continue

            # Skip directories
            if os.path.isdir(os.path.join(directory, filename)):
                continue

            # Add new file to database
            self.database[asset_type].append({
                "filename": filename,
                "path": os.path.join(directory, filename).replace("\\", "/"),
                "last_used": None
            })
            print(f"Added new {asset_type} asset: {filename}")

    def check_new_links(self):
        """Check for new links in links.txt and add them to the database"""
        if not os.path.exists(self.links_path):
            print(f"Warning: Links file {self.links_path} doesn't exist.")
            return

        # Get existing links
        existing_links = {link["url"]
                          for link in self.database["reddit_links"]}

        # Read links file
        new_links_added = False
        with open(self.links_path, 'r') as f:
            for line in f:
                url = line.strip()
                if url and url not in existing_links and url.startswith("http"):
                    self.database["reddit_links"].append({
                        "url": url,
                        "last_used": None
                    })
                    existing_links.add(url)
                    new_links_added = True
                    print(f"Added new Reddit link: {url}")

        # Save changes if any new links were added
        if new_links_added:
            self._save_database()

            # Clear the links.txt file after adding the links
            with open(self.links_path, 'w') as f:
                f.write("")

    def select_assets(self) -> Tuple[str, str, Optional[str]]:
        """
        Select a random combination of video, audio, and Reddit link
        that has been used less recently or not at all
        """
        # Include full timestamp with hours, minutes, and seconds
        today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Select audio
        audio = self._select_least_recently_used("audios")
        if audio:
            audio["last_used"] = today

        # Select Reddit link
        reddit_link = self._select_least_recently_used("reddit_links")
        link_url = None
        if reddit_link:
            reddit_link["last_used"] = today
            link_url = reddit_link["url"]

        # Save updated usage data
        self._save_database()

        # Return the selected assets with None for video (will be selected per channel)
        audio_filename = audio["filename"] if audio else None

        return None, audio_filename, link_url

    def _select_least_recently_used(self, asset_type: str) -> Optional[Dict]:
        """Select an asset that was least recently used or not used at all"""
        if not self.database[asset_type]:
            return None

        # First, try to find assets that have never been used
        never_used = [item for item in self.database[asset_type]
                      if item["last_used"] is None]
        if never_used:
            return random.choice(never_used)

        # If all assets have been used, properly compare timestamps to find the oldest
        def get_date_value(item):
            """Convert date string to datetime object for comparison"""
            if not item["last_used"]:
                return datetime.datetime.min
            try:
                # Try parsing with hours, minutes, seconds
                return datetime.datetime.strptime(item["last_used"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    # Fall back to date-only format for backward compatibility
                    return datetime.datetime.strptime(item["last_used"], "%Y-%m-%d")
                except ValueError:
                    return datetime.datetime.min

        # Return the item with the oldest timestamp
        return min(self.database[asset_type], key=get_date_value)

    def select_video_from_folder(self, folder_name):
        """
        Select a video from a specific folder
        """
        # Map folder name to channel number
        channel_num = None
        if folder_name.startswith("videos"):
            try:
                channel_num = int(folder_name[6:])
            except ValueError:
                pass

        if channel_num and 1 <= channel_num <= 4:
            asset_type = f"videos_channel{channel_num}"
        else:
            asset_type = "videos"  # Fallback to original videos

        # Filter videos by folder
        folder_path = os.path.join("assets", folder_name)
        if not os.path.exists(folder_path):
            print(f"Warning: Folder {folder_path} does not exist.")
            return None

        # Get all video files from the folder
        video_files = []
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):
                # Check if this file is in our database
                found = False
                for video in self.database[asset_type]:
                    if video['filename'] == filename:
                        video_files.append(video)
                        found = True
                        break

                # If not in database, add it
                if not found:
                    new_video = {
                        'filename': filename,
                        'path': os.path.join(folder_path, filename).replace("\\", "/"),
                        'last_used': None
                    }
                    self.database[asset_type].append(new_video)
                    video_files.append(new_video)

        if not video_files:
            return None

        # Sort by last_used (None or oldest first)
        video_files.sort(key=lambda x: x['last_used'] or '0000-00-00')

        # Mark the video as used
        video_files[0]["last_used"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
        self._save_database()

        return video_files[0]['path']

    def select_audio(self):
        """
        Select an audio file using the existing logic
        """
        audio = self._select_least_recently_used("audios")
        if audio:
            audio["last_used"] = datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S")
            self._save_database()
            return audio["filename"]
        return None
