import os

class Config:
    YT_API = os.environ.get("YT_API", "https://youtube-downloader.mn-bots.workers.dev")
    MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE", 2000))  # MB
    DOWNLOAD_DIR = "/tmp/mnbots"
