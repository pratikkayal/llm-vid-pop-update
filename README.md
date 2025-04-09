# llm-vid-pop

A comprehensive tool for downloading and managing YouTube video data for machine learning applications, particularly focused on video popularity prediction.

Overview
This repository provides tools to download YouTube videos, thumbnails, and captions at scale. It's designed to support research on video popularity prediction by efficiently collecting and organizing a large corpus of video data.

Main Features
Parallel downloading of videos, thumbnails, and captions
Progress tracking to resume interrupted downloads
Error handling for unavailable or restricted content
Support for alternate data sources with automatic fallback

# Repository Structure

llm-vid-pop/
├── youtube_dataset_collector.py       # Main script for downloading YouTube content
├── quantiles/                 # Contains video IDs organized by popularity quantiles
├── videos/                    # Downloaded videos in mp4 format
├── thumbnails/                # Video thumbnails in jpg format
├── captions/                  # Video captions in json format
├── video_description/         # Video descriptions in text format
├── text_emb/                  # Text embeddings generated from video metadata
│   ├── mpnet_caption_features.pkl
│   ├── mpnet_coarse_llama370B_introduction_features.pkl
│   ├── mpnet_description_features.pkl
│   ├── mpnet_title_features.pkl
│   └── mpnet_videotext_features.pkl
├── videoemb/                  # Video embeddings extracted using CLIP
│   ├── 10frames_clip/         # Embeddings using every 10th frame
│   └── 20frames_clip/         # Embeddings using every 20th frame
└── thumbnail_emb/             # Thumbnail embeddings created using ResNet-56

# Usage
python youtube_dataset_collector.py -q QUANTILE -n NUM_VIDEOS

Where:

QUANTILE: The quantile ID to download videos from
NUM_VIDEOS: The number of videos to download from the specified quantile

# Example:
python youtube_dataset_collector.py -q 33 -n 1000

# How It Works
The script loads video IDs from the specified quantile file
It checks which videos have already been downloaded using the progress log
Downloads are processed in parallel with configurable thread count
Videos, thumbnails, and captions are downloaded to their respective directories
Progress is continuously tracked in CSV format for resumable downloads

# Requirements
Python 3.7+
pytube
youtube_transcript_api
pandas
numpy
tqdm