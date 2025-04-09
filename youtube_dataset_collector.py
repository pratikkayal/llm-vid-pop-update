import os, shutil
import time
import queue
import argparse
import threading
import numpy as np
import pandas as pd
import tqdm
import urllib.request
import time
# from concurrent.futures import ThreadPoolExecutor
# from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import threading
from pytube import YouTube
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter
formatter = JSONFormatter()

# Define constants
ROOT_DIR = '/storage-1/pratik/new_data/'
LOAD_DIR = 'quantiles/'
PROGRESS_DIR = 'progress/'

VIDEO_RESOLUTION = '144p'#'480p'
THUMBNAIL_RESOLUTION = 'hqdefault'

THUMBNAIL_DIR = os.path.join(ROOT_DIR, 'thumbnails', THUMBNAIL_RESOLUTION)
VIDEO_DIR = os.path.join(ROOT_DIR, 'videos', VIDEO_RESOLUTION)
CAPTION_DIR = os.path.join(ROOT_DIR, 'captions', 'en')

ALTERNATE_THUMBNAIL_DIR = os.path.join('/storage/pratik/new_data/', 'thumbnails', THUMBNAIL_RESOLUTION)
ALTERNATE_CAPTION_DIR = os.path.join('/storage/pratik/new_data/', 'captions', 'en')

ONLY_VIDEO = False

all_quantiles = [0, 1, 2, 3, 10, 11, 12, 13, 20, 21, 22, 23, 30, 31, 32, 33]
selected_quantiles = [0, 33]

# Function to load video ids
def load_video_ids(file_name):
    df = pd.read_csv(os.path.join(ROOT_DIR, LOAD_DIR, file_name))
    video_ids = df['video_id'].tolist()

    # load progress csv file and remove already downloaded videos
    progress_file = os.path.join(ROOT_DIR, PROGRESS_DIR, 'progress.csv')
    if os.path.exists(progress_file):
        print('progress file exists\n you wont have to download the videos that are already downloaded')
        progress_df = pd.read_csv(progress_file)
        progress_ids = progress_df['video_id'].tolist()
        video_ids = [x for x in video_ids if x not in progress_ids]

    return video_ids

# Function to create directory if not exists
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Function to download video in 144p
def download_video(video_id, video_dir):
    url = f'https://www.youtube.com/watch?v={video_id}'
    yt = YouTube(url)
    try:

        video = yt.streams.filter(res=VIDEO_RESOLUTION, file_extension='mp4').first()
        if video.filesize < 1000000000:
            # video = filter.first()

            if video:
                video.download(video_dir, filename=video_id+'.mp4', max_retries=5)
                return ('', True)
            else:
                return (f"No {VIDEO_RESOLUTION} stream", False)
        else:
            return (f"Video size too large", False)
    except Exception as e:
        return (f"{e}", False)

def download_thumbnail(video_id, thumbnail_dir):
    url = 'https://img.youtube.com/vi/{}/{}.jpg'.format(video_id, THUMBNAIL_RESOLUTION)
    file_name = os.path.join(thumbnail_dir, video_id+'.jpg')
    try:
        urllib.request.urlretrieve(url, file_name)
        time.sleep(0.1)
        return ('', True)
    except Exception as e:
        return (f"{e}", False)

def download_caption(video_id, caption_dir):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
        # .format_transcript(transcript) turns the transcript into a JSON string.
        json_formatted = formatter.format_transcript(transcript)

        # Now we can write it out to a file.
        with open(os.path.join(caption_dir, video_id+'.json'), 'w') as f:
            f.write(json_formatted)
        return ('', True)
    except:
        try:
            list_transcripts = YouTubeTranscriptApi.list_transcripts(video_id)
            for transcripti in list_transcripts:
                transcript = transcripti.translate('en').fetch()
                break
            # .format_transcript(transcript) turns the transcript into a JSON string.
            json_formatted = formatter.format_transcript(transcript)
            # Now we can write it out to a file.
            with open(os.path.join(caption_dir, video_id+'.json'), 'w') as f:
                f.write(json_formatted)
            return ('', True)
        except Exception as e:
            return (f"{e}", False)

# Function to download videos for a list of video ids

def download_videos_for_video_ids(video_ids):
    progress_queue = queue.Queue()

    def download(video_id):
        try:
            if ONLY_VIDEO:
                error_video, success_video = download_video(video_id, VIDEO_DIR)
                # put progress in queue for video only
                progress_queue.put({
                    'video_id': video_id,
                    'success_video': success_video,
                    'error_video': error_video,
                })
            else:
                # check if video in VIDEO_DIR
                if( os.path.exists(os.path.join(VIDEO_DIR, video_id+'.mp4')) ):
                    error_video, success_video = ('', True)
                else:
                    error_video, success_video = download_video(video_id, VIDEO_DIR)
                if(os.path.exists(os.path.join(THUMBNAIL_DIR, video_id+'.jpg'))):
                    error_thumbnail, success_thumbnail = ('', True)
                elif( os.path.exists(os.path.join(ALTERNATE_THUMBNAIL_DIR, video_id+'.jpg')) ):
                    # copy the thumbnail
                    shutil.copy(os.path.join(ALTERNATE_THUMBNAIL_DIR, video_id+'.jpg'), os.path.join(THUMBNAIL_DIR, video_id+'.jpg'))
                    error_thumbnail, success_thumbnail = ('', True)
                else:
                    error_thumbnail, success_thumbnail = download_thumbnail(video_id, THUMBNAIL_DIR)
                if(os.path.exists(os.path.join(CAPTION_DIR, video_id+'.json'))):
                    error_caption, success_caption = ('', True)
                elif( os.path.exists(os.path.join(ALTERNATE_CAPTION_DIR, video_id+'.json')) ):
                    # copy the caption
                    shutil.copy(os.path.join(ALTERNATE_CAPTION_DIR, video_id+'.json'), os.path.join(CAPTION_DIR, video_id+'.json'))
                    error_caption, success_caption = ('', True)
                else:
                    error_caption, success_caption = download_caption(video_id, CAPTION_DIR)

                # put progress in queue
                progress_queue.put({
                    'video_id': video_id,
                    'success_video': success_video,
                    'success_thumbnail': success_thumbnail,
                    'success_caption': success_caption,
                    'error_video': error_video,
                    'error_thumbnail': error_thumbnail,
                    'error_caption': error_caption,
                })
        except Exception as e:
            print(f"Error downloading video {video_id}: {e}")

    def monitor_task(future, timeout):
        time.sleep(timeout)
        if not future.done():
            future.cancel()

    def download_with_timeout(video_id, timeout):
        future = Future()
        threading.Thread(target=monitor_task, args=(future, timeout)).start()

        try:
            result = download(video_id)
            if not future.set_running_or_notify_cancel():
                raise Exception('Task was cancelled')
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

        return future


    def write_progress():
        if not os.path.exists(os.path.join(ROOT_DIR, PROGRESS_DIR, 'progress.csv')):
            create_directory(os.path.join(ROOT_DIR, PROGRESS_DIR))
            if ONLY_VIDEO:
                with open(os.path.join(ROOT_DIR, PROGRESS_DIR, 'progress.csv'), 'w') as f:
                    f.write('video_id,video_downloaded,error_video\n')
            else:
                with open(os.path.join(ROOT_DIR, PROGRESS_DIR, 'progress.csv'), 'w') as f:
                    f.write('video_id,video_downloaded,thumbnail_downloaded,caption_downloaded,error_video,error_thumbnail,error_caption\n')
        while True:
            try:
                item = progress_queue.get_nowait()
                if item == 'DONE':
                    break
                if ONLY_VIDEO:
                    with open(os.path.join(ROOT_DIR, PROGRESS_DIR, 'progress.csv'), 'a') as f:
                        error_video = '"' + str(item['error_video']).replace('"', '""') + '"' if item['error_video'] else '""'
                        f.write(f"{item['video_id']},{item['success_video']},{error_video}\n")
                        
                else:
                    with open(os.path.join(ROOT_DIR, PROGRESS_DIR, 'progress.csv'), 'a') as f:
                        error_video = '"' + str(item['error_video']).replace('"', '""') + '"' if item['error_video'] else '""'
                        error_thumbnail = '"' + str(item['error_thumbnail']).replace('"', '""') + '"' if item['error_thumbnail'] else '""'
                        error_caption = '"' + str(item['error_caption']).replace('"', '""') + '"' if item['error_caption'] else '""'
                        f.write(f"{item['video_id']},{item['success_video']},{item['success_thumbnail']},{item['success_caption']},{error_video},{error_thumbnail},{error_caption}\n")

            except queue.Empty:
                time.sleep(0.1)  # wait for 100 milliseconds before trying again

    # start the write_progress thread
    progress_thread = threading.Thread(target=write_progress)
    progress_thread.start()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_with_timeout, video_id, 600) for video_id in video_ids}
        for future in tqdm.tqdm(as_completed(futures), total=len(video_ids)):
            try:
                future.result()  # this will raise an exception if the task failed
            except Exception as e:
                print(f"Error: {e}")

    # signal the write_progress thread to stop
    progress_queue.put('DONE')
    progress_thread.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download YouTube videos, thumbnails, and captions.')
    parser.add_argument('-q', '--quantile', type=int, required=True, help='Quantile to download videos from.')
    parser.add_argument('-n', '--num_videos', type=int, required=True, help='Number of videos to download.')
    args = parser.parse_args()
    create_directory(THUMBNAIL_DIR)
    create_directory(CAPTION_DIR)
    create_directory(VIDEO_DIR)
    video_ids = load_video_ids(f'quantile_{args.quantile}.csv')
    # Limit the number of videos to download
    video_ids = video_ids[:args.num_videos]
    print('Downloading videos for quantile', args.quantile, 'with', len(video_ids), 'videos.')
    download_videos_for_video_ids(video_ids)
    print(f"Downloaded videos for quantile {args.quantile}")