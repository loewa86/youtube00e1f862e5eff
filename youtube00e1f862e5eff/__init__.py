
import time
import requests
import random
import json
from bs4 import BeautifulSoup
from typing import AsyncGenerator
from datetime import datetime, timedelta
from itertools import islice
from youtube_comment_downloader import * #youtube_comment_downloader==0.1.68
from exorde_data import (
    Item,
    Content,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId
)
import logging

"""
- Fetch https://www.youtube.com/results?search_query={KEYWORD} example: https://www.youtube.com/results?search_query=bitcoin
- Get all video URLs + their titles
- use youtube-comment library to extract all comments (with id, timestamp, and text)
- rebuild comment URLs from id, select those with recent timestamp
- add title to comment text (as first sentence).
- that's all folks
"""

global MAX_EXPIRATION_SECONDS
MAX_EXPIRATION_SECONDS = 360

yt_comment_dl = YoutubeCommentDownloader()

# GLOBAL VARIABLES
USER_AGENT_LIST = [
    'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15'
]
DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10


def is_within_timeframe_seconds(input_timestamp, timeframe_sec):
    input_timestamp = int(input_timestamp)
    current_timestamp = int(time.time())  # Get the current UNIX timestamp
    elapsed_time = current_timestamp - input_timestamp

    if elapsed_time <= timeframe_sec:
        return True
    else:
        return False
    
def extract_url_parts(urls):
    result = []
    for url in urls:
        # Split the URL at the '&' character and keep only the first part
        url_part = url.split('&')[0]
        result.append(url_part)
    return result

def convert_timestamp(timestamp):
    dt = datetime.utcfromtimestamp(int(timestamp))
    formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.00Z")
    return formatted_dt

async def scrape(keyword, max_oldness_seconds, maximum_items_to_collect):
    URL = "https://www.youtube.com/results?search_query={}".format(keyword)
    print("Youtube URL: ",URL)
    response = requests.get(URL, headers={'User-Agent': random.choice(USER_AGENT_LIST)}, timeout=8.0)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the script tag containing the JSON data
    script_tag = soup.find('script', text=lambda text: text and 'var ytInitialData' in text)

    urls = []
    titles = []
    if script_tag:
        # Extract the JSON content
        json_str = script_tag.text
        start_index = json_str.find('var ytInitialData = ') + len('var ytInitialData = ')
        end_index = json_str.rfind('};') + 1
        json_data_str = json_str[start_index:end_index]
        try:
            # Parse the JSON data
            data = json.loads(json_data_str)

            # Extract titles and URLs
            if 'contents' in data:
                primary_contents = data['contents']['twoColumnSearchResultsRenderer']['primaryContents']
                for item in primary_contents['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents']:
                    if 'videoRenderer' in item:
                        video = item['videoRenderer']
                        title = video['title']['runs'][0]['text']
                        url_suffix = video['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url']
                        full_url = f"https://www.youtube.com{url_suffix}"
                        print(f"Title: {title}, URL: {full_url}")
                        urls.append(full_url)
                        titles.append(title)

        except json.JSONDecodeError:
            print("[Youtube-text] Invalid JSON data in var ytInitialData.")
    else:
        print("[Youtube-text] No ytInitialData found.")

    yielded_items = 0
    urls = extract_url_parts(urls)
    for url, title in zip(urls, titles):
        print("URL: ",url)
        comments = yt_comment_dl.get_comments_from_url(url, sort_by=SORT_BY_RECENT)
        youtube_video_url = url
        for comment in islice(comments, 10):
            comment_timestamp = int(round(comment['time_parsed'],1))
            comment_url = youtube_video_url + "&lc=" +  comment['cid']
            comment_id = comment['cid']
            comment_content = title + " . " + comment['text']
            comment_datetime = convert_timestamp(comment_timestamp)
            if is_within_timeframe_seconds(comment_timestamp, max_oldness_seconds):
                comment_obj = {'url':comment_url, 'content':comment_content, 'title':title, 'created_at':comment_datetime, 'external_id':comment_id}
                print("[Youtube-text] found new comment: ",comment_obj)
                yield Item(
                    content=Content(str(comment_content)),
                    created_at=CreatedAt(str(comment_obj['created_at'])),
                    title=Title(str(comment_obj['title'])),
                    domain=Domain("youtube.com"),
                    url=Url(comment_url),
                    external_id=ExternalId(str(comment_obj['external_id']))
                )
                yielded_items += 1
                if yielded_items >= maximum_items_to_collect:
                    break
            

def read_parameters(parameters):
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return max_oldness_seconds, maximum_items_to_collect, min_post_length


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    selected_keyword = parameters['keyword']
    logging.info(f"[Youtube] - Scraping ideas posted less than {max_oldness_seconds} seconds ago.")

    async for item in scrape(selected_keyword, max_oldness_seconds):
        yielded_items += 1
        yield item
        if yielded_items >= maximum_items_to_collect:
            break
