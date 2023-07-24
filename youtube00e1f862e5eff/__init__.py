
import time
import asyncio
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
MAX_TOTAL_COMMENTS_TO_CHECK = 500
PROBABILITY_ADDING_SUFFIX = 0.75
PROBABILITY_DEFAULT_KEYWORD = 0.3

DEFAULT_OLDNESS_SECONDS = 360
DEFAULT_MAXIMUM_ITEMS = 25
DEFAULT_MIN_POST_LENGTH = 10

DEFAULT_KEYWORDS = \
["news", "news", "press", "silentsunday", "saturday", "monday", "tuesday" "bitcoin", "ethereum", "eth", "btc", "usdt", "cryptocurrency", "solana",
"doge", "cardano", "monero", "dogecoin", "polkadot", "ripple", "xrp", "stablecoin", "defi", "cbdc", "nasdaq", "sp500",  "BNB", "ETF", "SpotETF", "iphone", "it",
"usbc", "eu", "hack", "staking", "proof of work", "hacker", "hackers", "virtualreality", "metaverse", "tech", "technology", "art", "game", "trading", "groundnews", "breakingnews",
"Gensler", "FED", "SEC", "IMF", "Macron", "Biden", "Putin", "Zelensky", "Trump", "legal", "bitcoiners", "bitcoincash", "ethtrading", "cryptonews",
"cryptomarket", "cryptoart", "CPTPP", "brexit", "trade", "economy", "USpolitics", "UKpolitics", "NHL", "computer", "computerscience", "stem", "gpt4",
"billgates", "ai", "chatgpt", "openai", "wissen", "french", "meat", "support", "aid", "mutualaid", "mastodon", "bluesky", "animal", "animalrights",
"BitcoinETF", "Crypto", "altcoin", "DeFi", "GameFi", "web3", "web3", "trade",  "NFT", "NFTs", "cryptocurrencies", "Cryptos", "reddit", "elon musk",
"politics", "business", "twitter", "digital", "airdrop", "gamestop", "finance", "liquidity","token", "economy", "markets", "stocks", "crisis", "gpt", "gpt3",
"russia", "war", "ukraine", "luxury", "LVMH", "Elon musk", "conflict", "bank", "Gensler", "emeutes", "FaceID", "Riot", "riots", "riot", "France",
"UnitedStates", "USA", "China", "Germany", "Europe", "Canada", "Mexico", "Brazil", "price", "market", "NYSE","NASDAQ", "CAC", "CAC40", "G20", "OilPrice", 
"FTSE", "NYSE", "WallStreet", "money", "forex", "trading", "currency", "USD", "WarrenBuffett", "BlackRock", "Berkshire", "IPO", "Apple", "Tesla","Alphabet",
 "FBstock","debt", "bonds", "XAUUSD", "SP500", "DowJones", "satoshi", "shorts", "live", "algotrading", "tradingalgoritmico", "prorealtime", "ig", "igmarkets", 
 "win", "trading", "trader", "algorithm", "cfdauto", "algos", "bottrading", "tradingrobot", "robottrading", "prorealtimetrading", "algorithmictrading",
"usa ", "canada ", "denmark", "russia", "japan", "italy", "spain", "uk", "eu", "social", "iran", "war","socialism", "Biden", "democracy", "justice", "canada", "leftist",
"election", "vote", "protocol", "network", "org", "organization", "charity", "money", "scam", "token", "tokens", "ecosystem",
"rightwing",  "DAX", "NASDAQ", "RUSSELL", "RUSSELL2000", "GOLD", "XAUUSD", "DAX40", "IBEX", "IBEX35", "oil", "crude", "crudeoil", "us500", "russell", "russell2000", "worldcoin", "sam atlman", "elon musk"]

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

yt_comment_dl = YoutubeCommentDownloader()

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

def randomly_add_search_filter(input_URL, p):
    suffixes = [
        "&sp=CAI%253D",      # newest_videos_suffix
        "&sp=CAASAhAB",      # relevance_videos_suffix
        "&sp=EgQIARAB",      # last_hour_videos_suffix
        "&sp=EgQIAhAB"       # last_day_videos_suffix
    ]
    if random.random() < p:
        # Choose one of the suffixes based on probability distribution
        chosen_suffix = random.choices(suffixes, weights=[0.25, 0.40, 0.15, 0.2])[0]
        return input_URL + chosen_suffix
    else:
        return input_URL

async def scrape(keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check):
    URL = "https://www.youtube.com/results?search_query={}".format(keyword)
    URL = randomly_add_search_filter(URL, p= PROBABILITY_ADDING_SUFFIX )
    logging.info(f"[Youtube] Looking at video URL: {URL}")
    response = requests.get(URL, headers={'User-Agent': random.choice(USER_AGENT_LIST)}, timeout=8.0)

    soup = BeautifulSoup(response.text, 'html.parser')

    URLs_remaining_trials = 10
    # Find the script tag containing the JSON data
    script_tag = soup.find('script', text=lambda text: text and 'var ytInitialData' in text)

    urls = []
    titles = []
    if script_tag:
        await asyncio.sleep(0.2) 
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
                        urls.append(full_url)
                        titles.append(title)

        except json.JSONDecodeError:
            logging.info("[Youtube] Invalid JSON data in var ytInitialData.")
    else:
        logging.info("[Youtube] No ytInitialData found.")

    yielded_items = 0
    nb_comments_checked = 0
    urls = extract_url_parts(urls)
    for url, title in zip(urls, titles):
        await asyncio.sleep(0.2) 
        # skip URL randomly with 10% chance
        if random.random() < 0.1:
            continue
        comments = yt_comment_dl.get_comments_from_url(url, sort_by=SORT_BY_RECENT)
        youtube_video_url = url
        comments_list = list(comments)
        nb_comments = len(comments_list)
        nb_comments_checked += nb_comments
        logging.info(f"[Youtube] checking the {nb_comments} comments on video: {title}")
        for comment in comments_list:
            comment_timestamp = int(round(comment['time_parsed'],1))
            comment_url = youtube_video_url + "&lc=" +  comment['cid']
            comment_id = comment['cid']
            comment_content = title + " . " + comment['text']
            comment_datetime = convert_timestamp(comment_timestamp)
            if is_within_timeframe_seconds(comment_timestamp, max_oldness_seconds):
                comment_obj = {'url':comment_url, 'content':comment_content, 'title':title, 'created_at':comment_datetime, 'external_id':comment_id}
                logging.info("[Youtube] found new comment: ",comment_obj)
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
        if nb_comments_checked >= max_total_comments_to_check:
            break
        
        URLs_remaining_trials -= 1
        if URLs_remaining_trials <= 0:
            break
            
def randomly_replace_or_choose_keyword(input_string, p):
    if random.random() < p:
        return input_string
    else:
        return random.choice(DEFAULT_KEYWORDS)

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

        try:
            probability_to_select_default_kws = parameters.get("probability_to_select_default_kws", PROBABILITY_DEFAULT_KEYWORD)
        except KeyError:
            probability_to_select_default_kws = PROBABILITY_DEFAULT_KEYWORD

        try:
            max_total_comments_to_check = parameters.get("max_total_comments_to_check", MAX_TOTAL_COMMENTS_TO_CHECK)
        except KeyError:
            max_total_comments_to_check = MAX_TOTAL_COMMENTS_TO_CHECK


    else:
        # Assign default values if parameters is empty or None
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH
        probability_to_select_default_kws = PROBABILITY_DEFAULT_KEYWORD
        max_total_comments_to_check = MAX_TOTAL_COMMENTS_TO_CHECK

    return max_oldness_seconds, maximum_items_to_collect, min_post_length, probability_to_select_default_kws, max_total_comments_to_check


def convert_spaces_to_plus(input_string):
    return input_string.replace(" ", "+")

async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    yielded_items = 0
    max_oldness_seconds, maximum_items_to_collect, min_post_length, probability_to_select_default_kws, max_total_comments_to_check  = read_parameters(parameters)
    selected_keyword = ""
    
    try:
        if "keyword" in parameters:
            selected_keyword = parameters["keyword"]
        # replace it, with some probability, by a main default keyword
        selected_keyword = randomly_replace_or_choose_keyword(selected_keyword, p=probability_to_select_default_kws)
        selected_keyword = convert_spaces_to_plus(selected_keyword)
    except Exception as e:
        logging.exception(f"[Youtube parameters] parameters: {parameters}. Error when reading keyword: {e}")        
        selected_keyword = randomly_replace_or_choose_keyword("", p=1)

    logging.info(f"[Youtube] - Scraping latest comments posted less than {max_oldness_seconds} seconds ago, on youtube videos related to keyword: {selected_keyword}.")
    async for item in scrape(selected_keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check):
        yielded_items += 1
        yield item
        if yielded_items >= maximum_items_to_collect:
            break
