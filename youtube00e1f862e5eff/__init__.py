import re
from typing import AsyncGenerator
import aiohttp
import dateparser
import time
import asyncio
import requests
import random
import json
from bs4 import BeautifulSoup
from typing import AsyncGenerator
from datetime import datetime
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
try:
    import nltk
    nltk.download('stopwords')
    stopwords = nltk.corpus.stopwords.words('english')
except Exception as e:
    logging.exception(f"[Youtube] nltk.corpus.stopwords.words('english') error: {e}")
    stopwords = []

"""
- Fetch https://www.youtube.com/results?search_query={KEYWORD} example: https://www.youtube.com/results?search_query=bitcoin
- Get all video URLs + their titles
- use youtube-comment library to extract all comments (with id, timestamp, and text)
- rebuild comment URLs from id, select those with recent timestamp
- add title to comment text (as first sentence).
- that's all folks
"""

YOUTUBE_VIDEO_URL = 'https://www.youtube.com/watch?v={youtube_id}'
YOUTUBE_CONSENT_URL = 'https://consent.youtube.com/save'

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

NB_AJAX_CONSECUTIVE_MAX_TRIALS = 15
REQUEST_TIMEOUT = 10
POST_REQUEST_TIMEOUT = 4
SORT_BY_POPULAR = 0
SORT_BY_RECENT = 1


YT_CFG_RE = r'ytcfg\.set\s*\(\s*({.+?})\s*\)\s*;'
YT_INITIAL_DATA_RE = r'(?:window\s*\[\s*["\']ytInitialData["\']\s*\]|ytInitialData)\s*=\s*({.+?})\s*;\s*(?:var\s+meta|</script|\n)'
YT_HIDDEN_INPUT_RE = r'<input\s+type="hidden"\s+name="([A-Za-z0-9_]+)"\s+value="([A-Za-z0-9_\-\.]*)"\s*(?:required|)\s*>'


class YoutubeCommentDownloader:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = USER_AGENT
        self.session.cookies.set('CONSENT', 'YES+cb', domain='.youtube.com')                
        self.MAX_ITERATIONS_CONTINUATIONS_AJAX = 10000

    def ajax_request(self, endpoint, ytcfg, retries=5, sleep=7):
        url = 'https://www.youtube.com' + endpoint['commandMetadata']['webCommandMetadata']['apiUrl']

        data = {'context': ytcfg['INNERTUBE_CONTEXT'],
                'continuation': endpoint['continuationCommand']['token']}

        for _ in range(retries):
            response = self.session.post(url, params={'key': ytcfg['INNERTUBE_API_KEY']}, json=data, timeout=POST_REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response.json()
            if response.status_code in [403, 413]:
                return {}
            else:
                # print("Response status code: %d. Retrying in %d seconds" % (response.status_code, sleep))
                time.sleep(sleep)

    def get_comments(self, youtube_id, *args, **kwargs):
        return self.get_comments_from_url(YOUTUBE_VIDEO_URL.format(youtube_id=youtube_id), *args, **kwargs)

    def get_comments_from_url(self, youtube_url, sort_by=SORT_BY_RECENT, language=None, sleep=0.25):
        response = self.session.get(youtube_url, timeout=REQUEST_TIMEOUT)

        if 'consent' in str(response.url):
            # We may get redirected to a separate page for cookie consent. If this happens we agree automatically.
            params = dict(re.findall(YT_HIDDEN_INPUT_RE, response.text))
            params.update({'continue': youtube_url, 'set_eom': False, 'set_ytc': True, 'set_apyt': True})
            response = self.session.post(YOUTUBE_CONSENT_URL, params=params, timeout=POST_REQUEST_TIMEOUT)

        html = response.text
        ytcfg = json.loads(self.regex_search(html, YT_CFG_RE, default=''))
        if not ytcfg:
            return  # Unable to extract configuration
        if language:
            ytcfg['INNERTUBE_CONTEXT']['client']['hl'] = language

        data = json.loads(self.regex_search(html, YT_INITIAL_DATA_RE, default=''))

        item_section = next(self.search_dict(data, 'itemSectionRenderer'), None)
        renderer = next(self.search_dict(item_section, 'continuationItemRenderer'), None) if item_section else None
        if not renderer:
            # Comments disabled?
            return

        sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu:
            # No sort menu. Maybe this is a request for community posts?
            section_list = next(self.search_dict(data, 'sectionListRenderer'), {})
            continuations = list(self.search_dict(section_list, 'continuationEndpoint'))
            # Retry..
            data = self.ajax_request(continuations[0], ytcfg) if continuations else {}
            sort_menu = next(self.search_dict(data, 'sortFilterSubMenuRenderer'), {}).get('subMenuItems', [])
        if not sort_menu or sort_by >= len(sort_menu):
            raise RuntimeError('Failed to set sorting')
        continuations = [sort_menu[sort_by]['serviceEndpoint']]

        while continuations:
            continuation = continuations.pop()
            response = self.ajax_request(continuation, ytcfg)

            if not response or self.MAX_ITERATIONS_CONTINUATIONS_AJAX == 0:
                break
            self.MAX_ITERATIONS_CONTINUATIONS_AJAX -= 1

            error = next(self.search_dict(response, 'externalErrorMessage'), None)
            if error:
                raise RuntimeError('Error returned from server: ' + error)

            actions = list(self.search_dict(response, 'reloadContinuationItemsCommand')) + \
                      list(self.search_dict(response, 'appendContinuationItemsAction'))
            for action in actions:
                for item in action.get('continuationItems', []):
                    if action['targetId'] in ['comments-section',
                                              'engagement-panel-comments-section',
                                              'shorts-engagement-panel-comments-section']:
                        # Process continuations for comments and replies.
                        continuations[:0] = [ep for ep in self.search_dict(item, 'continuationEndpoint')]
                    if action['targetId'].startswith('comment-replies-item') and 'continuationItemRenderer' in item:
                        # Process the 'Show more replies' button
                        continuations.append(next(self.search_dict(item, 'buttonRenderer'))['command'])

            for comment in reversed(list(self.search_dict(response, 'commentRenderer'))):
                result = {'cid': comment['commentId'],
                          'text': ''.join([c['text'] for c in comment['contentText'].get('runs', [])]),
                          'time': comment['publishedTimeText']['runs'][0]['text'],
                          'author': comment.get('authorText', {}).get('simpleText', ''),
                          'channel': comment['authorEndpoint']['browseEndpoint'].get('browseId', ''),
                          'votes': comment.get('voteCount', {}).get('simpleText', '0'),
                          'photo': comment['authorThumbnail']['thumbnails'][-1]['url'],
                          'heart': next(self.search_dict(comment, 'isHearted'), False),
                          'reply': '.' in comment['commentId']}

                try:
                    result['time_parsed'] = dateparser.parse(result['time'].split('(')[0].strip()).timestamp()
                except AttributeError:
                    pass

                paid = (
                    comment.get('paidCommentChipRenderer', {})
                    .get('pdgCommentChipRenderer', {})
                    .get('chipText', {})
                    .get('simpleText')
                )
                if paid:
                    result['paid'] = paid
                
                yield result
            # print("Sleeping for %s seconds" % sleep)
            time.sleep(sleep)

    @staticmethod
    def regex_search(text, pattern, group=1, default=None):
        match = re.search(pattern, text)
        return match.group(group) if match else default

    @staticmethod
    def search_dict(partial, search_key):
        stack = [partial]
        while stack:
            current_item = stack.pop()
            if isinstance(current_item, dict):
                for key, value in current_item.items():
                    if key == search_key:
                        yield value
                    else:
                        stack.append(value)
            elif isinstance(current_item, list):
                stack.extend(current_item)
                
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
        chosen_suffix = random.choices(suffixes, weights=[0.20, 0.10, 0.35, 0.35])[0]
        return input_URL + chosen_suffix
    else:
        return input_URL
    
async def scrape(keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check):
    URL = "https://www.youtube.com/results?search_query={}".format(keyword)
    URL = randomly_add_search_filter(URL, p=PROBABILITY_ADDING_SUFFIX)
    logging.info(f"[Youtube] Looking at video URL: {URL}")

    async with aiohttp.ClientSession(headers={'User-Agent': random.choice(USER_AGENT_LIST)}) as session:
        try:
            async with session.get(URL, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                html = await response.text()
        except aiohttp.ClientError as e:
            logging.error(f"An error occurred during the request: {e}")
            return

    soup = BeautifulSoup(html, 'html.parser')

    URLs_remaining_trials = 10
    await asyncio.sleep(2)
    # Find the script tag containing the JSON data
    script_tag = soup.find('script', string=lambda text: text and 'var ytInitialData' in text)

    urls = []
    titles = []
    if script_tag:
        await asyncio.sleep(0.1) 
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


    last_n_video_comment_count = []
    n_rolling_size = 10
    n_rolling_size_min = 4

    yielded_items = 0
    nb_comments_checked = 0
    urls = extract_url_parts(urls)
    for url, title in zip(urls, titles):
        await asyncio.sleep(1) 
        # skip URL randomly with 10% chance
        if random.random() < 0.1:
            logging.info(f"[Youtube] Randomly skipping URL: {url}")
            continue
        youtube_video_url = url
        # Run the generator function and handle the timeout
        comments_list = []       
        ###################################################################
        # Exponential backoff to prevent rate limiting
        # use a polynomial formula based on the number of 0 in last_n_video_comment_count
        # f(nb_zeros) = 3 + nb_zeros^2
        nb_zeros = 0
        # iterate from the end of the array, count consecutive 0 and break when we find a non 0
        if len(last_n_video_comment_count) >= n_rolling_size_min:
            for i in range(len(last_n_video_comment_count)-1, -1, -1):
                if last_n_video_comment_count[i] == 0:
                    nb_zeros += 1
                else:
                    break
                # compute the sleep time
            random_inter_sleep = round(1 + nb_zeros**1.5,0) ## 1.5 is the exponent
            logging.info(f"[Youtube] [RATE LIMITE PREVENTION] Waiting  {random_inter_sleep} seconds...")
            await asyncio.sleep(random_inter_sleep)
        ###################################################################

        try:
            comments_list = yt_comment_dl.get_comments_from_url(url, sort_by=SORT_BY_RECENT)

            ###### ROLLING WINDOWS OF COMMENTS COUNT ######
            ### ADD LATEST COMMENTS COUNT TO THE ROLLING WINDOW
            # turn generator into list
            comments_list = list(comments_list)
            last_n_video_comment_count.append(len(comments_list))
            ### REMOVE THE OLDEST COMMENTS COUNT FROM THE ROLLING WINDOW
            if len(last_n_video_comment_count) > n_rolling_size:
                last_n_video_comment_count.pop(0)
            ### CHECK IF THE ROLLING WINDOW IS FULL
            if len(last_n_video_comment_count) == n_rolling_size:
                ### CHECK IF THE ROLLING WINDOW IS FULL OF 0
                if sum(last_n_video_comment_count) == 0:
                    ### IF YES, STOP THE PROCESS
                    logging.info("[Youtube] [RATE LIMITE PROTECTION] The rolling window of comments count is full of 0s. Stopping the scraping iteration...")
                    break
        except Exception as e:      
            logging.exception(f"[Youtube] get_comments_from_url - error: {e}")

        nb_comments = len(comments_list)
        nb_comments_checked += nb_comments
        logging.info(f"[Youtube] checking the {nb_comments} comments on video: {title}")
        for comment in comments_list:
            try:
                comment_timestamp = int(round(comment['time_parsed'],1))
            except Exception as e:
                logging.exception(f"[Youtube] parsing comment datetime error: {e}\n \
                THIS CAN BE DUE TO FOREIGN/SPECIAL DATE FORMAT, not handled at this date.\n Please report this to the Exorde discord, with your region/VPS location.")

            comment_url = youtube_video_url + "&lc=" +  comment['cid']
            comment_id = comment['cid']
            # make a titled_context from the title of the video, without special characters and punctuation
            # randomly remove some words from the title & stop words        
            try:
                title_base = " ".join([word for word in title.split(" ") if word not in stopwords])
                titled_context = title_base
            except Exception as e:
                logging.exception(f"[Youtube] stopwords error: {e}")
                titled_context = title
            if random.random() < 0.15:
                # remove up to 40% of the title
                titled_context = " ".join([word for word in title.split(" ") if random.random() > 0.4])
            elif random.random() < 0.30:
                # remove up to 20% of the title
                titled_context = " ".join([word for word in title.split(" ") if random.random() > 0.3])
            # remove non alpha-numeric characters that are single words
            titled_context = " ".join([word for word in titled_context.split(" ") if word.isalnum() and len(word) > 1])
            # add a dot at the end
            comment_content = titled_context + ". " + comment['text']
            comment_datetime = convert_timestamp(comment_timestamp)
            if is_within_timeframe_seconds(comment_timestamp, max_oldness_seconds):
                comment_obj = {'url':comment_url, 'content':comment_content, 'title':title, 'created_at':comment_datetime, 'external_id':comment_id}
                logging.info(f"[Youtube] found new comment: {comment_obj}")
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
    
    content_map = {}
    await asyncio.sleep(1)
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
    try:
        async for item in scrape(selected_keyword, max_oldness_seconds, maximum_items_to_collect, max_total_comments_to_check):
            # check if the content is not already in the map
            if item['content'] in content_map:
                continue
            else:
                content_map[item['content']] = True
            # check if the content is not too short
            if len(item.content) < min_post_length:
                continue
            yielded_items += 1
            yield item
            if yielded_items >= maximum_items_to_collect:
                break
    except asyncio.exceptions.TimeoutError:
        logging.info(f"[Youtube] Internal requests are taking longer than {REQUEST_TIMEOUT} - we must give up & move on. Check your network.")
