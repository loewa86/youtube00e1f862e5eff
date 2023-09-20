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
import yake
import re
import string
from keybert import KeyBERT
try:
    import nltk
    nltk.download('punkt')
except:
    print("nltk already downloaded or error")


def is_good_1gram(word):
    special_chars = set(string.punctuation.replace("-", ""))
    length = len(word)
    
    if length <= 3:
        return all(char == "-" or char not in special_chars for char in word)
    else:
        return all(char not in special_chars for char in word)


def filter_strings(input_list):
    output_list = []
    for s in input_list:
        if not isinstance(s, str):  # Check if s is a string
            continue  # Skip this iteration of the loop if s is not a string
        if not is_good_1gram(s):
            continue # skip if bad word, bad!
        # Count the number of special characters in the string
        special_char_count = sum([1 for char in s if (char in string.punctuation or char.isnumeric() or not char.isalpha())])

        # Remove leading and trailing special characters
        s = re.sub('^[^A-Za-z0-9 ]+|[^A-Za-z0-9 ]+$', '', s)
        s = re.sub(r'\\u[\da-fA-F]{4}', '', s)

        # Check if there's any alphabetical character in the string
        contains_letter = any(char.isalpha() for char in s)

        # If the number of special characters is less than 30% of the total characters and the string contains at least one letter, add to output list
        if len(s) > 0 and special_char_count * 100 / len(s) <= 20 and contains_letter:
            if s not in output_list:
                output_list.append(s)

    return output_list

### YAKE PARAMETERS

language = "en"
deduplication_thresold = 0.9
deduplication_algo = 'seqm'
windowSize = 1
max_ngram_size_1 = 1 # 1-grams are enough for most use cases
numOfKeywords_1 = 15 # important to keep high enough

kw_extractor1 = yake.KeywordExtractor(
    lan=language,
    n=max_ngram_size_1,
    dedupLim=deduplication_thresold,
    dedupFunc=deduplication_algo,
    windowsSize=windowSize,
    top=numOfKeywords_1,
)

_extract_keywords1 = lambda text: kw_extractor1.extract_keywords(text)

language = "en"
deduplication_thresold = 0.9
deduplication_algo = 'seqm'
windowSize = 7
max_ngram_size_2 = 2 # 2-grams
numOfKeywords_2 = 10 # important to keep high enough

kw_extractor2 = yake.KeywordExtractor(
    lan=language,
    n=max_ngram_size_2,
    dedupLim=deduplication_thresold,
    dedupFunc=deduplication_algo,
    windowsSize=windowSize,
    top=numOfKeywords_2,
)
_extract_keywords_bis = lambda text: kw_extractor2.extract_keywords(text)

_kw_bert_model = KeyBERT(model='all-MiniLM-L6-v2')
th_kw_bert = 0.175
_extract_keywords2 = lambda text: [keyword[0] for keyword in _kw_bert_model.extract_keywords(text) if keyword[1] > th_kw_bert]

def get_extra_special_keywords(text):
    def is_valid_keyword(word):
        uppercase_count = sum(1 for char in word if char.isupper())
        isalpha_count = sum(1 for char in word if char.isalpha())
        total_chars = len(word)
        punctuation = re.compile(r'[^\w\s,]')
        return (uppercase_count / total_chars >= 0.3) and (punctuation.search(word) is not None) and (isalpha_count>1)
    
    words = nltk.word_tokenize(text)
    filtered_words = filter(is_valid_keyword, words)
    return list(filtered_words)

def get_concatened_keywords(strings_list):
    # Check if some keywords are made of concatenated words (of size >=2). Exemple: "$RET#Renewable_Energy_Token" becomes "$RET","#Renewable_Energy_Token"
    # Find all groups of [$,#,@] followed by uppercase letters and lowercase letters
    # If the number of groups is greater than 1, split the string into multiple strings
    # Define the regular expression pattern to match the special characters '$' and '#'
    output_list = []
    for s in strings_list:
        pattern = r'[$#]+'
        # Use re.split to split the input string based on the pattern
        parts = re.split(pattern, s)
        # Filter out empty strings from the result
        parts = [part for part in parts if part]
        if len(parts) > 1:
            for part in parts:
                if len(part) > 2:
                    output_list.append(part)
            continue
        output_list.append(s)
    return output_list

# get $XXXX and XXXX$ words for stocks and crypto
def get_ticker_symbols(text):
    text = text.replace("\n", "\n ")
    text = text.replace("$", " $")
    words = text.split(" ")
    words = [word for word in words if word.startswith("$") and len(word) > 1]
    # remove punctuation like comma and dot
    words = [word.replace(",", "").replace(".", "") for word in words]
    return words
    
def get_symbol_acronyms(text):
    # GET "S&P" "SP500" "5G" "AI" types of acronyms
    # it must have at least 40% upper  case letters
    # up to 80% digits
    # and up to 50% special characters
    # and at least 2 characters
    def is_valid_acronym(word):
        uppercase_count = sum(1 for char in word if char.isupper())
        isalpha_count = sum(1 for char in word if char.isalpha())
        total_chars = len(word)
        return (uppercase_count / total_chars >= 0.3) and (isalpha_count>=1) and len(word) >= 2
    
    # split by space and special punctuation: comma, point, period
    # not nltk tokenize
    words = text.split(" ")
    words = [word for word in words if len(word) > 1]
    # remove punctuation like comma and dot
    words = [word.replace(",", "").replace(".", "") for word in words]    
    filtered_words = filter(is_valid_acronym, words)   
    acronyms = list(filtered_words) # make it a normal python list
    return acronyms

def remove_invalid_keywords(input_list):
    output_list = []
    for s in input_list:
        # remove any double slash and any url. ex: "//CONNECT.COM" and "https://CONNECT.COM"
        s = re.sub(r'//|https?:\/\/.*[\r\n]*', '', s)
        if len(s) > 2:
            output_list.append(s)
    return output_list

def extract_keywords(content):   
    kx1 = _extract_keywords1(content)
    keywords_weighted = list(set(kx1))
    keywords_ = [e[0] for e in set(keywords_weighted)]
    keywords_.extend(_extract_keywords2(content))    
    kx2 = _extract_keywords_bis(content)
    keywords_weighted = list(set(kx2))
    keywords_.extend([e[0] for e in set(keywords_weighted) if e[1] > 0.5])
    keywords_ = filter_strings(keywords_)
    try:
        keywords_.extend(get_ticker_symbols(content))   
    except Exception as e:
        print(f"Error in ticker symbols extraction: {e}")
    try:
        bonus_keywords = get_extra_special_keywords(content)
        keywords_.extend(bonus_keywords)         
        acronyms = get_symbol_acronyms(content)
        keywords_.extend(acronyms)
        keywords_ = get_concatened_keywords(keywords_)
        keywords_ = remove_invalid_keywords(keywords_)
    except Exception as e:
        print(f"Error in advanced keywords extraction: {e}")
    return list(set(keywords_))

def clean_strings(input_list):
    # Convert all strings to lowercase for case insensitivity
    input_list = [s.lower() for s in input_list]

    # Remove duplicates while preserving order
    unique_strings = list(dict.fromkeys(input_list))

    # Filter out 1-word strings that are substrings of 2-grams
    cleaned_strings = []
    for s1 in unique_strings:
        is_substring = False
        for s2 in unique_strings:
            if s1 != s2 and s1 in s2:
                is_substring = True
                break
        if not is_substring:
            cleaned_strings.append(s1)

    # Convert the cleaned strings back to their original case
    cleaned_strings = [s.title() for s in cleaned_strings]

    return cleaned_strings

try:
    extract_keywords_title = lambda text: clean_strings(extract_keywords(text))
except Exception as e:
    print("[Youtube] FAILED extract_keywords_title: ",e)


############################################################################################################
############################################################################################################
############################################################################################################

############################################################################################################
############################################################################################################
############################################################################################################
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
["news", "news", "press", "silentsunday", "saturday", "monday", "tuesday" "bitcoin", "ethereum", "eth", "btc", "usdt", "cryptocurrency", "solana", "crypto", "spy", "s&p", "protor gamble",
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
        chosen_suffix = random.choices(suffixes, weights=[0.40, 0.10, 0.30, 0.20])[0]
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

    yielded_items = 0
    nb_comments_checked = 0
    urls = extract_url_parts(urls)
    for url, title in zip(urls, titles):
        await asyncio.sleep(1) 
        # skip URL randomly with 10% chance
        if random.random() < 0.1:
            continue
        youtube_video_url = url
        # Run the generator function and handle the timeout
        comments_list = []
        try:
            comments_list = yt_comment_dl.get_comments_from_url(url, sort_by=SORT_BY_RECENT)
        except Exception as e:      
            logging.exception(f"[Youtube] get_comments_from_url - error: {e}")

        # turn generator into list
        comments_list = list(comments_list)
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
            try:
                title_kws = extract_keywords_title(title)
            except Exception as e:
                logging.exception(f"[Youtube] extract_keywords_title - error: {e}")
                title_kws = []
            # shuffle the list
            random.shuffle(title_kws)
            # select 70% strings from title_kws and order them randomly in a title_str
            title_string = ""   
            for kw in title_kws:
                if random.random() <= 0.63:
                    title_string += kw + " "
                    # remove all non alphanumeric characters
                    title_string = re.sub(r'\W+', ' ', title_string)
            title_kws = title_string

            comment_content = title_string + " . " + comment['text']
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
            yielded_items += 1
            yield item
            if yielded_items >= maximum_items_to_collect:
                break
    except asyncio.exceptions.TimeoutError:
        logging.info(f"[Youtube] Internal requests are taking longer than {REQUEST_TIMEOUT} - we must give up & move on. Check your network.")
            
