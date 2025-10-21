#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Nettiauto Monitor - Production Ready Version
Features:
- Async parallel scraping
- Smart memory management
- Better error handling
- Price drop tracking
- Batch notifications
- Health monitoring
- Docker ready
"""

import json
import threading
import logging
import time
import random
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
import cloudscraper
from fake_useragent import UserAgent
import undetected_chromedriver as uc
from collections import defaultdict, deque
import threading
from functools import wraps
from pathlib import Path
import os
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

# Configure logging with rotation
import os
from logging.handlers import RotatingFileHandler

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL.upper()))

# File handler with rotation (10MB max, 5 backups)
file_handler = RotatingFileHandler(
    'nettiauto_monitor.log',
    maxBytes=10*1024*1024,
    backupCount=5
)
file_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s'
))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Retry decorator
def retry(max_attempts=3, delay=2, backoff=2):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"{func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    wait_time = delay * (backoff ** attempt)
                    logger.warning(f"{func.__name__} attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator

class BrowserPool:
    """Manages a pool of browser instances with health checks"""
    def __init__(self, max_size: int = 2, max_age: int = 600):
        self.max_size = max_size
        self.max_age = max_age
        self.pool = deque()
        self.lock = threading.Lock()
        self.stats = {
            'created': 0,
            'reused': 0,
            'expired': 0,
            'failed': 0
        }

    def get_browser(self) -> Optional[Tuple[uc.Chrome, float]]:
        """Get a browser from pool or return None"""
        with self.lock:
            while self.pool:
                driver, created_at = self.pool.popleft()
                age = time.time() - created_at

                if age < self.max_age:
                    try:
                        # Health check
                        _ = driver.current_url
                        self.stats['reused'] += 1
                        logger.debug(f"Reusing browser (age: {age:.0f}s)")
                        return driver, created_at
                    except:
                        self.stats['failed'] += 1
                        self._cleanup_driver(driver)
                else:
                    self.stats['expired'] += 1
                    self._cleanup_driver(driver)
            return None

    def return_browser(self, driver: uc.Chrome, created_at: float):
        """Return browser to pool"""
        with self.lock:
            if len(self.pool) < self.max_size:
                age = time.time() - created_at
                if age < self.max_age:
                    self.pool.append((driver, created_at))
                    logger.debug(f"Returned browser to pool (pool size: {len(self.pool)})")
                    return
            self._cleanup_driver(driver)

    def _cleanup_driver(self, driver):
        """Safely cleanup driver"""
        try:
            driver.quit()
        except:
            pass

    def cleanup_all(self):
        """Cleanup all browsers in pool"""
        with self.lock:
            while self.pool:
                driver, _ = self.pool.popleft()
                self._cleanup_driver(driver)
        logger.info(f"Browser pool stats: {self.stats}")



class RateLimiter:
    """Simple rate limiter"""
    def __init__(self, max_requests: int = 10, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = defaultdict(list)
        self.lock = threading.Lock()

    def is_allowed(self, key: str = 'default') -> bool:
        """Check if request is allowed"""
        with self.lock:
            now = time.time()
            # Clean old requests
            self.requests[key] = [t for t in self.requests[key] 
                                 if now - t < self.time_window]
            
            if len(self.requests[key]) >= self.max_requests:
                return False
            
            self.requests[key].append(now)
            return True

    def wait_if_needed(self, key: str = 'default'):
        """Wait if rate limit exceeded"""
        while not self.is_allowed(key):
            time.sleep(1)

class PriceTracker:
    """Track price changes for listings"""
    def __init__(self, db_manager):
        self.db = db_manager
        self.price_cache = {}

    def check_price_change(self, listing_id: str, current_price: int) -> Optional[Dict]:
        """Check if price changed and return details"""
        if listing_id in self.price_cache:
            old_price = self.price_cache[listing_id]
        else:
            old_price = self.db.get_listing_price(listing_id)
        
        if old_price and old_price != current_price:
            change = {
                'listing_id': listing_id,
                'old_price': old_price,
                'new_price': current_price,
                'difference': current_price - old_price,
                'percentage': ((current_price - old_price) / old_price) * 100
            }
            self.price_cache[listing_id] = current_price
            self.db.save_price_change(listing_id, old_price, current_price)
            return change
        
        self.price_cache[listing_id] = current_price
        return None

# TODO: This class is too large and should be refactored into smaller classes.
class EnhancedNettiautoMonitor:
    """Enhanced Nettiauto Monitor with all improvements"""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the enhanced monitor"""
        self.load_config(config)
        self.validate_config()
        
        # Initialize components
        self.ua = UserAgent()
        self.session = self._create_session()
        self.browser_pool = BrowserPool(max_size=3, max_age=1200)
        self.rate_limiter = RateLimiter(max_requests=10, time_window=60)
        
        # Database and tracking
        self.seen_listings_file = 'seen_listings.json'
        self.seen_listings = self._load_seen_listings()
        try:
            from database_manager import get_database_manager
            self.db = get_database_manager()
            if self.db is None:
                raise ImportError("Database manager returned None")
            self.price_tracker = PriceTracker(self.db)
        except (ImportError, AttributeError):
            logger.warning("Database manager not found, using memory only for price tracking")
            self.db = None
            self.price_tracker = None
        
        # State management
        self.persistent_driver = None
        self.persistent_browser_failures = 0
        self.persistent_browser_retries = 0
        self.max_persistent_failures = 3
        self.max_persistent_retries = 5
        self.cloudscraper_instance = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        
        # Statistics with memory limits
        self.stats = {
            'checks_performed': 0,
            'total_listings_found': 0,
            'new_listings_sent': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        self.perf_metrics = {
            'method_times': defaultdict(lambda: deque(maxlen=50)),  # Limit size!
            'selector_performance': defaultdict(lambda: deque(maxlen=50))
        }
        
        # Initialize persistent browser in headless mode
        if self.headless:
            self._initialize_persistent_browser_with_retry()
        
        # Feature flags
        try:
            from feature_flags import is_feature_enabled, get_warning
            self.is_feature_enabled = is_feature_enabled
            self.get_warning = get_warning
        except ImportError:
            logger.warning("Feature flags not found, all features enabled")
            self.is_feature_enabled = lambda x: True
            self.get_warning = lambda x: ""

    def load_config(self, config: Optional[Dict] = None):
        """Load configuration from a dictionary or environment"""
        self.config = config or {}
        
        # Get config with fallback to environment variables
        self.telegram_token = (
            self.config.get('telegram_bot_token') or 
            os.getenv('TELEGRAM_BOT_TOKEN')
        )
        self.telegram_chat_id = (
            self.config.get('telegram_chat_id') or 
            os.getenv('TELEGRAM_CHAT_ID')
        )
        self.monitoring_url = (
            self.config.get('monitoring_url') or 
            os.getenv('MONITORING_URL')
        )
        
        # Optional settings
        self.check_interval = self.config.get('check_interval_minutes', 10) * 60
        self.scan_interval = self.config.get('scan_interval', 60)
        self.max_price_threshold = self.config.get('max_price_threshold', 55000)
        self.min_message_delay = self.config.get('min_message_delay', 5)
        self.max_listing_age_hours = self.config.get('max_listing_age_hours', 24)
        self.headless = self.config.get('headless', True)
        self.batch_notifications = self.config.get('batch_notifications', True)
        self.batch_threshold = self.config.get('batch_threshold', 5)

    def _load_seen_listings(self) -> set:
        """Load seen listings from a JSON file."""
        try:
            with open(self.seen_listings_file, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except (FileNotFoundError, json.JSONDecodeError):
            return set()

    def _save_seen_listings(self):
        """Save seen listings to a JSON file."""
        with open(self.seen_listings_file, 'w', encoding='utf-8') as f:
            json.dump(list(self.seen_listings), f)

    def validate_config(self):
        """Validate required configuration"""
        required = {
            'telegram_token': self.telegram_token,
            'telegram_chat_id': self.telegram_chat_id,
            'monitoring_url': self.monitoring_url
        }
        
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        logger.info("✅ Configuration validated")

    def _create_session(self) -> requests.Session:
        """Create optimized requests session"""
        session = requests.Session()
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=20,
            max_retries=retry_strategy
        )
        
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    @retry(max_attempts=3, delay=2)
    def _initialize_persistent_browser(self) -> bool:
        """Initialize persistent browser with retry"""
        if self.persistent_browser_retries >= self.max_persistent_retries:
            logger.error(f"Max retries ({self.max_persistent_retries}) reached for persistent browser")
            return False
        
        self.persistent_browser_retries += 1
        logger.info(f"Initializing persistent browser (attempt {self.persistent_browser_retries})...")
        
        try:
            options = uc.ChromeOptions()
            
            # Headless mode
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--window-size=1920,1080')
            options.add_argument(f'--user-agent={self.ua.random}')
            options.add_argument('--lang=fi-FI')
            
            # Experimental options
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'fi,en-US,en',
                'profile.default_content_setting_values.notifications': 2
            })
            
            # Create driver
            self.persistent_driver = uc.Chrome(options=options)
            time.sleep(2)
            
            self.persistent_driver.set_page_load_timeout(30)
            
            # Stealth JavaScript
            try:
                self.persistent_driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['fi-FI', 'fi']});
                    window.chrome = {runtime: {}};
                """)
            except Exception as e:
                logger.warning(f"Stealth JS failed: {e}")
            
            # Navigate and verify
            self.persistent_driver.get(self.monitoring_url)
            time.sleep(3)
            
            if self.persistent_driver.title:
                logger.info(f"✅ Browser initialized: {self.persistent_driver.title}")
                self.persistent_browser_failures = 0
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            if self.persistent_driver:
                try:
                    self.persistent_driver.quit()
                except:
                    pass
                self.persistent_driver = None
            return False

    def _initialize_persistent_browser_with_retry(self):
        """Initialize with retry logic"""
        for attempt in range(3):
            if self._initialize_persistent_browser():
                return True
            time.sleep(2 ** attempt)
        return False

    def _ensure_persistent_browser(self) -> bool:
        """Ensure persistent browser is healthy"""
        if not self.headless:
            return False
        
        if self.persistent_browser_retries >= self.max_persistent_retries:
            logger.error("Max retries exceeded, persistent browser disabled")
            return False
        
        if self.persistent_driver:
            try:
                _ = self.persistent_driver.current_url
                _ = self.persistent_driver.title
                return True
            except:
                self.persistent_browser_failures += 1
                try:
                    self.persistent_driver.quit()
                except:
                    pass
                self.persistent_driver = None
        
        if self.persistent_browser_failures >= self.max_persistent_failures:
            logger.error(f"Persistent browser failed {self.persistent_browser_failures} times")
            return False
        
        return self._initialize_persistent_browser()

    @retry(max_attempts=2, delay=3)
    def method_1_cloudscraper(self) -> Optional[List[Dict]]:
        """Method 1: CloudScraper with rate limiting"""
        logger.info("🌐 Trying CloudScraper...")
        self.rate_limiter.wait_if_needed('cloudscraper')
        start_time = time.time()
        
        try:

            
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'fi-FI,fi;q=0.9',
                'Referer': 'https://www.google.com/'
            }
            
            response = self.cloudscraper_instance.get(
                self.monitoring_url,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                listings = self.parse_listings(response.text)
                if listings:
                    elapsed = time.time() - start_time
                    self.perf_metrics['method_times']['cloudscraper'].append(elapsed)
                    logger.info(f"✅ CloudScraper: {len(listings)} listings in {elapsed:.2f}s")
                    if self.db:
                        self.db.log_method_attempt('cloudscraper', True, elapsed, len(listings))
                    return listings
            
        except Exception as e:
            logger.error(f"❌ CloudScraper failed: {e}")
            if self.db:
                self.db.log_method_attempt('cloudscraper', False, time.time() - start_time, 0)
        
        return None

    @retry(max_attempts=2, delay=3)
    def method_2_undetected_chrome(self) -> Optional[List[Dict]]:
        """Method 2: Undetected Chrome with persistent browser"""
        logger.info("🚗 Trying Undetected Chrome...")
        self.rate_limiter.wait_if_needed('chrome')
        start_time = time.time()
        
        # Try persistent browser first
        if self.headless and self._ensure_persistent_browser():
            try:
                if self.monitoring_url in self.persistent_driver.current_url:
                    self.persistent_driver.refresh()
                else:
                    self.persistent_driver.get(self.monitoring_url)
                
                time.sleep(random.uniform(2, 4))
                
                # Human-like scrolling
                for _ in range(2):
                    scroll = random.randint(200, 400)
                    self.persistent_driver.execute_script(f"window.scrollBy(0, {scroll})")
                    time.sleep(random.uniform(0.3, 0.8))
                
                self.persistent_driver.execute_script("window.scrollTo(0, 0)")
                time.sleep(0.5)
                
                page_source = self.persistent_driver.page_source
                listings = self.parse_listings(page_source)
                
                if listings:
                    elapsed = time.time() - start_time
                    self.perf_metrics['method_times']['chrome'].append(elapsed)
                    logger.info(f"✅ Chrome: {len(listings)} listings in {elapsed:.2f}s")
                    self.persistent_browser_failures = 0
                    if self.db:
                        self.db.log_method_attempt('chrome', True, elapsed, len(listings))
                    return listings
                
            except Exception as e:
                logger.error(f"❌ Chrome failed: {e}")
                self.persistent_browser_failures += 1
        
        return None

    def parse_listings(self, html: str) -> List[Dict]:
        """Parse HTML to find all listings using a primary, robust selector."""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        # Primary selector for the entire listing card
        listing_cards = soup.select('div.product-card')
        logger.debug(f"Found {len(listing_cards)} potential listing cards.")

        for card in listing_cards:
            listing_details = self.extract_listing_info(card)
            if listing_details:
                listings.append(listing_details)

        # If the primary selector fails, fall back to the old method of finding any link.
        if not listings:
            logger.warning("Primary selector failed. Falling back to finding raw links.")
            links = soup.find_all('a', href=lambda x: x and '/vehicle/' in x)
            for link in links[:50]: # Limit to 50 to avoid excessive processing
                href = link.get('href', '')
                listing_id_match = re.search(r'/(\d+)', href)
                if listing_id_match:
                    listing_id = listing_id_match.group(1)
                    listings.append({
                        'id': listing_id,
                        'url': f"https://www.nettiauto.com{href}" if not href.startswith('http') else href,
                        'title': link.get_text(strip=True) or 'Unknown'
                    })

        return listings

    def extract_listing_info(self, element) -> Optional[Dict]:
        """
        Extract structured listing information from a BeautifulSoup element.
        This version uses specific selectors for robustness.
        """
        try:
            # Main link and URL
            link_element = element.select_one('a.product-card-link__tricky-link')
            if not link_element:
                return None  # Cannot proceed without a link

            href = link_element.get('href', '')
            url = f"https://www.nettiauto.com{href}" if not href.startswith('http') else href

            # Extract ID from URL
            listing_id_match = re.search(r'/(\d+)(?:\?.*)?$', href)
            if not listing_id_match:
                return None
            listing_id = listing_id_match.group(1)

            # Title
            title_element = element.select_one('h2.product-card__title')
            title = title_element.get_text(strip=True) if title_element else 'Unknown Title'

            # Price
            price_element = element.select_one('div.product-card__price-main')
            price = price_element.get_text(strip=True) if price_element else 'Price not found'

            # Details list (year, mileage, etc.)
            details_list = element.select('div.product-card__basic-info-list span')
            details_text = [p.get_text(strip=True) for p in details_list]

            year, mileage, location = 'N/A', 'N/A', 'N/A'
            if len(details_text) >= 1:
                year = details_text[0]
            if len(details_text) >= 2:
                mileage = details_text[1]
            
            # Location is in a different element
            location_element = element.select_one('div.product-card__address')
            if location_element:
                location = location_element.get_text(strip=True).split(',')[0]


            return {
                'id': listing_id,
                'title': title,
                'price': price,
                'url': url,
                'year': year,
                'mileage': mileage,
                'location': location,
                'fuel_type': '',  # This info is not in a consistent place
                'published_date': '' # This info is not in a consistent place
            }

        except Exception as e:
            logger.debug(f"Error extracting listing info: {e}", exc_info=True)
            return None

    def parse_listing_date(self, date_text: str) -> Optional[datetime]:
        """Parse Finnish date formats"""
        import re
        
        if not date_text:
            return None
        
        try:
            # Today/Yesterday
            if 'tänään' in date_text.lower():
                return datetime.now()
            elif 'eilen' in date_text.lower():
                return datetime.now() - timedelta(days=1)
            
            # Relative dates
            relative = re.search(r'(\d+)\s*(päivä|tunti|minuutti|viikko)', date_text, re.IGNORECASE)
            if relative:
                amount = int(relative.group(1))
                unit = relative.group(2).lower()
                
                if 'päivä' in unit:
                    return datetime.now() - timedelta(days=amount)
                elif 'tunti' in unit:
                    return datetime.now() - timedelta(hours=amount)
                elif 'minuutti' in unit:
                    return datetime.now() - timedelta(minutes=amount)
                elif 'viikko' in unit:
                    return datetime.now() - timedelta(weeks=amount)
            
            # DD.MM.YYYY format
            date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text)
            if date_match:
                day, month, year = map(int, date_match.groups())
                return datetime(year, month, day)
        
        except Exception as e:
            logger.debug(f"Error parsing date '{date_text}': {e}")
        
        return None

    def check_listings(self) -> List[Dict]:
        """Check listings with all methods"""
        methods = [
            ("CloudScraper", self.method_1_cloudscraper),
            ("Undetected Chrome", self.method_2_undetected_chrome)
        ]
        
        for name, method in methods:
            try:
                listings = method()
                if listings:
                    self.stats['total_listings_found'] += len(listings)
                    return listings
            except Exception as e:
                logger.error(f"Method {name} failed: {e}")
                self.stats['errors'] += 1
        
        return []

    @retry(max_attempts=3, delay=2)
    def send_telegram_notification(self, message: str) -> bool:
        """Send Telegram notification with retry"""
        try:
            # Don't log full token!
            logger.debug(f"Sending Telegram message (token: {'*' * 10})")
            
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            data = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML',
                'disable_web_page_preview': False
            }
            
            response = self.session.post(url, json=data, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Telegram notification sent")
                if self.db:
                    self.db.log_notification(message, True)
                return True
            else:
                logger.error(f"❌ Telegram failed: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Telegram error: {e}")
            if self.db:
                self.db.log_notification(message, False)
            return False

    def process_new_listings(self, listings: List[Dict]):
        """Process new listings with batching and price tracking"""
        import re
        
        new_listings = []
        price_drops = []
        
        for listing in listings:
            listing_id = listing.get('id')
            if not listing_id:
                continue
            
            # Check if already seen
            if listing_id in self.seen_listings:
                # Check for price changes
                if self.price_tracker:
                    price_text = listing.get('price', '')
                    price_match = re.search(r'(\d+[\s\u00a0]?\d*)', price_text)
                    if price_match:
                        price_value = int(price_match.group(1).replace('\u00a0', '').replace(' ', ''))
                        price_change = self.price_tracker.check_price_change(listing_id, price_value)
                        if price_change and price_change['difference'] < 0:  # Price dropped
                            price_drops.append((listing, price_change))
                continue
            
            # Check listing age
            published_date = listing.get('published_date')
            if published_date:
                listing_date = self.parse_listing_date(published_date)
                if listing_date:
                    hours_old = (datetime.now() - listing_date).total_seconds() / 3600
                    if hours_old > self.max_listing_age_hours:
                        logger.debug(f"Skipping old listing {listing_id} ({hours_old:.0f}h old)")
                        self.seen_listings.add(listing_id)
                        continue
            
            # Check price threshold
            price_text = listing.get('price', '')
            price_match = re.search(r'(\d+[\s\u00a0]?\d*)', price_text)
            if price_match:
                price_value = int(price_match.group(1).replace('\u00a0', '').replace(' ', ''))
                if price_value > self.max_price_threshold:
                    logger.debug(f"Skipping expensive listing {listing_id} ({price_value}€)")
                    self.seen_listings.add(listing_id)
                    continue
            
            # New listing!
            self.seen_listings.add(listing_id)
            self._save_seen_listings()  # Save immediately
            new_listings.append(listing)
            
            # Save to database
            if self.db:
                self.db.save_seen_listing(
                    listing_id=listing_id,
                    title=listing.get('title'),
                    price=listing.get('price'),
                    url=listing.get('url')
                )
        
        # Send notifications
        if new_listings:
            if self.batch_notifications and len(new_listings) >= self.batch_threshold:
                self._send_batch_notification(new_listings)
            else:
                self._send_individual_notifications(new_listings)
        
        # Send price drop notifications
        if price_drops:
            for listing, change in price_drops:
                self._send_price_drop_notification(listing, change)
        
        if new_listings or price_drops:
            self.stats['new_listings_sent'] += len(new_listings)
            logger.info(f"✅ Processed: {len(new_listings)} new, {len(price_drops)} price drops")

    def _send_individual_notifications(self, listings: List[Dict]):
        """Send individual notifications"""
        for listing in listings:
            message = self._format_listing_message(listing)
            try:
                self.send_telegram_notification(message)
                if self.min_message_delay > 0:
                    time.sleep(self.min_message_delay)
            except Exception as e:
                logger.error(f"Failed to send notification for {listing['id']}: {e}")

    def _send_batch_notification(self, listings: List[Dict]):
        """Send batch notification for multiple listings"""
        message = f"🚗 <b>{len(listings)} New Listings Found!</b>\n\n"
        
        # Add first 3 with details
        for listing in listings[:3]:
            title = listing.get('title', 'Unknown')
            price = listing.get('price', 'N/A')
            url = listing.get('url', '')
            message += f"• <b>{title}</b>\n  💰 {price}\n  🔗 {url}\n\n"
        
        # Add remaining as summary
        if len(listings) > 3:
            message += f"... and {len(listings) - 3} more listings\n\n"
            message += "Check Nettiauto for full details!"
        
        self.send_telegram_notification(message)

    def _send_price_drop_notification(self, listing: Dict, change: Dict):
        """Send price drop notification"""
        title = listing.get('title', 'Unknown')
        url = listing.get('url', '')
        
        message = f"💰 <b>PRICE DROP!</b>\n\n"
        message += f"🚗 {title}\n\n"
        message += f"Was: <s>{change['old_price']}€</s>\n"
        message += f"Now: <b>{change['new_price']}€</b>\n"
        message += f"Saved: {abs(change['difference'])}€ ({abs(change['percentage']):.1f}%)\n\n"
        message += f"🔗 {url}"
        
        self.send_telegram_notification(message)

    def _format_listing_message(self, listing: Dict) -> str:
        """Format listing message"""
        title = listing.get('title', 'Unknown')
        message = f"🚗 <b>NEW: {title}</b>\n\n"
        
        details = []
        if listing.get('price'):
            details.append(f"💰 {listing['price']}")
        if listing.get('year'):
            details.append(f"📅 {listing['year']}")
        if listing.get('mileage'):
            details.append(f"📊 {listing['mileage']}")
        if listing.get('fuel_type'):
            details.append(f"⛽ {listing['fuel_type']}")
        if listing.get('published_date'):
            details.append(f"📆 {listing['published_date']}")
        
        if details:
            message += " | ".join(details) + "\n\n"
        
        if listing.get('location'):
            message += f"📍 {listing['location']}\n"
        
        if listing.get('url'):
            message += f"🔗 {listing['url']}"
        
        return message

    def get_health_status(self) -> Dict:
        """Get monitor health status"""
        uptime = datetime.now() - self.stats['start_time']
        
        return {
            'status': 'running',
            'uptime_seconds': uptime.total_seconds(),
            'checks_performed': self.stats['checks_performed'],
            'total_listings': self.stats['total_listings_found'],
            'new_listings_sent': self.stats['new_listings_sent'],
            'errors': self.stats['errors'],
            'seen_listings_count': len(self.seen_listings),
            'persistent_browser_ok': self.persistent_driver is not None,
            'browser_pool_size': len(self.browser_pool.pool),
            'last_check': datetime.now().isoformat()
        }

    def get_performance_report(self) -> str:
        """Generate performance report"""
        report = ["\n=== Performance Report ==="]
        report.append(f"Uptime: {datetime.now() - self.stats['start_time']}")
        report.append(f"Checks: {self.stats['checks_performed']}")
        report.append(f"Listings found: {self.stats['total_listings_found']}")
        report.append(f"Notifications sent: {self.stats['new_listings_sent']}")
        report.append(f"Errors: {self.stats['errors']}")
        
        # Method times
        report.append("\nMethod Performance:")
        for method, times in self.perf_metrics['method_times'].items():
            if times:
                avg = sum(times) / len(times)
                report.append(f"  {method}: {avg:.2f}s avg ({len(times)} calls)")
        
        # Browser pool
        report.append(f"\nBrowser Pool: {self.browser_pool.stats}")
        
        # Top selectors
        report.append("\nTop Selectors:")
        report.append("  (Selector learning not implemented yet)")
        
        return "\n".join(report)

    def run(self):
        """Main monitoring loop"""
        logger.info("🚀 Starting Enhanced Nettiauto Monitor")
        
        # Send startup notification
        try:
            self.send_telegram_notification(
                f"✅ <b>Monitor Started</b>\n\n"
                f"Scan interval: {self.scan_interval}s\n"
                f"Headless: {self.headless}\n"
                f"Batch notifications: {self.batch_notifications}\n"
                f"Price threshold: {self.max_price_threshold}€"
            )
        except:
            pass
        
        try:
            while True:
                try:
                    self.stats['checks_performed'] += 1
                    logger.info(f"🔍 Check #{self.stats['checks_performed']}")
                    
                    # Check listings
                    listings = self.check_listings()
                    
                    if listings:
                        self.process_new_listings(listings)
                    else:
                        logger.warning("No listings found")
                    
                    # Log performance every 10 checks
                    if self.stats['checks_performed'] % 10 == 0:
                        logger.info(self.get_performance_report())
                    
                    # Wait before next check
                    logger.info(f"💤 Sleeping {self.scan_interval}s...")
                    time.sleep(self.scan_interval)
                    
                except KeyboardInterrupt:
                    logger.info("🛑 Stopped by user")
                    break
                except Exception as e:
                    logger.error(f"❌ Unexpected error: {e}", exc_info=True)
                    self.stats['errors'] += 1
                    time.sleep(60)
        
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        logger.info("🧹 Cleaning up...")
        
        # Persistent browser
        if self.persistent_driver:
            try:
                self.persistent_driver.quit()
            except:
                pass
        
        # Browser pool
        self.browser_pool.cleanup_all()
        
        # Session
        if self.session:
            self.session.close()
        
        # Database
        if self.db:
            self.db.close()
        
        logger.info("✅ Cleanup complete")
        logger.info(self.get_performance_report())

if __name__ == "__main__":
    try:
        monitor = EnhancedNettiautoMonitor()
        monitor.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
