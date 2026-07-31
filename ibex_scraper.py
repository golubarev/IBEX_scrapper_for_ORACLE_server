import requests
import json
import psycopg2
from datetime import datetime, timedelta
import pytz
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_NAME = os.getenv('DB_NAME', 'ibex_market')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

IBEX_API_URL = 'https://ibex.bg/wp-json/v1/page/74562'
EET = pytz.timezone('Europe/Sofia')

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IBEXScraper:
    def __init__(self):
        self.conn = None
        self.prices_found_today = False
        self.last_check_date = None

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            logger.info("✓ Database connection successful")
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            raise

    def log_scrape(self, status, message):
        """Log scraping activity to database"""
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO scrape_log (status, message) VALUES (%s, %s)",
                (status, message)
            )
            self.conn.commit()
            cur.close()
        except Exception as e:
            logger.error(f"Failed to log scrape: {e}")

    def scrape_prices(self):
        """Scrape prices from ibex.bg API"""
        try:
            logger.info("📡 Fetching prices from ibex.bg API...")

            # Get today's date for the API call
            today = datetime.now(EET).date()

            # API endpoint with date parameter
            api_url = f'https://ibex.bg/wp-json/v1/page/74562?date={today}'

            response = requests.get(api_url, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Check if we have the required data
            if not data.get('main_data') or not data.get('price_data') or not data.get('summary'):
                logger.warning("Incomplete data received from API")
                return None

            # Parse the response
            prices_data = {
                'date': data.get('date'),
                'prices_15min': [],
                'prices_hourly': [],
                'daily_volumes': {}
            }

            # Parse 15-minute prices from main_data
            for item in data['main_data']:
                try:
                    price = float(item['price'])
                    prices_data['prices_15min'].append(price)
                except (ValueError, KeyError) as e:
                    logger.error(f"Error parsing 15-min price: {e}")
                    return None

            # Parse hourly prices from price_data
            for item in data['price_data']:
                try:
                    price = float(item['price_index'])
                    prices_data['prices_hourly'].append(price)
                except (ValueError, KeyError) as e:
                    logger.error(f"Error parsing hourly price: {e}")
                    return None

            # Parse daily volumes from summary
            try:
                summary = data['summary']
                prices_data['daily_volumes'] = {
                    'base': float(summary['base']),
                    'peak': float(summary['peak']),
                    'off_peak': float(summary['off_peak']),
                    'total_volume': float(summary['total_volume'].replace(',', ''))
                }
            except (ValueError, KeyError) as e:
                logger.error(f"Error parsing daily volumes: {e}")
                return None

            logger.info(
                f"✓ Successfully parsed {len(prices_data['prices_15min'])} 15-min prices and {len(prices_data['prices_hourly'])} hourly prices")
            return prices_data

        except requests.exceptions.RequestException as e:
            logger.error(f"✗ API request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"✗ Failed to parse JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"✗ Unexpected error during scraping: {e}")
            self.log_scrape('error', f'Scraping failed: {str(e)}')
            return None

    def insert_prices(self, prices_data):
        """Insert scraped prices into database"""
        try:
            if not prices_data:
                return False

            cur = self.conn.cursor()
            date_obj = datetime.strptime(prices_data['date'], '%Y-%m-%d').date()

            # Insert 15-minute prices (96 periods per day)
            logger.info(f"Inserting {len(prices_data['prices_15min'])} 15-minute prices...")
            for period, price in enumerate(prices_data['prices_15min'], 1):
                cur.execute(
                    "INSERT INTO prices_15min (date, period, price) VALUES (%s, %s, %s) "
                    "ON CONFLICT (date, period) DO UPDATE SET price = EXCLUDED.price",
                    (date_obj, period, price)
                )

            # Insert hourly prices (24 hours per day)
            logger.info(f"Inserting {len(prices_data['prices_hourly'])} hourly prices...")
            for hour, price in enumerate(prices_data['prices_hourly']):
                cur.execute(
                    "INSERT INTO prices_hourly (date, hour, price) VALUES (%s, %s, %s) "
                    "ON CONFLICT (date, hour) DO UPDATE SET price = EXCLUDED.price",
                    (date_obj, hour, price)
                )

            # Insert daily volumes
            if prices_data['daily_volumes']:
                logger.info("Inserting daily volume summary...")
                vol = prices_data['daily_volumes']
                cur.execute(
                    "INSERT INTO daily_volumes (date, base_volume, peak_volume, off_peak_volume, total_volume) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT (date) DO UPDATE SET "
                    "base_volume = EXCLUDED.base_volume, "
                    "peak_volume = EXCLUDED.peak_volume, "
                    "off_peak_volume = EXCLUDED.off_peak_volume, "
                    "total_volume = EXCLUDED.total_volume",
                    (date_obj, vol['base'], vol['peak'], vol['off_peak'], vol['total_volume'])
                )

            self.conn.commit()
            cur.close()
            logger.info(f"✓ All prices inserted successfully for {date_obj}")
            self.log_scrape('success', f'Prices found and inserted for {date_obj}')
            return True

        except psycopg2.Error as e:
            logger.error(f"✗ Database error during insert: {e}")
            if self.conn:
                self.conn.rollback()
            self.log_scrape('error', f'Insert failed: {str(e)}')
            return False
        except Exception as e:
            logger.error(f"✗ Unexpected error during insert: {e}")
            self.log_scrape('error', f'Insert failed: {str(e)}')
            return False

    def check_and_scrape(self):
        """Check if it's time to scrape and attempt to fetch prices"""
        now = datetime.now(EET)
        current_date = now.date()
        current_hour = now.hour
        current_minute = now.minute

        # Reset daily flag at midnight
        if self.last_check_date != current_date:
            self.prices_found_today = False
            self.last_check_date = current_date
            logger.info("🔄 Daily reset - starting fresh search for today's prices")

        # Only scrape after 14:00 EET
        if current_hour >= 14 and not self.prices_found_today:
            logger.info(f"⏰ Checking for prices... ({current_hour:02d}:{current_minute:02d} EET)")
            prices_data = self.scrape_prices()

            if prices_data:
                if self.insert_prices(prices_data):
                    self.prices_found_today = True
                    logger.info("🎉 Prices found and stored! Pausing checks until tomorrow.")
            else:
                logger.info("⏳ Prices not yet available, will retry in 5 minutes...")
        elif self.prices_found_today:
            logger.debug("Prices already found today, skipping check")
        elif current_hour < 14:
            logger.debug(
                f"Too early ({current_hour:02d}:{current_minute:02d}), prices not published yet (after 14:00 EET)")

    def start(self):
        """Start the scraper with APScheduler"""
        try:
            self.connect_db()

            scheduler = BackgroundScheduler()
            # Run every 5 minutes
            scheduler.add_job(self.check_and_scrape, 'interval', minutes=5, id='ibex_scraper')

            scheduler.start()
            logger.info("=" * 60)
            logger.info("🚀 IBEX Scraper STARTED")
            logger.info("=" * 60)
            logger.info(f"📍 Database: {DB_NAME} @ {DB_HOST}")
            logger.info(f"⏱️  Polling: Every 5 minutes after 14:00 EET")
            logger.info(f"🌍 Timezone: Europe/Sofia (EET)")
            logger.info("=" * 60)

            try:
                # Keep the scheduler running
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("⛔ Keyboard interrupt received")
                scheduler.shutdown()
                if self.conn:
                    self.conn.close()
                logger.info("✓ Scraper stopped gracefully")
        except Exception as e:
            logger.error(f"✗ Failed to start scraper: {e}")
            raise


if __name__ == "__main__":
    scraper = IBEXScraper()
    scraper.start()