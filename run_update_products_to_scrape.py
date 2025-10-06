import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from update_products_to_scrape import UpdateProductsToScrape
from settings import kickz
from libs.logger import Logger

# At 05:00 on Monday
"""
CRON: 0 5 * * 1 cd /home/ec2-user/autopricing-kickz/ && /home/ec2-user/anaconda3/bin/python run_update_products_to_scrape.py
"""

# SENTRY settings
sentry_logging = LoggingIntegration(
    level = logging.INFO,        
    event_level = logging.ERROR 
)
sentry_sdk.init(
    dsn = kickz.sentry_dsn,
    integrations = [sentry_logging]
)

# logging
logger = Logger().get_full_logger(
    filename = './logs/update_products_to_scrape.log',
    log_level = logging.INFO,
    print_level = logging.INFO)

def run():
    try:
        updater = UpdateProductsToScrape(settings = kickz)
        updater.run()
        
    except Exception as e:
        logger.exception('Exception occured')

if __name__ == '__main__':
    run()
