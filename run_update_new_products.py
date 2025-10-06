import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from update_new_products import NewProductsAdder
from settings import kickz
from libs.logger import Logger

# kazdy den 21:00
"""
CRON: 0 22 * * * cd /home/ec2-user/autopricing-kickz/ && /home/ec2-user/anaconda3/bin/python run_update_new_products.py
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
    filename = './logs/update_new_products.log',
    log_level = logging.INFO,
    print_level = logging.INFO
)

def run():
    try:
        updater = NewProductsAdder(settings=kickz)
        updater.add_new_products(store_in_s3=True)
        
    except Exception as e:
        logger.exception('Exception occured')

if __name__ == '__main__':
    run()