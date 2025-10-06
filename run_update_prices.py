import pandas as pd
import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from libs.logger import Logger
from update_prices import PricingLogic
from settings import kickz

# 10 minut po polnoci kazdy den okrem nedele
"""
CRON: 10 2 * * 1,2,3,4,5,6 cd /home/ec2-user/autopricing-kickz/ && /home/ec2-user/anaconda3/bin/python run_update_prices.py
"""

# SENTRY settings
sentry_logging = LoggingIntegration(
    level=logging.INFO,        
    event_level=logging.ERROR 
)
sentry_sdk.init(
    dsn=kickz.sentry_dsn,
    integrations=[sentry_logging]
)

# logging
logger = Logger().get_full_logger(
    filename='./logs/update_prices.log',
    log_level=logging.INFO,
    print_level=logging.INFO
)


def run_AP():
    try:
        updater = PricingLogic(settings=kickz)
        updater.run(insert_into_production=True, insert_into_s3=True)
        
        #debug
        df_md = pd.DataFrame(updater.methods_durations)
        df_md.to_parquet('methods_durations_last_run.parquet',index=False)
                
    except Exception as e:
        logger.exception("Exception occurred")

if __name__ == '__main__':
    run_AP()
