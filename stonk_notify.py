import requests
import json
import yaml
from joblib import Parallel, delayed
import multiprocessing
import time
import yfinance as yf
import argparse
import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    format="[%(asctime)s]: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)

parser = argparse.ArgumentParser(description="Stonks Notify")
parser.add_argument(
    "--config_path",
    default="",
    type=str,
    required=True,
    help="path to the yaml config file",
)
args = parser.parse_args()

config_path = args.config_path
n_cores = multiprocessing.cpu_count()


def stonk_checker(stonk, webhook_url, update_freq):
    notification_sent = False
    while True:
        stock_data = yf.download(tickers=stonk["ticker"], period="1d", interavl="1m")
        row = stock_data.iloc[-1]
        logging.info("Stock: {}, Current Price: {}".format(stonk["ticker"], row.Open))
        contents = {}
        if row.Open >= stonk["high"]:
            contents["text"] = (
                "The value of stock `{}` crossed above your threshold value `{}` 🎉. It is currently priced at `{}`"
            ).format(stonk["ticker"], stonk["high"], row.Open)
            requests.post(webhook_url, json.dumps(contents))
            notification_sent = True
            logging.info("Slack Notification: " + contents["text"])
        elif row.Open <= stonk["low"]:
            contents["text"] = (
                "The value of stock `{}` is below your threshold value `{}` 😟. It is currently priced at `{}`"
            ).format(stonk["ticker"], stonk["low"], row.Open)
            requests.post(webhook_url, json.dumps(contents))
            notification_sent = True
            logging.info("Slack Notification: " + contents["text"])

        if notification_sent:
            time.sleep(update_freq * 60)
            notification_sent = False


def load_config(path):
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    return config


if __name__ == "__main__":
    config = load_config(config_path)
    webhook_url = config.pop("webhook_url")
    update_freq = config.pop("update_frequency")
    logging.info("Starting Process")
    try:
        Parallel(n_jobs=n_cores, backend="multiprocessing")(
            delayed(stonk_checker)(v, webhook_url, update_freq)
            for _, v in config.items()
        )
    except KeyboardInterrupt:
        logging.info(".......Exiting Process.......")