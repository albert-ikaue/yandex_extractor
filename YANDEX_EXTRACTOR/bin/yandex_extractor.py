# -*- coding: utf-8 -*-
# (C) 2018 IKAUE, Marketing de Optimizacion
# created by Albert Moral Lleó <albert@ikaue.com>

"""
This module generates uploads to BQ a table with the data extracted from yandex webmaster API.
"""




import requests
import json
import sys
import os
import logging
from apiclient.discovery import build
from collections import OrderedDict
from datetime import datetime, timedelta
from google.cloud import bigquery
from oauth2client.service_account import ServiceAccountCredentials
from dateutil.parser import parse


sys.path.insert(1, "../core/")
from config_helper import set_log_file


def date_range():
    gsc_date_start = datetime.strftime(datetime.now() - timedelta(5), "%Y-%m-%d")
    gsc_date_end = datetime.strftime(datetime.now() - timedelta(5), "%Y-%m-%d")

    # fetch the current script
    script_file = sys.argv[1] if len(sys.argv) > 1 else sys.argv[0]

    # parse command-line date inputs
    dt = list(filter(lambda x: is_date(x), sys.argv))

    if len(dt) > 0:
        gsc_date_start = dt[0]

        if len(dt) > 1:
            gsc_date_end = dt[1]

    # generate the date range
    gsc_date_range = list(map(lambda x: datetime.strftime(x, "%Y-%m-%d"),
                              [datetime.strptime(gsc_date_start, "%Y-%m-%d") + timedelta(days=x) for x in range(0, (
                                      datetime.strptime(gsc_date_end, "%Y-%m-%d") - datetime.strptime(
                                  gsc_date_start, "%Y-%m-%d")).days + 1)]))

    if len(gsc_date_range) == 0:
        raise Exception(u"[gsc] specified date range is incorrect")

    # reverse the date range
    gsc_date_range = list(reversed(gsc_date_range))

    return gsc_date_range

def GET_request(date):
    """
    This function returns a json with the clicks and impressions from yandex.webmaster API v4 from Zara site.
    :return: json including clicks and impressions
    """
    # OAuth token of the user that requests will be made on behalf of
    token = 'AQAAAABT99pbAAcUcF1YciaFek7iiwOOsNQCYzQ'

    # Login of the advertising agency client
    # Required parameter if requests are made on behalf of an advertising agency
    clientLogin = 'marketingdigital@zara.com'

    headers = {
        # OAuth token. The word Bearer must be used
        "Authorization": 'OAuth AQAAAABDFBfdAAcVB0yqdlcRyEzIu8BBs1TTLuE',
        # Login of the advertising agency client
        "Client-Login": clientLogin,
        # Language for response messages
        "Accept-Language": "en",
        # Mode for report generation
        "processingMode": "auto"
        # Format for monetary values in the report
        # "returnMoneyInMicros": "false",
        # Don't include the row with the report name and date range in the report
        # "skipReportHeader": "true",
        # Don't include the row with column names in the report
        # "skipColumnHeader": "true",
        # Don't include the row with the number of statistics rows in the report
        # "skipReportSummary": "true"
    }

    API_URL = 'https://api.webmaster.yandex.net/v4'
    # action = "/user/1408752219/hosts/http:blog.ikhuerta.com:80/search-queries/all/history?query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS"
    action = f"/user/1125390301/hosts/https:www.zara.com:443/search-queries/all/history?query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS&date_from={date}&date_to={date}"

    resp = requests.get(API_URL + action, headers=headers)
    c = resp.json()
    return c

def is_date(s):
    r = False
    d = str(s)

    try:
        parse(d)
        r = True

        if len(d) < 8 or "." in d:
            r = False
    except:
        r = False

    return r

def set_logs(case_directory):
    """
    This function set the logs for this script file.
    :param logs: log file name
    :return:
    """

    if os.path.isdir(case_directory):
        log_file_name = 'log.txt'
        logger = set_log_file(case_directory, log_file_name)
        logger.addHandler(logging.StreamHandler())
    else:
        message = 'Input case directory not existing - Aborting'
        logging.error(message)
        sys.exit(1)

def main():
    # Initialize logs
    set_logs("../core/logs")

    json_key_file = "ikaue-bb8.json"





    # build the BigQuery service object
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =json_key_file
    cl = bigquery.Client()

    gsc_date_range = date_range()
    # traverse the date range
    for date in gsc_date_range:

        json_resp = GET_request(date)

        text_shows=[]
        for shows in json_resp['indicators']['TOTAL_SHOWS']:
            text_shows.append(shows)
            print(text_shows)

        text_clicks=[]
        for clicks in json_resp['indicators']['TOTAL_CLICKS']:
            text_clicks.append(clicks)
            print(text_clicks)


if __name__ == "__main__":
    main()