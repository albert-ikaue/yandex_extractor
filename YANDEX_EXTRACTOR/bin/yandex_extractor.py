# -*- coding: utf-8 -*-
# (C) 2018 IKAUE, Marketing de Optimizacion
# created by Albert Moral Lleó <albert@ikaue.com>

"""
This module generates uploads to BQ a table with the data extracted from yandex webmaster API.
"""
import time

import requests
import json
import sys
import os
import logging
import pandas as pd
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

    return gsc_date_range,script_file

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
    action = f"/user/1125390301/hosts/https:www.zara.com:443/search-queries/all/history?query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS&date_from={date}&date_to={date}"

    retry_count = 0
    retry_max = 1

    try:
        resp = requests.get(API_URL + action, headers=headers)
    except Exception as message:
        if "400" or "401" in message:
            logging.error(f"Could not retrieve html, authentication or token error: {message}")
            sys.exit(1)
        elif retry_count < retry_max:
            print(f"Retrying ... (count {retry_count})")
            # sleep for fifteen minutes
            time.sleep(10)

            # increase the counter
            retry_count = retry_count + 1

        else:
            logging.error(f"Could not retrieve response: {message}")
            raise Exception(str(message))

    return resp.json()

def upload_bq(bq_project, bq_dataset, table_name,gsc_schemas,bq_tmp_file,cl,bq_dataset_location,bq_check,bq_alert_empty,
             bq_alert_callback,script_file):
    """
    This function uploads the CSV resultant to a BQ table.
    :param bq_project: BQ project
    :param bq_dataset: BQ dataset
    :param table_name: Table name
    :param gsc_schemas: BQ table Schema
    :param bq_tmp_file: BQ tmp file
    :param cl: BQ Client
    :param bq_dataset_location: BQ Dataset Location
    :param bq_check: Checks
    :param bq_alert_empty: Alerts
    :param bq_alert_callback: Alerts
    :param script_file: script file name
    :return: nothing
    """


    # create the configuration for an upload job
    final_table_name = u"%s.%s.%s" % (bq_project, bq_dataset, table_name)
    jc = bigquery.LoadJobConfig()
    jc.schema = gsc_schemas
    jc.source_format = bigquery.SourceFormat.CSV
    jc.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # create a job to upload the rows
    with open(bq_tmp_file, "rb") as f:

        jb = cl.load_table_from_file(f, final_table_name, location=bq_dataset_location, job_config=jc)

        try:
            # upload the rows
            rs = jb.result()

            # check if the table was created successfully
            if bq_check == True:
                if not cl.get_table(final_table_name):
                    if bq_alert_empty == True:
                        bq_alert_callback(script_file, u"[bq] table '%s' was not created" % final_table_name)
        except Exception as e:
            logging.error(f"Could not upload the table to BQ: {e}")

            print(u"ERROR: %s" % table_name)

            if jb.errors:
                for i in jb.errors:
                    print(u"ERROR: %s" % i["message"])
            else:
                print(e)

        f.close()

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
    #set_logs("../core/logs")

    bq_check=False
    bq_alert_empty=False
    bq_alert_callback = lambda x, y: requests.post("https://hook.integromat.com/kahmpduow7ftbnqbv1eeermaim9r8kos",
                                                   data={
                                                       "origin": "VM-GCE-Analytics.pem",
                                                       "tool": "ikp-extract",
                                                       "script": x,
                                                       "error": y
                                                   })

    gsc_schemas = [bigquery.SchemaField('date', 'STRING', 'NULLABLE', None, ()),
                   bigquery.SchemaField('clicks', 'FLOAT', 'NULLABLE', None, ()),
                   bigquery.SchemaField('impressions', 'FLOAT', 'NULLABLE', None, ()),
                    ]


    json_key_file = "ikaue-bb8.json"
    bq_tmp_file='../core/df.csv'

    bq_project='ikaue-bb8'
    bq_dataset='testing'
    bq_dataset_location='EU'

    # build the BigQuery service object
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =json_key_file
    cl = bigquery.Client()

    gsc_date_range,script_file = date_range()
    # traverse the date range

    dfObj = pd.DataFrame(columns=["date", "clicks", "impressions"])

    for date in gsc_date_range:

        table_name = f'zara_yandex_{date}'
        json_data = GET_request(date)

        for shows in json_data['indicators']['TOTAL_SHOWS']:
            impressions = shows['value']

        for clicks in json_data['indicators']['TOTAL_CLICKS']:
            clicks = clicks['value']

        dfObj = dfObj.append({'date': date, 'clicks': clicks, 'impressions': impressions}, ignore_index=True)


    print(u">> %s rows to process" % (len(dfObj) if "dfObj" in locals() else 0))
    dfObj.to_csv(bq_tmp_file, index=False)

    upload_bq(bq_project, bq_dataset, table_name,gsc_schemas,bq_tmp_file,cl,bq_dataset_location,bq_check,bq_alert_empty,
             bq_alert_callback,script_file)



if __name__ == "__main__":
    main()