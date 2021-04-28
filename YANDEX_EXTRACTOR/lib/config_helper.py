from datetime import datetime, timedelta
import logging
import os,sys
import pandas as pd
from google.cloud import bigquery




def set_log_file(target_folder: str, log_file_name: str):
    """
    Helper function to set up a log file in addition to the stdout messages.
    :param target_folder: Folder where to create the log file
    :param log_file_name: name of the log file.
    """
    # Set up log file
    log_file = os.path.join(target_folder, log_file_name)
    file_handler = logging.FileHandler(log_file, mode='a')
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    return root


def set_data(option,date,offset):
    flatten_date = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")

    if option == "summary":

        action = f"/user/1125390301/hosts/https:www.zara.com:443/search-queries/all/history?query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION&date_from={date}&date_to={date}"

        gsc_schemas = [bigquery.SchemaField('avg_click_pos', 'STRING', 'NULLABLE', None, ()),
                        bigquery.SchemaField('avg_impressions_pos', 'STRING', 'NULLABLE', None, ()),
                        bigquery.SchemaField('clicks', 'STRING', 'NULLABLE', None, ()),
                        bigquery.SchemaField('date', 'STRING', 'NULLABLE', None, ()),
                        bigquery.SchemaField('impressions', 'STRING', 'NULLABLE', None, ())]

        table_name = f'ywt_zara_all_summary_{flatten_date}'
        return action,gsc_schemas,table_name


    if option == "byDevice_MOB":
        action = f"/user/1125390301/hosts/https:www.zara.com:443/search-queries/all/history?query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION&device_type_indicator=MOBILE_AND_TABLET&date_from={date}&date_to={date}"

        gsc_schemas = [bigquery.SchemaField('avg_click_pos', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('avg_impressions_pos', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('clicks', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('date', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('impressions', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('device', 'STRING', 'NULLABLE', None, ())]

        table_name = f"ywt_zara_all_byDevice_detail_{flatten_date}"
        return action,gsc_schemas,table_name

    if option == "byDevice_DESK":
        action = f"/user/1125390301/hosts/https:www.zara.com:443/search-queries/all/history?query_indicator=TOTAL_SHOWS&query_indicator=TOTAL_CLICKS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION&device_type_indicator=DESKTOP&date_from={date}&date_to={date}"

        gsc_schemas = [bigquery.SchemaField('avg_click_pos', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('avg_impressions_pos', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('clicks', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('date', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('impressions', 'STRING', 'NULLABLE', None, ()),
                     bigquery.SchemaField('device', 'STRING', 'NULLABLE', None, ())]

        table_name = f"ywt_zara_all_byDevice_detail_{flatten_date}"
        return action,gsc_schemas,table_name

    if option == "byQueries":
        action = f"/user/1125390301/hosts/https:www.zara.com:443/search-queries/popular?order_by=TOTAL_CLICKS&query_indicator=TOTAL_CLICKS&query_indicator=TOTAL_SHOWS&query_indicator=AVG_SHOW_POSITION&query_indicator=AVG_CLICK_POSITION&date_from={date}&date_to={date}&offset={offset}"

        gsc_schemas = [bigquery.SchemaField('avg_click_pos', 'STRING', 'NULLABLE', None, ()),
                      bigquery.SchemaField('avg_impressions_pos', 'STRING', 'NULLABLE', None, ()),
                      bigquery.SchemaField('clicks', 'STRING', 'NULLABLE', None, ()),
                      bigquery.SchemaField('date', 'STRING', 'NULLABLE', None, ()),
                      bigquery.SchemaField('impressions', 'STRING', 'NULLABLE', None, ()),
                      bigquery.SchemaField('queryID', 'STRING', 'NULLABLE', None, ()),
                      bigquery.SchemaField('queryText', 'STRING', 'NULLABLE', None, ())]

        table_name = f"ywt_zara_all_byQueries_detail_{flatten_date}"
        return action,gsc_schemas,table_name


def obtain_data(json_data, option, date):
    # Create empty df
    dfObj = pd.DataFrame()

    if option == "summary":
        for shows in json_data['indicators']['TOTAL_SHOWS']:
            impressions = shows['value']

        for clicks in json_data['indicators']['TOTAL_CLICKS']:
            clicks = clicks['value']

        for av_im in json_data['indicators']['AVG_SHOW_POSITION']:
            imp_av = av_im['value']

        for av_cl in json_data['indicators']['AVG_CLICK_POSITION']:
            clcl_av = av_cl['value']


        return dfObj.append({'date': date, 'avg_click_pos': clcl_av, 'avg_impressions_pos': imp_av, 'clicks': clicks,
                             'impressions': impressions}, ignore_index=True)

    elif option == "byDevice_DESK":
        for shows in json_data['indicators']['TOTAL_SHOWS']:
            impressions = shows['value']

        for clicks in json_data['indicators']['TOTAL_CLICKS']:
            clicks = clicks['value']

        for av_im in json_data['indicators']['AVG_SHOW_POSITION']:
            imp_av = av_im['value']

        for av_cl in json_data['indicators']['AVG_CLICK_POSITION']:
            clcl_av = av_cl['value']

        dev="DESKTOP"

        return dfObj.append({'date': date, 'avg_click_pos': clcl_av, 'avg_impressions_pos': imp_av, 'clicks': clicks,
                             'impressions': impressions,'device': dev}, ignore_index=True)


    elif option == "byDevice_MOB":
        for shows in json_data['indicators']['TOTAL_SHOWS']:
            impressions = shows['value']

        for clicks in json_data['indicators']['TOTAL_CLICKS']:
            clicks = clicks['value']

        for av_im in json_data['indicators']['AVG_SHOW_POSITION']:
            imp_av = av_im['value']

        for av_cl in json_data['indicators']['AVG_CLICK_POSITION']:
            clcl_av = av_cl['value']

        dev="MOBILE_AND_TABLET"

        return dfObj.append({'date': date, 'avg_click_pos': clcl_av, 'avg_impressions_pos': imp_av, 'clicks': clicks,
                             'impressions': impressions,'device': dev}, ignore_index=True)

    else:


        for query in json_data["queries"]:
            impressions = query['indicators']['TOTAL_SHOWS']

            clicks = query['indicators']['TOTAL_CLICKS']

            imp_av = query['indicators']['AVG_SHOW_POSITION']

            clcl_av = query['indicators']['AVG_CLICK_POSITION']

            query_id = query['query_id']

            query_text = query['query_text']

            dict = {'date': date, 'avg_click_pos': clcl_av, 'avg_impressions_pos': imp_av, 'clicks': clicks,
                    'impressions': impressions, 'query_id': query_id, 'query_text': query_text}
            dfObj = dfObj.append(dict, ignore_index=True)


        return dfObj