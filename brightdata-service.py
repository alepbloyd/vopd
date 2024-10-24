import argparse
import requests
import json
import pandas as pd
import datetime as DT
import time
import dateutil.parser
import csv
from dotenv import load_dotenv
import os
from pprint import pprint as pp

load_dotenv()
BRIGHTDATA_RESULT_ADDRESS = "https://api.brightdata.com/datasets/v3/snapshot"
BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_TOKEN")
BRIGHTDATA_DATASET_ID = os.getenv("BRIGHTDATA_DATASET_ID")
BRIGHTDATA_TRIGGER_API_ADDRESS = f"https://api.brightdata.com/datasets/v3/trigger?dataset_id={BRIGHTDATA_DATASET_ID}&type=discover_new&discover_by=profile_url"


def clean_handle_list(input_list):
    """
    Helper function for removing white space and @s from handles
    """
    stripped_list = [handle.rstrip() for handle in input_list]
    no_ats_list = [handle.replace("@", "") for handle in stripped_list]
    return no_ats_list


def todays_date():
    """
    Helper function to return today's date in YYYY-MM-DD format
    """
    return DT.date.today().strftime("%Y-%m-%d")


def yesterdays_date():
    """
    Helper function to return yesterday's date in YYYY-MM-DD format
    """
    return (DT.date.today() - DT.timedelta(days=1)).strftime("%Y-%m-%d")


def week_ago_date():
    """
    Helper function to return a week ago's date in YYYY-MM-DD format
    """
    return (DT.date.today() - DT.timedelta(days=7)).strftime("%Y-%m-%d")


def date_range_request_body(start_date, end_date, handle_list):
    """
    Generate the body of the API request for triggering collection through
    BrightData API.
    start_date and end_date format should be YYYY-MM-DD (i.e. "2024-10-15")
    handle_list should be a list of twitter handles
    """
    request_block = []
    for handle in handle_list:
        request_block.append(
            {
                "url": f"https://x.com/{handle}",
                "start_date": start_date,
                "end_date": end_date,
            }
        )
    return request_block


def trigger_brightdata_snapshot(start_date, end_date, handles_list):
    """
    Pass in a start_date and end_date in "YYYY-MM-DD" format and a list of twitter handles
    """
    response = requests.post(
        BRIGHTDATA_TRIGGER_API_ADDRESS,
        headers={
            "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
            "Content-Type": "application/json",
        },
        data=json.dumps(date_range_request_body(start_date, end_date, handles_list)),
    )
    return response.json()


def get_snapshot_results(snapshot_id):
    """
    Fetch results from snapshot ID, retry if still processing
    """
    result = requests.get(
        headers={
            "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
            "Content-Type": "application/json",
        },
        url=f"{BRIGHTDATA_RESULT_ADDRESS}/{snapshot_id}?format=json",
    )
    if result.status_code != 200:
        print(result.json()["status"], "- trying again in 10 seconds")
        time.sleep(10)
        get_snapshot_results(snapshot_id)
    else:
        return result.json()


def write_results_to_csv(result_json, output_sheet_name):
    """
    Write from JSON response result to given output sheet name
    """
    header_row = [
        "id",
        "user_posted",
        "name",
        "description",
        "date_posted",
        "photos",
        "url",
        "replies",
        "reposts",
        "likes",
        "views",
        "hashtags",
        "followers",
        "biography",
        "timestamp",
    ]
    with open(output_sheet_name, "w+", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header_row)
        tweet_rows = []
        for tweet in result_json:
            id = tweet["id"]
            user_posted = tweet["user_posted"]
            name = tweet["name"]
            description = tweet["description"]
            date_posted = dateutil.parser.parse(tweet["date_posted"])
            photos = tweet["photos"]
            url = tweet["url"]
            replies = tweet["replies"]
            reposts = tweet["reposts"]
            likes = tweet["likes"]
            views = tweet["views"]
            hashtags = tweet["hashtags"]
            followers = tweet["followers"]
            biography = tweet["biography"]
            timestamp = dateutil.parser.parse(tweet["timestamp"])

            if description is not None:
                description = str(description.replace("’", ""))

            if biography is not None:
                biography = str(biography.replace("’", ""))

            tweet_rows.append(
                [
                    id,
                    user_posted,
                    name,
                    description,
                    date_posted,
                    photos,
                    url,
                    replies,
                    reposts,
                    likes,
                    views,
                    hashtags,
                    followers,
                    biography,
                    timestamp,
                ]
            )
        writer.writerows(tweet_rows)


def collect_tweets(start_date, end_date, handles_list, output_sheet):
    print(f"Triggering collection of tweets from {len(handles_list)} accounts")
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")
    trigger_response = trigger_brightdata_snapshot(start_date, end_date, handles_list)
    snapshot_id = trigger_response["snapshot_id"]
    print(f"Snapshot ID: {snapshot_id}")
    results = get_snapshot_results(snapshot_id)
    print(f"Snapshot retrieved, writing to {output_sheet}")
    write_results_to_csv(results, output_sheet)
    print(f"Complete")

# collect_tweets(
#     start_date="2024-10-15",
#     end_date="2024-10-22",
#     handles_list=["SenatorBaldwin", "SenSherrodBrown"],
#     output_sheet="testing123.csv",
# )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--handlescsv', help='csv file containing twitter handles to collect tweets from (default = handles.csv)',
                        type=str,
                        default='handles.csv')
    parser.add_argument('--start-date', help='start date of collection in YYYY-MM-DD format (default is yesterday)',
                        type=str,
                        default=yesterdays_date())
    parser.add_argument('--end-date', help='end date of collection in YYYY-MM-DD format (default is today)',
                        type=str,
                        default=todays_date())
    parser.add_argument('--outputfile', help='filepath for results csv (default = bd-tweets.csv)',
                        default='bd-tweets.csv')
    args = parser.parse_args()
    
    handles_path = args.handlescsv
    output_path = args.outputfile
    
    with open(handles_path, 'r') as file:
        handles = clean_handle_list(file.read().split('\n'))
        
    collect_tweets(start_date=args.start_date,
                   end_date=args.end_date,
                   handles_list=handles,
                   output_sheet=output_path
                   )