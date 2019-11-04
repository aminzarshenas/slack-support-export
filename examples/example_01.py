from datetime import datetime
from datetime import timedelta
import os

from supporttracker.client import SlackClient
from supporttracker.extractor import SupportExtractor
from supporttracker.templates import platform_ds_request_template
from supporttracker.utils.utils import extract_user_id


if __name__ == "__main__":

    # set API keys
    SLACK_API_KEY_FILE = os.environ["HOME"] + "/creds/slack-API-Key.txt"
    with open(SLACK_API_KEY_FILE, "r") as f:
        os.environ["SLACK_API_KEY"] = f.read()[:-1]

    # channel name, request manager name, and the file to save the results
    channel_name = "platform-ds-slack-workflow-test"
    request_manager_name = 'request manager'
    csv_file = "./example_01_support_requests.csv"

    # dates we are interested to read the supports from
    max_date = datetime(2019, 11, 3)
    min_date = max_date - timedelta(2)

    # get all of the messages for that specific date range
    sc = SlackClient()
    messages_df = sc.get_messages(channel_name, min_date, max_date)

    # extract the support requests and its fields
    se = SupportExtractor(request_manager_name, platform_ds_request_template)
    requests_df = se.extract_requests(messages_df)

    # post processing of the dataframe
    # convert the user id column to user name column
    requests_df['user'] = requests_df['user'].apply(extract_user_id)
    requests_df['user'] = requests_df['user'].apply(sc.get_user_name)
    requests_df.to_csv(csv_file)