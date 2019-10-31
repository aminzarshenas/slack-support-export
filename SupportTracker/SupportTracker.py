from datetime import datetime
from datetime import timedelta
import json
import os

import pandas as pd
import slacker


class SupportTracker:
    """ A client to read specific threads and populate to a G-sheet.

    **Environment variables**

    * ``SLACK_API_KEY``: The API key for your user in slack.

    """

    TOKEN_ENV_VAR = "SLACK_API_KEY"
    MAX_PAGE_SIZE = 100
    MAX_PAGE = 1
    REQUEST_MANAGER_NAME = "request manager"

    def __init__(self):
        # Set up Slack client
        self.client = slacker.Slacker(token=os.environ[self.TOKEN_ENV_VAR])
        self.client.auth.test()


    def get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a pandas dataframe containing the messages and their fields
        """
        messages = self._get_messages(channel_name, min_date, max_date)
        messages = [self._parse_message(m) for m in messages]
        messages = pd.DataFrame(messages)
        messages = messages[["date_time", "iid", "username", "permalink", "text"]]
        return messages


    def _get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a list of dicts of messages and their fields
        """
        page = self.MAX_PAGE
        messages = []
        stap = False
        while page >= 0 and not stap:
            res = self._get_one_page_messages(channel_name, page, self.MAX_PAGE_SIZE)
            if res:
                for msg in res:
                    t = self._timestamp_string_to_datetime(msg["ts"])
                    if t < min_date:
                        stop = True
                        break
                    if t >= min_date and t < max_date:
                        messages.append(msg)
            page = page - 1

        messages.reverse()
        return messages


    def _get_one_page_messages(self, channel_name: str, page: int, page_size: int):

        """
        :param channel_name: name of the channel to look for the messages in
        :param page: starting page for pagination
        :param page_size: page_size for pagination

        :return: return a list of dicts of messages and their fields
        """
        res = self.client.search.messages(
                    query=f'in:{channel_name}',
                    sort='timestamp',
                    sort_dir='desc',
                    page=page,
                    count=page_size
                ).body['messages']['matches']
        return res


    def _parse_message(self, message: dict):

        """
        Extracts and converts the important fields of the message

        :param message: message to be parsed

        :return: modified message
        """
        keys = ["ts", "iid", "username", "permalink", "text"]
        message = {k: message[k] for k in keys}
        message["date_time"] = self._timestamp_string_to_datetime(message["ts"])
        message["ts"] = None
        return message


    def _timestamp_string_to_datetime(self, timestamp_string: str):
        """
        :param timestamp_string: timestamp

        :return: return the corresponding datetime
        """
        t = datetime.fromtimestamp(int(float(timestamp_string)))
        return t


    def _get_channel_id(self, channel_name: str):
        """
        Find the channel id from the channel name.

        :param channel_name: name of the channel

        :return: channel id
        """
        channel_id = None
        channel_list = self.client.channels.list().body["channels"]
        channel_id = [ch["id"] for ch in channel_list if ch["name"] == channel_name]
        if len(channel_id) > 0:
            channel_id = channel_id[0]
        return channel_id


    def _get_user_id(self, user_name):
        """ Description
        Find the user id from the user name.

        :param user_name: name of the user

        :return: user id
        """
        user_id = None
        user_list = self.client.users.list().body["members"]
        user_id = [u["id"] for u in user_list if u["name"] == user_name]
        if len(user_id) > 0:
            user_id = user_id[0]
        return user_id

    def _get_user_id_workflow(self, user_name):
        """ Description
        Find the user id from the user name for a workflow user.

        :param user_name: name of the user

        :return: user id
        """
        user_id = None
        user_list = self.client.users.list().body["members"]
        for u in user_list:
            if "real_name" in u.keys():
                if u["real_name"] == user_name:
                    user_id = u["id"]
        return user_id



if __name__ == "__main__":
    channel_name = "platform-ds-slack-workflow-test"
    user_name = 'Request Manager'
    csv_file = "support_track.csv"

    max_date = datetime(2019, 10, 31)
    min_date = max_date - timedelta(1)

    SLACK_API_KEY_FILE = os.environ["HOME"] + "/creds/slack-API-Key.txt"
    with open(SLACK_API_KEY_FILE, "r") as f:
        os.environ["SLACK_API_KEY"] = f.read()[:-1]

    st = SupportTracker()
    df = st.get_messages(channel_name, min_date, max_date)
    df.to_csv(csv_file)



