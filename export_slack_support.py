import json
import os
from datetime import datetime, timedelta

import pandas as pd
import slacker



class SupportTracker:
    """ A client to read specific threads and populate to a G-sheet.

    **Environment variables**

    * ``SLACK_API_KEY``: The API key for your user in slack.

    """

    TOKEN_ENV_VAR = "SLACK_API_KEY"
    MAX_PAGE_SIZE = 100
    REQUEST_MANAGER_NAME = "request manager"

    def __init__(self):
        # Set up Slack client
        self.client = slacker.Slacker(token=os.environ[self.TOKEN_ENV_VAR])
        self.client.auth.test()


    def get_request_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a pandas dataframe containing the messages and their fields
        """


    def _get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a list of dicts of messages and their fields
        """

        channel_id = self._get_channel_id(channel_name)
        messages = self.client.channels.history(channel_id, latest=datetime.timestamp(max_date), oldest=datetime.timestamp(min_date)).body['messages']
        return messages

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



channel_name = "platform-ds-slack-workflow-test"
user_name = 'Request Manager'

SLACK_API_KEY_FILE = os.environ["HOME"] + "/creds/slack-API-Key.txt"
with open(SLACK_API_KEY_FILE, "r") as f:
    os.environ["SLACK_API_KEY"] = f.read()[:-1]

st = SupportTracker()
print(st._get_user_id_workflow(user_name))



channel_name = "platform-ds-slack-workflow-test"
user_name = 'request manager'


# m = st.client.search.messages(
#             query=f'in:{channel_name}',
#             sort='timestamp',
#             sort_dir='asc',
#             page=0,
#             count=1000
#         )
# for i in m.body['messages']['matches']:
#     print((i['permalink'], datetime.fromtimestamp(int(float(i['ts'])))))

# print(set([i['username'] for i in m.body['messages']['matches']]))

# max_date = datetime(2019, 10, 31)
# min_date = max_date - timedelta(1)



