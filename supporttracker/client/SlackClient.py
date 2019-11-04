from datetime import datetime
import logging
import os

import pandas as pd
import slacker

from supporttracker.utils.logging import _log_info
from supporttracker.utils.utils import timestamp_string_to_datetime


class SlackClient:
    """
    A client that can read messages, retrieve user id, channel id, etc.

    **Environment variables**

    * ``SLACK_API_KEY``: The API key for your user in slack.

    """

    _TOKEN_ENV_VAR = "SLACK_API_KEY"
    _MAX_PAGE_SIZE = 100
    _MAX_PAGE = 10

    def __init__(self):
        _log_info("Initiating the Slack client.")
        self.client = slacker.Slacker(token=os.environ[self._TOKEN_ENV_VAR])
        self.client.auth.test()

    def get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        Get messages in a range of datetime.

        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a pandas dataframe containing the messages and their fields
        """
        logging.info(f"Pulling the messages between {min_date} and {max_date} from {channel_name}.")
        messages = self._get_messages(channel_name, min_date, max_date)
        messages = [self._parse_message(m) for m in messages]
        messages = pd.DataFrame(messages)
        messages = messages[["date_time", "iid", "username", "permalink", "text"]]
        messages = messages.drop_duplicates(["username", "date_time", "text"])
        logging.info(f"Done pulling the messages.")
        return messages

    def get_channel_id(self, channel_name: str):
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

    def get_user_id(self, user_name):
        """
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

    def get_user_name(self, user_id):
        """
        Find the user name from the user id.

        :param user_id: id of the user

        :return: user name
        """
        user_name = None
        user_list = self.client.users.list().body["members"]
        user_name = [u["name"] for u in user_list if u["id"] == user_id]
        if len(user_name) > 0:
            user_name = user_name[0]
        return user_name

    def get_user_id_workflow(self, user_name):
        """
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

    def get_team_id(self, team_name):
        """
        Find the team id from the team name.

        :param team_name: name of the team

        :return: team id
        """
        team_id = None
        team_list = self.client.usergroups.list().body["usergroups"]
        team_id = [u["team_id"] for u in team_list if u["handle"] == team_name]
        if len(team_id) > 0:
            team_id = team_id[0]
        return team_id

    def _get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        Get messages in a range of datetime.

        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a list of dicts of messages and their fields
        """
        page = self._MAX_PAGE
        messages = []
        stop = False
        while page >= 0 and not stop:
            res = self._get_one_page_messages(channel_name, page, self._MAX_PAGE_SIZE)
            if res:
                for msg in res:
                    t = timestamp_string_to_datetime(msg["ts"])
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
        message["date_time"] = timestamp_string_to_datetime(message["ts"])
        message["ts"] = None
        return message










