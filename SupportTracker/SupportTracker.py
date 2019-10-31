from datetime import datetime
from datetime import timedelta
import json
import os

import pandas as pd
import re
import slacker


class SupportTracker:
    """ A client to read specific threads and populate to a G-sheet.

    **Environment variables**

    * ``SLACK_API_KEY``: The API key for your user in slack.

    """

    TOKEN_ENV_VAR = "SLACK_API_KEY"
    MAX_PAGE_SIZE = 100
    MAX_PAGE = 2
    REQUEST_MANAGER_NAME = "request manager"
    TEAM_NAME = "uptakeio-support"

    def __init__(self):
        # Set up Slack client
        self.client = slacker.Slacker(token=os.environ[self.TOKEN_ENV_VAR])
        self.client.auth.test()


    def get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        Get messages in a range of datetime.

        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a pandas dataframe containing the messages and their fields
        """
        messages = self._get_messages(channel_name, min_date, max_date)
        messages = [self._parse_message(m) for m in messages]
        messages = pd.DataFrame(messages)
        messages = messages[["date_time", "iid", "username", "permalink", "text"]]
        messages = messages.drop_duplicates(["username", "date_time"])
        return messages


    def analyzie_messages(self, messages: pd.DataFrame):
        """

        :param messages: A dataframe of all messages to be analyzed

        :retrun: A dataframe analyzed and ready to be pushed to support track sheet
        """
        rm_messages = messages[messages["username"] == self.REQUEST_MANAGER_NAME]
        col_names = ["date_time", "user", "link", "response_date", "resolved_date",
                     "category", "urgency", "request", "tenant", "language",
                     "library", "code_link", "code", "log_link", "log"]
        df = pd.DataFrame()
        for _, msg in rm_messages.iterrows():
            parsed = self._parse_request(msg)
            first_message = self._find_first_response(msg, messages)
            parsed["date_time"] = msg["date_time"]
            parsed["link"] = msg["permalink"]
            parsed["response_date"] = first_message["date_time"]
            parsed["resolved_date"] = None
            parsed = {k:[parsed[k]] for k in parsed.keys()}
            df = pd.concat([df, pd.DataFrame(parsed)])

        df = df[col_names]
        return(df)


    def _parse_request(self, message: pd.Series):
        """
        Parses a specific request for its fields and returns the fields.

        :param message: A message to be used for parsing

        :retrun: A dict of fields including:
        """
        text = message["text"]
        text = text.replace('\n', '')
        fields, starts, ends = self._get_template_info()
        res = {}
        for (f, s, e) in zip(fields, starts, ends):
            res[f] = self._extract_string(text, s, e)
        res["user"] = self._get_user_name(res["user"][2:-1])
        return res


    def _get_template_info(self):
        fields = ["urgency", "request", "user", "category",
                  "tenant", "language", "library",
                  "code_link", "code", "log_link", "log"]
        starts = ["circle:", "\*Request:\*", "\*From:\*", "\*Category:\*",
                  "\*Tenant:\*", "\*Programming language used:\*", "\*Specific library used:\*",
                  "\*Link to the code:\*", "\*Code:\*", "\*Link to the error logs:\*", "\*Error logs:\*"]
        ends = [":speech_balloon:", ":woman-raising-hand:", ":computer:", ":house:",
                ":python:", ":tensorflow:", ":hacker::link:",
                 ":hacker:", ":error::link:", ":error:", ""]
        return fields, starts, ends


    def _extract_string(self, text: str, start: str, end: str):
        """
        Gets a specific response from the text

        :param text: Input text to look for the response from
        :param start: The starting word to look for the response
        :param end: The ending word to look for the response

        :retrun: The response
        """
        try:
            res = re.search(f'{start}(.*){end}', text).group(1)
            res = res.strip()
            return res
        except:
            return None


    def _find_first_response(self, message: pd.Series, messages: pd.DataFrame):
        """
        Finds the first message that was replied to the message in its thread

        :param message: A message to be used as the initiation of conversation
        :param messages: A dataframe of all messages to be analyzed

        :retrun: A message that is the first message response to the message in the input
        """
        first_message = None
        if "thread" in message["permalink"]:
            timestamps = [self._extract_thread_ts(m) for m in messages["permalink"]]
            timestamp = self._extract_thread_ts(message["permalink"])
            indexes = [t == timestamp for t in timestamps]
            thread_messages = messages[indexes]
            thread_messages = thread_messages.sort_values("date_time", ascending = False)
            if thread_messages.shape[0] > 1:
                first_message = thread_messages.iloc[1, :]
            return first_message


    def _extract_thread_ts(self, link):
        """
        Get the thread timestamp from the message link

        :param link: permalink of the message

        :return: return the thread timestamp string
        """
        if "thread" in link:
            return link[-16:]
        else:
            return None


    def _get_messages(self, channel_name: str, min_date: datetime, max_date: datetime):
        """
        Get messages in a range of datetime.

        :param channel_name: name of the channel to look for the messages in
        :param min_date: minimum date to look for the messages
        :param max_date: maximum date to look for the messages

        :return: return a list of dicts of messages and their fields
        """
        page = self.MAX_PAGE
        messages = []
        stop = False
        while page >= 0 and not stop:
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


    def _get_user_name(self, user_id):
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

    def _get_user_id_workflow(self, user_name):
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

    def _get_team_id(self, team_name):
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



if __name__ == "__main__":
    channel_name = "platform-ds-slack-workflow-test"
    user_name = 'request manager'
    team_name = "uptakeio-support"
    csv_file = "support_track.csv"

    max_date = datetime(2019, 11, 1)
    min_date = max_date - timedelta(1)

    SLACK_API_KEY_FILE = os.environ["HOME"] + "/creds/slack-API-Key.txt"
    with open(SLACK_API_KEY_FILE, "r") as f:
        os.environ["SLACK_API_KEY"] = f.read()[:-1]

    st = SupportTracker()
    df = st.get_messages(channel_name, min_date, max_date)
    df = st.analyzie_messages(df)
    df.to_csv(csv_file)



