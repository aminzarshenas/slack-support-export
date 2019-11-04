import pandas as pd

from supporttracker.client import SlackClient
from supporttracker.utils.logging import _log_info
from supporttracker.utils.utils import extract_string
from supporttracker.utils.utils import extract_thread_ts


class SupportExtractor:
    """
    A class that can parse and extract support requests.
    """

    def __init__(self,
                 request_manager_names,
                 request_filter,
                 request_template_main,
                 request_template_thread):
        self.request_manager_names = request_manager_names
        self.request_filter = request_filter
        self.request_template_main = request_template_main
        self.request_template_thread = request_template_thread

    def extract_requests(self, messages: pd.DataFrame):
        """
        :param messages: A dataframe of all messages to be analyzed
        :retrun: A dataframe analyzed and ready to be pushed to support track sheet
        """
        req_messages = messages[messages["username"].isin(self.request_manager_names)]
        req_messages = req_messages[req_messages["text"].str.contains(self.request_filter)]

        _log_info("Extracting the support requests.")
        columns = ["date_time", "link", "response_date", "resolved_date"]
        columns.extend(self.request_template_main.keys())
        columns.extend(self.request_template_thread.keys())
        main_df = pd.DataFrame(columns=columns)
        i = 0
        for _, msg in req_messages.iterrows():
            _log_info(f"Extracting support request #{i}.")
            df = pd.DataFrame({k: [None] for k in columns})

            # request main message
            parsed = self._parse_request(msg, self.request_template_main)
            df = self._update_datafram(df, parsed)

            # request thread message
            first_response = self._find_nth_response(msg, messages, 1)
            if first_response is not None:
                parsed = self._parse_request(first_response, self.request_template_thread)
                df = self._update_datafram(df, parsed)

            # first non-request reply in the thread
            second_response = self._find_nth_response(msg, messages, 2)
            if second_response is not None:
                df["response_date"] = second_response["date_time"]

            # meta data
            df["date_time"] = msg["date_time"]
            df["link"] = msg["permalink"]
            df["resolved_date"] = None

            main_df = pd.concat([main_df, df])
            i = i + 1

        _log_info("Done extracting the support requests.")
        return(main_df)

    def _update_datafram(self, df: pd.DataFrame, updates: dict):
        """
        Updates a dataframe based on a dictornary.
        :param df: Pandas dataframe to be updated
        :param updates: fields and their new values

        :retrun: Update dataframe
        """
        for k in updates.keys():
            df[k] = updates[k]
        return df

    def _parse_request(self, message: pd.Series, template: dict):
        """
        Parses a specific request for its fields and returns the fields.

        :param message: A message to be used for parsing
        :param template: A template that can be used to extract specific fields

        :retrun: A dict of fields
        """
        text = message["text"]
        text = text.replace('\n', '')
        res = {}
        for f in template.keys():
            res[f] = extract_string(
                        text,
                        template[f]["start"],
                        template[f]["end"]
                    )
        return res

    def _find_nth_response(self, message: pd.Series, messages: pd.DataFrame, n: int):
        """
        Finds the nth message that was replied to the message in its thread

        :param message: A message to be used as the initiation of conversation
        :param messages: A dataframe of all messages to be analyzed
        :param n: To return the n th message

        :retrun: A message that is the nth message response in the thread
        """
        nth_message = None
        if "thread" in message["permalink"]:
            timestamps = [extract_thread_ts(m) for m in messages["permalink"]]
            timestamp = extract_thread_ts(message["permalink"])
            indexes = [t == timestamp for t in timestamps]
            thread_messages = messages[indexes]
            thread_messages = thread_messages.sort_values("date_time", ascending = True)
            if thread_messages.shape[0] > n:
                nth_message = thread_messages.iloc[n, :]
        return nth_message










