import pandas as pd

from supporttracker.client import SlackClient
from supporttracker.utils.logging import _log_info
from supporttracker.utils.utils import extract_string
from supporttracker.utils.utils import extract_thread_ts


class SupportExtractor:
    """
    A class that can parse and extract support requests.
    """

    def __init__(self, request_manager_name, request_template):
        self.request_manager_name = request_manager_name
        self.request_template = request_template

    def extract_requests(self, messages: pd.DataFrame):
        """
        :param messages: A dataframe of all messages to be analyzed
        :retrun: A dataframe analyzed and ready to be pushed to support track sheet
        """
        req_messages = messages[messages["username"] == self.request_manager_name]

        _log_info("Extracting the support requests.")
        df = pd.DataFrame()
        i = 0
        for _, msg in req_messages.iterrows():
            _log_info(f"Extracting support request #{i}.")
            parsed = self._parse_request(msg)
            parsed["date_time"] = msg["date_time"]
            parsed["link"] = msg["permalink"]
            parsed["resolved_date"] = None
            first_response = self._find_first_response(msg, messages)
            if first_response:
                parsed["response_date"] = first_response["date_time"]
            parsed = {k:[parsed[k]] for k in parsed.keys()}
            df = pd.concat([df, pd.DataFrame(parsed)])
            i = i + 1
        _log_info("Done extracting the support requests.")

        return(df)

    def _parse_request(self, message: pd.Series):
        """
        Parses a specific request for its fields and returns the fields.

        :param message: A message to be used for parsing

        :retrun: A dict of fields including:
        """
        text = message["text"]
        text = text.replace('\n', '')
        res = {}
        for f in self.request_template.keys():
            res[f] = extract_string(
                        text,
                        self.request_template[f]["start"],
                        self.request_template[f]["end"]
                    )
        return res

    def _find_first_response(self, message: pd.Series, messages: pd.DataFrame):
        """
        Finds the first message that was replied to the message in its thread

        :param message: A message to be used as the initiation of conversation
        :param messages: A dataframe of all messages to be analyzed

        :retrun: A message that is the first message response to the message in the input
        """
        first_message = None
        if "thread" in message["permalink"]:
            timestamps = [extract_thread_ts(m) for m in messages["permalink"]]
            timestamp = extract_thread_ts(message["permalink"])
            indexes = [t == timestamp for t in timestamps]
            thread_messages = messages[indexes]
            thread_messages = thread_messages.sort_values("date_time", ascending = False)
            if thread_messages.shape[0] > 1:
                first_message = thread_messages.iloc[1, :]
        return first_message










