from datetime import datetime


def parse_message(message: dict):

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


def timestamp_string_to_datetime(timestamp_string: str):
    """
    :param timestamp_string: timestamp

    :return: return the corresponding datetime
    """
    t = datetime.fromtimestamp(int(float(timestamp_string)))
    return t


def get_channel_id(channel_name: str):
    """
    Find the channel id from the channel name.

    :param channel_name: name of the channel

    :return: channel id
    """
    channel_id = None
    client =
    channel_list = self.client.channels.list().body["channels"]
    channel_id = [ch["id"] for ch in channel_list if ch["name"] == channel_name]
    if len(channel_id) > 0:
        channel_id = channel_id[0]
    return channel_id


def get_user_id(self, user_name):
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

def get_user_id_workflow(self, user_name):
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