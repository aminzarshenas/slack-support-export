from datetime import datetime
import re



def extract_string(text: str, start: str, end: str):
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


def extract_thread_ts(link):
    """
    Get the thread timestamp from the message link

    :param link: permalink of the message

    :return: return the thread timestamp string
    """
    if "thread" in link:
        return link[-16:]
    else:
        return None


def extract_user_id(user_id):
    """
    Get the user id exracted from a bad format (e.g, 123 from <@123>)

    :param user_id: user id in a bad format

    :return: return user id in good format
    """
    return user_id[2:-1]


def timestamp_string_to_datetime(timestamp_string: str):
    """
    :param timestamp_string: timestamp

    :return: return the corresponding datetime
    """
    t = datetime.fromtimestamp(int(float(timestamp_string)))
    return t









