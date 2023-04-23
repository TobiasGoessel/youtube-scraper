from googleapiclient.discovery import build
import pandas as pd
import requests
from apiclient import errors


DATA_DICTS = []


def load_comments(match, video_title, channel_name, video_releasedate):
    """
    This function loads the infomration from each comment page which is given from get_comment_threads()
    :param match:
    :param video_title:
    :param channel_name:
    :param video_releasedate:
    :return data_list:
    """
    data_list = []
    for item in match["items"]:
        comment = item["snippet"]["topLevelComment"]
        author = comment["snippet"]["authorDisplayName"]
        text = comment["snippet"]["textDisplay"]
        likes = comment["snippet"]["likeCount"]
        date = comment["snippet"]["updatedAt"]
        video_id = comment["snippet"]["videoId"]
        replysCount = item["snippet"]["totalReplyCount"]
        video_title = video_title
        channel_name = channel_name
        video_releasedate = video_releasedate
        datas = {"Channel": channel_name, "Video-Title": video_title, "video_releasedate": video_releasedate,
                 "Author": author, "Comments": text, "Likes": likes, "Replies": replysCount, "Date": date, "videoID": video_id}
        data_list.append(datas)

    return data_list


def get_comment_threads(cfg_api_key: str, cfg_api_service_name: str, cfg_api_version: str, video_id: str,
                        next_page_token: str):
    """
    This function gets all information to build the youtube api connection and returns one page of comment threads
    if it raises an error the quota is probably exceeded; you can adjust by maxResults
    :param cfg_api_key:
    :param cfg_api_service_name:
    :param cfg_api_version:
    :param video_id:
    :param next_page_token:
    :return results:
    """
    try:
        youtube = build(cfg_api_service_name, cfg_api_version, developerKey=cfg_api_key)
        results = youtube.commentThreads().list(
            part="snippet",
            #maxResults=50,
            videoId=video_id,
            textFormat="plainText",
            pageToken=next_page_token
        ).execute()
        return results
    except errors.HttpError:
        print("Quota exceeded")


def youtubecomment_grabber(cfg_api_key: str, cfg_api_service_name: str, cfg_api_version: str, video_id=None,
                           video_title=None, channel_name=None, video_releasedate=None):
    """
    This function takes all arguments from the configloader() and video_id, video_title, channel_name,
    video_release date to get all the comments from each page and checks for another nextPageToken and returns a
    pandas-dataframe and write them to a .csv for each video.

    :param cfg_api_key:
    :param cfg_api_service_name:
    :param cfg_api_version:
    :param video_id:
    :param video_title:
    :param channel_name:
    :param video_releasedate:
    :return dataframe:
    """
    dataframe = pd.DataFrame()
    match = get_comment_threads(cfg_api_key=cfg_api_key, cfg_api_service_name=cfg_api_service_name,
                                cfg_api_version=cfg_api_version, video_id=video_id, next_page_token="")
    try:
        next_page_token = match["nextPageToken"]
    except KeyError:
        next_page_token = None
        print("No next_page_token left.")
    data_from_yt = load_comments(match, video_title, channel_name, video_releasedate)
    dataframe = pd.concat([dataframe, pd.DataFrame(data_from_yt)])

    while next_page_token:
        try:
            match = get_comment_threads(cfg_api_key=cfg_api_key, cfg_api_service_name=cfg_api_service_name,
                                        cfg_api_version=cfg_api_version, video_id=video_id,
                                        next_page_token=next_page_token)
            next_page_token = match["nextPageToken"]
            data_from_yt = load_comments(match, video_title, channel_name, video_releasedate)
            dataframe = pd.concat([dataframe, pd.DataFrame(data_from_yt)])
        except KeyError:
            next_page_token = ""
            print("No more pages.")

    dataframe["Date"] = pd.to_datetime(dataframe["Date"], format="%Y-%m-%dT%H:%M:%S")
    dataframe.to_csv(f"..\\data\\comments\\{video_id}-comments.csv", encoding="UTF-8-sig", sep=";", index=False,
                     date_format="%Y-%m-%d %H:%M:%S")
    return dataframe


def yt_meta_reader(vid_id=None, part="statistics"):
    """
    This function calls the youtube API with a video_id AND writes a dataframe with the meta_data  -statistics.csv files

    :param vid_id:
    :param part:
    :return df:
    """
    relevant_parts = ["snippet", "statistics", "topicDetails", "subscriptions"]
    url = f"https://www.googleapis.com/youtube/v3/videos?part={part}&" \
          f"id={vid_id}&key=AIzaSyANO0L6u7oTDJjxKnzyg4j_gGHYjtDcNpE"
    data = requests.get(url).json()
    df = pd.json_normalize(data, record_path=["items"])
    df.to_csv(f"..\\data\\statistics\\{vid_id}-statistics.csv", encoding="UTF-8-sig", sep=";")
    return df


def yt_meta_comment_concatenator(path_meta=None, path_commentdata=None):
    """
    This function takes a path_meta and path_commentdata dataframe to concetenate to -wholesome.csv files.

    :param path_meta:
    :param path_commentdata:
    :return:
    """
    df_meta = pd.read_csv(path_meta, encoding="UTF-8-sig", sep=";")
    df_meta = df_meta.rename(columns={"id": "snippet.videoId"})
    videoid =  df_meta["snippet.videoId"][0]
    df_meta = df_meta.set_index("snippet.videoId")
    df_meta = df_meta.drop(columns=['Unnamed: 0', 'kind', 'etag'])
    df_commentdata = pd.read_csv(path_commentdata, encoding="UTF-8-sig", sep=";")
    df_commentdata = df_commentdata.rename(columns={"videoID": "snippet.videoId"})
    df_commentdata = df_commentdata.set_index("snippet.videoId")
    df_concated = df_meta.join(df_commentdata)
    df_concated.to_csv(f".\\data\\{videoid}-wholesome.csv", encoding="UTF-8-sig", sep=";")


def load_channellist(match):
    """
    This function takes a match from the get_comment_threads function to check if there are more pages to load.
    after getting the next page it writes the video_title, video_id, channel_name to the Global DATA_DICTS

    :param match:
    :return:
    """
    try:
        np_token = match["nextPageToken"]
    except KeyError:
        np_token = ""
        print("No nextpage token found.")

    for item in match["items"]:
        video_title = item["snippet"]["title"]
        video_id = item["snippet"]["resourceId"]["videoId"]
        channel_name = item["snippet"]["channelTitle"]
        date = item["snippet"]["publishedAt"]
        datas = {"video-title": video_title, "video_id": video_id, "Channel": channel_name, "Date": date,
                 "NP_token": np_token}
        DATA_DICTS.append(datas)
        if 'replies' in item.keys():
            for reply in item['replies']['comments']:
                rauthor = reply['snippet']['authorDisplayName']
                rtext = reply["snippet"]["textDisplay"]



def channellist_videoid_getter(playlist_id: str, cfg_api_service_name="", cfg_api_key="",
                               cfg_api_version="", np_token=""):
    """
    This function takes a playlist_id and configloader() to read all video_ids from a given playlist.

    :param playlist_id:
    :param cfg_api_service_name:
    :param cfg_api_key:
    :param cfg_api_version:
    :param np_token:
    :return results:
    """
    try:
        youtube = build(cfg_api_service_name, cfg_api_version, developerKey=cfg_api_key)
        results = youtube.playlistItems().list(
            part="snippet",
            maxResults=1000,
            playlistId=playlist_id,
            pageToken=np_token
        ).execute()
        return results
    except errors.HttpError:
        print("Quota exceeded.")


def channellist_grabber(cfg_api_service_name: str, cfg_api_version: str, cfg_api_key: str, playlist_id: str,
                        np_token: str):
    """
    This function starts the channellist_videoid_getter() to create a api-request for video_ids from a channel.
    During the API-Call, the function will check if there are nextPageToken to retrieve more comments.

    :param cfg_api_service_name:
    :param cfg_api_key:
    :param cfg_api_version:
    :param playlist_id:
    :param np_token:
    :return data:
    """
    try:
        youtube = build(cfg_api_service_name, cfg_api_version, developerKey=cfg_api_key)
        match = youtube.playlistItems().list(
            part="snippet",
            maxResults=1000,
            playlistId=playlist_id,
            pageToken=np_token
        ).execute()
    except errors.HttpError:
        print("Quota exceeded.")
        return match
    try:
        next_page_token = match["nextPageToken"]
    except KeyError:
        next_page_token = ""
        print("no more next page token")
    load_channellist(match)

    while next_page_token:
        try:
            match = channellist_videoid_getter(playlist_id=playlist_id, cfg_api_service_name=cfg_api_service_name,
                                               cfg_api_key=cfg_api_key, cfg_api_version=cfg_api_version,
                                               np_token=next_page_token)
            next_page_token = match["nextPageToken"]
            load_channellist(match)
        except KeyError:
            next_page_token = ""
            print("No more pages")
    data = pd.DataFrame(data=DATA_DICTS)
    data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%dT%H:%M:%SZ")
    data.to_csv(f"..\\data\\test.csv", encoding="UTF-8-sig", sep=";", index=False,
                date_format="%Y-%m-%d %H:%M:%S")
    return data


if __name__ == '__main__':
    channellist_grabber()
