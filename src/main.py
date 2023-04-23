import yt_scraper
import yaml
import argparse


# loading configuration file
def configloader(path: str):
    """
    Takes path to load config file.
    :param path:
    :return:
    """
    with open(path) as cfg:
        configuration = yaml.load(cfg, Loader=yaml.FullLoader)
    return configuration["ytcommentscraper"]


def scraper_channelist(cfg: dict):
    """
    This function takes cfg to start the channellist_grabber() and download a dataframe of video_ids and save them
    to desk.
    :param cfg:
    :return:
    """
    channel_list = yt_scraper.channellist_grabber(cfg_api_service_name=cfg["api_service_name"],
                                                  cfg_api_version=cfg["api_version"], cfg_api_key=cfg["APIKey"],
                                                  playlist_id="", np_token="")
    return channel_list


def scraper_comments(cfg: dict):
    """
    This function takes cfg to start the youtubecomment_grabber() and download a dataframe of comments for the given
    video_id and save them to desk.
    :param cfg:
    :return:
    """
    channel_list = yt_scraper.youtubecomment_grabber(cfg_api_key=cfg["APIKey"], cfg_api_service_name=cfg["api_service_name"],
                                                     cfg_api_version=cfg["api_version"], video_id="", video_title="",
                                                     channel_name="", video_releasedate="")
    return channel_list


parser = argparse.ArgumentParser()
parser.add_argument("-a", "--action", help="Get data by channellist or video_id use -a ""channel_list"" or "
                                           """video_id""")
args = parser.parse_args()


if __name__ == '__main__':
    if args.action == "channel_list":
        config = configloader(path="..\\data\\config\\config.yaml")
        scraper_channelist(config)
    if args.action == "video_id":
        config = configloader(path="..\\data\\config\\config.yaml")
        scraper_comments(config)
