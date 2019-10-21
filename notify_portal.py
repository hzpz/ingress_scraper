import argparse
from configparser import ConfigParser

import telebot
from pymysql import connect

DEFAULT_CONFIG = "default.ini"

PORTAL_SELECT_QUERY = """SELECT * FROM {db_ingress}.ingress_portals WHERE checked is NULL"""
PORTAL_UPDATE_QUERY = """UPDATE {db_ingress}.ingress_portals SET checked=1 WHERE id = %s"""


def create_config(config_path):
    """ Parse config. """
    config = dict()
    config_raw = ConfigParser()
    config_raw.read(DEFAULT_CONFIG)
    config_raw.read(config_path)
    config['db_r_host'] = config_raw.get(
        'DB',
        'HOST')
    config['db_r_name'] = config_raw.get(
        'DB',
        'NAME')
    config['db_r_user'] = config_raw.get(
        'DB',
        'USER')
    config['db_r_pass'] = config_raw.get(
        'DB',
        'PASSWORD')
    config['db_r_port'] = config_raw.getint(
        'DB',
        'PORT')
    config['db_r_charset'] = config_raw.get(
        'DB',
        'CHARSET')
    config['db_ingress'] = config_raw.get(
        'DB',
        'DB_INGRESS')
    config['telegram_token'] = config_raw.get(
        'Telegram',
        'BOT_TOKEN')
    config['telegram_channel'] = config_raw.get(
        'Telegram',
        'CHANNEL')

    return config


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", default="default.ini", help="Config file to use")
    args = parser.parse_args()
    config_path = args.config
    config = create_config(config_path)

    print("Initialize/Start DB Session")
    mydb_r = connect(
        host=config['db_r_host'],
        user=config['db_r_user'],
        passwd=config['db_r_pass'],
        database=config['db_ingress'],
        port=config['db_r_port'],
        charset=config['db_r_charset'],
        autocommit=True)

    mycursor_ingress = mydb_r.cursor()
    print("Connection clear")

    portal_select_query = PORTAL_SELECT_QUERY.format(db_ingress=config['db_ingress'])

    mycursor_ingress.execute(portal_select_query)
    portals = mycursor_ingress.fetchall()

    bot = telebot.TeleBot(config['telegram_token'])

    for id, external_id, lat, lon, name, url, updated, imported, checked in portals:
        message = 'Neues Portal: [{}](https://www.google.com/maps/search/?api=1&query={},{})'.format(name, str(lat), str(lon))
        bot.send_photo(config['telegram_channel'], url, message, parse_mode='Markdown')
        portal_update_query = PORTAL_UPDATE_QUERY.format(db_ingress=config['db_ingress'])
        mycursor_ingress.execute(portal_update_query, id)
