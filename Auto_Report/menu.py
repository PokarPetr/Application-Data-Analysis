"""The script collects application metrics for the past week,
generates a message and several graphs and sends them to the telegram chat.

Скрипт собирает метрики приложения за прошедшую неделю,
формирует сообщение и несколько графиков и отправляет их в телеграм-чат
"""

import telegram
import asyncio
import pandas as pd
import os

from query_template import Query
from read_db.CH import GetClickhouse
from feed_report import FeedReport
from get_plot import PlotObject


query_feed = Query.QUERY_FEED
data = GetClickhouse(query_feed).df                     # DataFrame with all interested metrics.
data['date'] = pd.to_datetime(data['date']).dt.date     # Transform values of 'data' column into the date format.
metrics_list = ['DAU', 'likes', 'views', 'events', 'posts']
data = [data.astype({metric: int}).dtypes for metric in metrics_list]   # Change data types of some metrics to 'int'.


mess = FeedReport(data).report          # Create a message to send it to the telegram chat
plot = PlotObject(data).plot_object     # Create a plot object to send it to the telegram chat

chat_id = 1259270015
bot = telegram.Bot(token=os.environ.get('REPORT_BOT_TOKEN'))

async def mess_send():
    """    Send the message to telegram"""
    await bot.sendMessage(chat_id=chat_id, text=mess)

async def plot_send():
    """    Send the plot object to telegram"""
    await bot.sendPhoto(chat_id=chat_id, photo=plot)

async def main():
    mess_task = asyncio.create_task(mess_send())
    plot_task = asyncio.create_task(plot_send())
    await asyncio.wait([mess_task, plot_task])

asyncio.run(main())
