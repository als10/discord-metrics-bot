import discord
import os
from dotenv import load_dotenv
import pandas as pd
import nest_asyncio
from datetime import datetime, timedelta

load_dotenv()
nest_asyncio.apply()

client = discord.Client()
file_dir = "data"
channels = []

@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))
    print('getting channels...')
    for server in client.guilds:
        for channel in server.channels:
            if str(channel.type) == 'text':
                channels.append(channel)
    print(f'got all {len(channels)} channels')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('$metrics'):
        params = message.content.split()[1:]

        today = datetime.today().strftime('%Y-%m-%d')
        file_location = f'{file_dir}/{today}.csv'

        print('getting messages...')
        data = pd.DataFrame(columns=['date', 'time', 'author', 'channel'])
        for channel in channels:
            channel_name = channel.name
            async for msg in channel.history(limit=20000):
                date = msg.created_at.strftime('%Y-%m-%d')
                time = msg.created_at.strftime('%H:%M')
                data = data.append({'date': date,
                                    'time': time,
                                    'author': msg.author.name,
                                    'channel': channel_name}, ignore_index=True)
    
        print('got all messages')
        data.to_csv(file_location)
        print(f'saved as csv to {file_location}')

        start_date = params[0] if len(params) > 0 else datetime.today().strftime('%Y-%m-%d')
        end_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=7)
        await calculate_metrics(data, start_date, end_date.strftime('%Y-%m-%d'), message.channel)

async def calculate_metrics(data, start_date, end_date, channel):
    print('calculating metrics...')
    metrics = {}

    print(f'total number of messages: {len(data)}')

    print(start_date, 'to', end_date)

    data = data.loc[(data['date'] >= start_date) & (data['date'] <= end_date)]
    weekly = {
        'messages': len(data),
        'active users': len(data['author'].unique())
    }
    metrics['weekly'] = weekly

    dates_in_week = list(data['date'].unique())
    daily = {}
    for date in dates_in_week:
        messages_on_date = data.loc[data['date'] == date]
        daily[date] = {
            'messages': len(messages_on_date),
            'active users': len(messages_on_date['author'].unique())
        }
    metrics['daily'] = daily

    total_users_who_joined = len(data.loc[data['channel'] == '👋-welcome-home-to-fullcircle'])
    metrics['members'] = { 'total users who joined': total_users_who_joined }

    await print_metrics(metrics, channel)

    print('got all metrics')


async def print_metrics(metrics, channel):
    message = ''
    for group in metrics:
        message += f'{group}\n'
        for metric in sorted(metrics[group].keys()):
            message += f'{metric}\t{metrics[group][metric]}\n'
        message += '\n'
    await channel.send(message)

# c = input('via discord message command? ')
# if c != 'y':
#     c = 'y'
#     while c == 'y':
#         file_name = input('file name: ')
#         start_date = input('start date: ')
#         end_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=7)
#         calculate_metrics(pd.read_csv(f'data/{file_name}'), start_date, end_date.strftime('%Y-%m-%d'))
#         c = input('continue? ')
                        
client.run(os.getenv('TOKEN'))