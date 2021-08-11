import discord
import os
from dotenv import load_dotenv
import pandas as pd
import nest_asyncio
from datetime import datetime, timedelta

load_dotenv()
nest_asyncio.apply()

client = discord.Client()
channels = []

@client.event
async def on_ready():
    print("We have logged in as {0.user}".format(client))
    print("getting channels...")
    for server in client.guilds:
        for channel in server.channels:
            if str(channel.type) == "text":
                channels.append(channel)
    print(f"got all {len(channels)} channels")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith("$metrics"):
        params = message.content.split()[1:]
        mode = 2

        if len(params) == 0:
            end_date = datetime.today()
            start_date = end_date - timedelta(days=6)
        else:
            start_date = datetime.strptime(params[0], "%Y-%m-%d")
            end_date = start_date + timedelta(days=6)
        
        print("getting messages...")
        data = pd.DataFrame(columns=["date", "time", "author", "channel"])

        for channel in channels:
            channel_name = channel.name
            try:
                async for msg in channel.history(limit=100000):
                    date = msg.created_at.strftime("%Y-%m-%d")
                    time = msg.created_at.strftime("%H:%M")
                    data = data.append(
                        {
                            "date": date,
                            "time": time,
                            "author": msg.author.name,
                            "channel": channel_name,
                        },
                        ignore_index=True,
                    )
            except Exception as e:
                continue

        print("got all messages")

        await message.channel.send(
            embed=calculate_metrics(
                data, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            )
        )


def calculate_metrics(data, start_date, end_date):
    print("calculating metrics...")
    metrics = {}

    print(f"total number of messages: {len(data)}")

    print(start_date, "to", end_date)

    data = data.loc[(data["date"] >= start_date) & (data["date"] <= end_date)]
    weekly = {"messages": len(data), "active users": len(data["author"].unique())}
    metrics["weekly"] = weekly

    dates_in_week = list(data["date"].unique())
    daily = {}
    for date in dates_in_week:
        messages_on_date = data.loc[data["date"] == date]
        daily[date] = {
            "messages": len(messages_on_date),
            "active users": len(messages_on_date["author"].unique()),
        }
    metrics["daily"] = daily

    try:
        total_users_who_joined = len(
            data.loc[data["channel"] == os.getenv("WELCOME_CHANNEL")]
        )
        metrics["members"] = {"total users who joined": total_users_who_joined}
    except Exception as e:
        pass

    print("got all metrics")
    return send_metrics_message(metrics)


def send_metrics_message(metrics):
    embed = discord.Embed(
        title=f"__**Server Metrics**__", color=0x03F8FC, timestamp=datetime.now()
    )
    for group in metrics:
        embed.add_field(
            name=f"**{group}**",
            value="\n".join(
                f"{m} {metrics[group][m]}" for m in sorted(metrics[group].keys())
            ),
            inline=False,
        )
    return embed


client.run(os.getenv("TOKEN"))
