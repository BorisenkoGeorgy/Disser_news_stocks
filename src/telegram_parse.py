import json
from datetime import datetime

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)

api_id = 9649158

api_hash = 'c2e8ba0cf1820324aafc7a11f98e4e29'

phone = '+79153585923'
username = 'jor_bored'

client = TelegramClient(username, api_id, api_hash)

channels = ['https://t.me/markettwits', 'https://t.me/investingcorp', 'https://t.me/economylive',
'https://t.me/banksta', 'https://t.me/FatCat18', 'https://t.me/StockNews100', 'https://t.me/gazpromneft_official',
'https://t.me/economika', 'https://t.me/nedvizhna24', 'https://t.me/divForever', 'https://t.me/DividendNews100',
'https://t.me/gazprom', 'https://t.me/cbonds']

async def main(phone, channels):
    await client.start()
    print("Client Created")

    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    for channel in channels:

        user_input_channel = channel

        if user_input_channel.isdigit():
            entity = PeerChannel(int(user_input_channel))
        else:
            entity = user_input_channel

        my_channel = await client.get_entity(entity)

        offset_id = 0
        limit = 1000
        all_messages = []
        total_messages = 0
        total_count_limit = 0

        while True:
            print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
            history = await client(GetHistoryRequest(
                peer=my_channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                all_messages.append(message.to_dict())
            offset_id = messages[len(messages) - 1].id
            total_messages = len(all_messages)
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break

        with open(f'Telegram data/{channel[13:]}_messages.json', 'w') as outfile:
            json.dump(all_messages, outfile, cls=DateTimeEncoder)

with client:
    client.loop.run_until_complete(main(phone, channels))