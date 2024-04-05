import os
import logging
from flask import Flask, request, redirect, render_template
from aiogram import Bot
from aiogram.dispatcher import Dispatcher
from datetime import datetime, timedelta
import random
import string
import asyncpg
import asyncio

# Налаштування логування
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_CONFIG = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT', '25060'),
    'database': os.environ.get('DB_NAME'),
    'ssl': os.environ.get('DB_SSL', 'require')
}

app = Flask(__name__)

class BaseBot:
    def __init__(self, bot_token, chat_id, identifiers_table, user_data_table, prefix):
        self.bot = Bot(token=bot_token)
        self.dp = Dispatcher(self.bot)
        self.chat_id = chat_id
        self.identifiers_table = identifiers_table
        self.user_data_table = user_data_table
        self.prefix = prefix

    async def insert_identifier(self, value):
        try:
            conn = await asyncpg.connect(**DB_CONFIG)
            await conn.execute(f'INSERT INTO {self.identifiers_table} (value) VALUES ($1)', value)
            await conn.close()
        except Exception as error:
            logging.error(f"Error inserting identifier: {error}")
            print(f"Error1 inserting identifier: {error}")
            raise

    async def get_identifier(self, value):
        try:
            conn = await asyncpg.connect(**DB_CONFIG)
            row = await conn.fetchrow(f'SELECT * FROM {self.identifiers_table} WHERE value = $1', value)
            await conn.close()
            return row
        except Exception as error:
            logging.error(f"Error getting identifier: {error}")
            print(f"Error getting identifier: {error}")
            raise

    async def save_to_database(self, data, identifier, ip_address, user_agent):
        try:
            connection = await asyncpg.connect(**DB_CONFIG)
            data['unique_identifier'] = str(data['unique_identifier'])

            query = f'''
                INSERT INTO {self.user_data_table} (
                    pixel, campaign_id, adset_id, ad_id, campaign_name,
                    adset_name, ad_name, placement, site_source_name,
                    fbclid, unique_identifier, channel_join_link, source_identifier,
                    ip_address, user_agent
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
                )
            '''

            await connection.execute(query,
                                     data['pixel'],
                                     data['campaign_id'],
                                     data['adset_id'],
                                     data['ad_id'],
                                     data['campaign_name'],
                                     data['adset_name'],
                                     data['ad_name'],
                                     data['placement'],
                                     data['site_source_name'],
                                     data['fbclid'],
                                     data['unique_identifier'],
                                     data['channel_join_link'],
                                     identifier,
                                     ip_address,
                                     user_agent)
            await connection.close()
        except Exception as error:
            logging.error(f"Error saving to the database: {error}")
            print(f"Error saving to the database: {error}")
            raise

# Ініціалізація ботів з бази даних
async def initialize_bots():
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        rows = await conn.fetch('SELECT * FROM bots')
        await conn.close()
        return [BaseBot(bot['bot_token'], bot['chat_id'], bot['identifiers_table'], bot['user_data_table'], bot['prefix']) for bot in rows]
    except Exception as error:
        logging.error(f"Error initializing bots: {error}")
        print(f"Error initializing bots: {error}")
        raise

# Ініціалізація ботів при запуску додатку
try:
    bots = asyncio.run(initialize_bots())
except Exception as e:
    logging.error(f"Failed to initialize bots on startup: {e}")
    print(f"Failed to initialize bots on startup: {e}")

@app.route('/getlinks')
def generate_identifier():
    try:
        bot_token = request.args.get('bot_token')
        bot = next((b for b in bots if b.bot.token == bot_token), None)

        if bot is None:
            return 'Invalid bot_token', 400

        identifier = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        asyncio.run(bot.insert_identifier(identifier))

        return f'Generated Identifier: {identifier}'
    except Exception as e:
        logging.error(f"Error in generate_identifier: {e}")
        print(f"Error in generate_identifier: {e}")
        return 'Internal Server Error', 500

@app.route('/identifiers')
async def show_identifiers():
    try:
        bot_token = request.args.get('bot_token')
        bot = next((b for b in bots if b.bot.token == bot_token), None)

        if bot is None:
            return 'Invalid bot_token', 400

        identifiers = await bot.get_identifier()
        return render_template('identifiers.html', identifiers=identifiers)
    except Exception as e:
        logging.error(f"Error in show_identifiers: {e}")
        print(f"Error in show_identifiers: {e}")
        return 'Internal Server Error', 500

async def create_telegram_link(bot, unique_identifier):
    try:
        response = await bot.bot.create_chat_invite_link(bot.chat_id, expire_date=timedelta(days=1), creates_join_request=True)
        return response.invite_link
    except Exception as error:
        logging.error(f"Error creating Telegram link: {error}")
        print(f"Error creating Telegram link: {error}")
        raise

@app.route('/<identifier>', methods=['GET'])
async def redirect_to_telegram(identifier):
    try:
        bot_prefix = identifier[:4]

        bot = next((b for b in bots if b.prefix == bot_prefix), None)

        if bot is None:
            return 'Invalid bot_prefix', 400

        identifier_row = await bot.get_identifier(identifier)

        if identifier_row:
            pixel = request.args.get('pixel')
            campaign_id = request.args.get('campaign_id')

            unique_identifier = datetime.now().timestamp()
            channel_join_link = await create_telegram_link(bot, unique_identifier)

            ip_address = request.headers.get('X-Forwarded-For', request.remote_addr)
            user_agent = request.user_agent.string

            await bot.save_to_database({
                'pixel': pixel,
                'campaign_id': campaign_id,
                'adset_id': request.args.get('adset_id'),
                'ad_id': request.args.get('ad_id'),
                'campaign_name': request.args.get('campaign_name'),
                'adset_name': request.args.get('adset_name'),
                'ad_name': request.args.get('ad_name'),
                'placement': request.args.get('placement'),
                'site_source_name': request.args.get('site_source_name'),
                'fbclid': request.args.get('fbclid'),
                'unique_identifier': unique_identifier,
                'channel_join_link': channel_join_link,
            }, identifier, ip_address, user_agent)

            identifier_tg = channel_join_link.split('+').pop()
            redirect_link = f'tg://join?invite={identifier_tg}'

            return redirect(redirect_link, code=302)
        else:
            return 'Identifier not found'
    except Exception as e:
        logging.error(f"Error in redirect_to_telegram: {e}")
        print(f"Error in redirect_to_telegram: {e}")
        return 'Internal Server Error', 500
        
if __name__ == '__main__':
    app.run()
