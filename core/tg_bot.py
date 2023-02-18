import json
import logging
from aiogram import Bot, Dispatcher, executor, types

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

with open("misc/keys.json", "r") as f:
    keys = json.load(f)

with open("misc/telegram_ids.json", "r") as f:
    ids = json.load(f)

API_TOKEN = keys['telegram']['private']

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands="start")
async def start(message: types.Message):
    print(f"New member: {message.from_user.id}")
    await message.answer(f"Ты готов? {message.from_user.id}")


async def send_signal(big_message: str):
    for user in ids:
        await bot.send_message(user, big_message)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
