from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

ADMINS = 5149506457
# Initialize bot and dispatcher
bot = Bot(token="6112215933:AAEGOt8jXb1zeIwfpmMkkY7Pa7z17W9MeYQ")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


async def problems(message: str):
    await bot.send_message(chat_id=ADMINS, text=message)


async def filesend(message: str, file_path: str):
    await bot.send_message(chat_id=ADMINS, text=message)
    with open(file_path, 'rb') as file:
        await bot.send_document(ADMINS, document=file)
