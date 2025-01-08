from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

ADMINS=[5149506457]
# Initialize bot and dispatcher
bot = Bot(token="6112215933:AAEGOt8jXb1zeIwfpmMkkY7Pa7z17W9MeYQ")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

async def problems(message: str):

    for admin_id in ADMINS:
        try:
            await bot.send_message(chat_id=admin_id, text=message,parse_mode="Markdown")
        except Exception as e:
            print(f"Failed to send message to admin {admin_id}: {e}")

async def filesend(message: str,file_path:str):

    for admin_id in ADMINS:
        try:
            await bot.send_message(chat_id=admin_id, text=message, parse_mode="Markdown")
            with open(file_path, 'rb') as file:
                await bot.send_document(admin_id, document=file)
        except Exception as e:
            print(f"Failed to send message to admin {admin_id}: {e}")