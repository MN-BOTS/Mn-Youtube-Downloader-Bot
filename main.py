import os
import asyncio
from dotenv import load_dotenv
from pyrogram import Client, idle
from aiohttp import web

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

app = Client(
    "MNBoT",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    plugins=dict(root="plugins")
)

async def health_check(request):
    """Simple handler to satisfy Render's port binding check."""
    return web.Response(text="Bot is alive!")

async def start_web_server():
    """Starts a tiny HTTP server on the PORT env variable (default 10000)."""
    port = int(os.environ.get("PORT", 8080))
    web_app = web.Application()
    web_app.router.add_get("/", health_check)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Health check server started on port {port}")

if __name__ == "__main__":
    app.start()
    print("Bot started! this repo was created by mntgxo")
    # Optionally start the health-check server (remove if using Background Worker)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_web_server())
    idle()
    app.stop()
