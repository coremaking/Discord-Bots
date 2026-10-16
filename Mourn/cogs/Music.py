import discord, asyncio, typing
from discord.ext import commands
from utils.paginator import Paginator

color = 0x2b2d31

class Music(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

# WIP

async def setup(bot):
    await bot.add_cog(Music(bot))
