import os, discord, asyncpg
from cogs.VoiceMaster import Buttons
from discord.ext import commands

color = 0x2b2d31

class mournContext(commands.Context):
    async def success(self, text):
        user = self.author
        await self.reply(embed=discord.Embed(description=f'{user.mention}: {text}', color=color), mention_author=False)

    async def warn(self, text):
        user = self.author
        await self.send(embed=discord.Embed(description=f'{user.mention}: {text}', color=color))

class Bot(commands.Bot):
    def __init__(self, *args, prefix=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix

    async def get_context(self, message, *, cls = mournContext):
        return await super().get_context(message, cls = cls)

    async def setup_hook(self):
        self.pool = await asyncpg.create_pool('postgresql://root:Aw49c39a290vg479@localhost:5432/mourn')
        self.db = self.pool
        await self.load_extension('jishaku')
        for file in os.listdir("cogs"):
            if file.endswith(".py"):
                name = file[:-3]
                await self.load_extension(f"cogs.{name}")

    async def close(self):
        await self.db.close()
        await super().close()

    async def on_message(self, msg):
        if (
            not self.is_ready()
            or msg.author.bot
            or msg.guild is None
        ):
            return
        await self.process_commands(msg)
