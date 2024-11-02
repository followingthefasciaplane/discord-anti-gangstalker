import os
import sys
import logging
import asyncio
import aiosqlite
import yaml
import time
import discord
from discord.ext import commands, tasks
from typing import List, Dict, Any, Optional
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import io
from collections import defaultdict

# loggin
logging.basicConfig(
    level=logging.DEBUG,  # set to INFO for less sql spam
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('AGS')

# config

DEFAULT_CONFIG = {
    'bot': {
        'prefix': '~',
        # 'token': 'YOUR_BOT_TOKEN_HERE'  # this is just a BOT_TOKEN environment variable now
    },
    'database': {
        'filename': 'member_activity_data.sqlite'
    },
    'guilds': {
        'GUILD_ID_PLACEHOLDER': {
            'channels_to_scan': [],
            'authorized_ids': [],
            'activity_check': {
                'message': "React to this message within 48 hours to confirm your activity.",
                'emoji': '✅',
                'duration_hours': 48
            },
            'rate_limit': {
                'rate': 1,
                'per': 1
            },
            'members_per_page': 10,
            'max_messages_per_channel': 10000,
            'days_to_scan': 30,  # new parameter for time-based scanning
            'ignore_role_id': None
        }
    }
}

class ConfigManager:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._lock = asyncio.Lock()
            # do not call asyncio.create_task here, u will regret it
        return cls._instance

    def _load_config(self) -> None:
        create_default_config()
        self._config = load_config()

    @property
    def config(self) -> Dict[str, Any]:
        return self._config

    def get_guild_config(self, guild_id: int) -> Dict[str, Any]:
        return self._config.get('guilds', {}).get(str(guild_id), {})

    def reload_config(self) -> None:
        with self._lock:
            self._load_config()

def create_default_config() -> None:
    if not os.path.exists('config.yaml'):
        try:
            with open('config.yaml', 'w') as config_file:
                yaml.dump(DEFAULT_CONFIG, config_file, default_flow_style=False)
            logger.info("Default config.yaml created. Please update it with your settings.")
        except Exception as e:
            logger.error(f"Failed to create config.yaml: {e}", exc_info=True)
            raise

def load_config() -> Dict[str, Any]:
    try:
        with open('config.yaml', 'r') as config_file:
            config = yaml.safe_load(config_file)
        # validation of configuration
        if not config.get('bot', {}).get('prefix'):
            raise ValueError("Bot prefix not set in config.yaml.")

        # ensure required fields are present
        if 'database' not in config or 'filename' not in config['database']:
            raise ValueError("Database filename not set in config.yaml.")

        return config
    except Exception as e:
        logger.error(f"Error loading config.yaml: {e}", exc_info=True)
        raise

# db

class MemberData:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def initialize(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS members (
                    guild_id TEXT,
                    member_id TEXT,
                    message_count INTEGER,
                    join_date TEXT,
                    displayname TEXT,
                    username TEXT,
                    first_message_date TEXT,
                    last_message_date TEXT,
                    category TEXT,
                    left INTEGER DEFAULT 0,
                    PRIMARY KEY (guild_id, member_id)
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS activity_checks (
                    guild_id TEXT PRIMARY KEY,
                    message_id TEXT,
                    channel_id TEXT,
                    end_time TEXT
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS reactions (
                    guild_id TEXT,
                    user_id TEXT,
                    reacted INTEGER,
                    PRIMARY KEY (guild_id, user_id)
                )
            ''')
            # create indexes, could maybe do some more here but it should be quick as is
            await db.execute('CREATE INDEX IF NOT EXISTS idx_members_guild_member ON members (guild_id, member_id)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_members_category ON members (guild_id, category)')
            await db.execute('CREATE INDEX IF NOT EXISTS idx_reactions_guild_user ON reactions (guild_id, user_id)')
            await db.commit()

    @asynccontextmanager
    async def get_db(self):
        try:
            async with aiosqlite.connect(self.db_path) as db:
                yield db
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            raise

    async def update_member(self, guild_id: str, member_id: str, data: Dict[str, Any]) -> None:
        async with self.get_db() as conn:
            await conn.execute('''
                INSERT OR REPLACE INTO members 
                (guild_id, member_id, message_count, join_date, displayname, username, 
                first_message_date, last_message_date, category, left)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                guild_id, member_id, data['message_count'], data['join_date'],
                data['displayname'], data['username'], data['first_message_date'],
                data['last_message_date'], data['category'], data.get('left', 0)
            ))
            await conn.commit()

    async def get_member(self, guild_id: str, member_id: str) -> Optional[Dict[str, Any]]:
        async with self.get_db() as conn:
            async with conn.execute('''
                SELECT * FROM members WHERE guild_id = ? AND member_id = ?
            ''', (guild_id, member_id)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(zip([col[0] for col in cursor.description], row))
        return None

    async def get_members_by_category(self, guild_id: str, category: str) -> List[Dict[str, Any]]:
        async with self.get_db() as conn:
            async with conn.execute('''
                SELECT * FROM members 
                WHERE guild_id = ? AND category = ?
                ORDER BY message_count DESC
            ''', (guild_id, category)) as cursor:
                rows = await cursor.fetchall()
                return [dict(zip([col[0] for col in cursor.description], row)) for row in rows]

    async def get_all_members(self, guild_id: str) -> List[Dict[str, Any]]:
        async with self.get_db() as conn:
            async with conn.execute('''
                SELECT * FROM members WHERE guild_id = ?
            ''', (guild_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(zip([col[0] for col in cursor.description], row)) for row in rows]

    async def batch_update_members(self, guild_id: str, member_data_list: List[Dict[str, Any]]) -> None:
        async with self.get_db() as conn:
            await conn.executemany('''
                INSERT OR REPLACE INTO members 
                (guild_id, member_id, message_count, join_date, displayname, username,
                first_message_date, last_message_date, category, left)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (guild_id, data['member_id'], data['message_count'], data['join_date'],
                 data['displayname'], data['username'], data['first_message_date'],
                 data['last_message_date'], data['category'], data.get('left', 0))
                for data in member_data_list
            ])
            await conn.commit()

class ActivityCheckData:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def get_db(self):
        try:
            async with aiosqlite.connect(self.db_path) as db:
                yield db
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            raise

    async def save_activity_check(self, guild_id: str, message_id: str,
                                channel_id: str, end_time: str) -> None:
        async with self.get_db() as conn:
            await conn.execute('''
                INSERT OR REPLACE INTO activity_checks 
                (guild_id, message_id, channel_id, end_time)
                VALUES (?, ?, ?, ?)
            ''', (guild_id, message_id, channel_id, end_time))
            await conn.commit()

    async def get_activity_check(self, guild_id: str) -> Optional[Dict[str, Any]]:
        async with self.get_db() as conn:
            async with conn.execute('''
                SELECT * FROM activity_checks WHERE guild_id = ?
            ''', (guild_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(zip([col[0] for col in cursor.description], row))
        return None

    async def delete_activity_check(self, guild_id: str) -> None:
        async with self.get_db() as conn:
            await conn.execute('DELETE FROM activity_checks WHERE guild_id = ?', (guild_id,))
            await conn.commit()

class ReactionData:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def get_db(self):
        try:
            async with aiosqlite.connect(self.db_path) as db:
                yield db
        except Exception as e:
            logger.error(f"Database error: {e}", exc_info=True)
            raise

    async def save_reaction(self, guild_id: str, user_id: str, reacted: int) -> None:
        async with self.get_db() as conn:
            await conn.execute('''
                INSERT OR REPLACE INTO reactions (guild_id, user_id, reacted)
                VALUES (?, ?, ?)
            ''', (guild_id, user_id, reacted))
            await conn.commit()

    async def get_reacted_users(self, guild_id: str) -> List[str]:
        async with self.get_db() as conn:
            async with conn.execute('''
                SELECT user_id FROM reactions WHERE guild_id = ? AND reacted = 1
            ''', (guild_id,)) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def delete_reactions(self, guild_id: str) -> None:
        async with self.get_db() as conn:
            await conn.execute('DELETE FROM reactions WHERE guild_id = ?', (guild_id,))
            await conn.commit()

# util

class RateLimiter:
    def __init__(self, rate: int, per: float):
        self.rate = rate
        self.per = per
        self.tokens = rate
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.updated_at = now
            self.tokens += elapsed * (self.rate / self.per)
            if self.tokens > self.rate:
                self.tokens = self.rate
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) * (self.per / self.rate)
                await asyncio.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1

@dataclass
class PerformanceMetrics:
    operation: str
    duration: float
    success: bool
    error: Optional[str] = None

class PerformanceMonitor:
    def __init__(self, enabled: bool = False):
        self.metrics: List[PerformanceMetrics] = []
        self.enabled = enabled

    @asynccontextmanager
    async def measure(self, operation: str):
        if not self.enabled:
            yield
            return
        start = time.perf_counter()
        try:
            yield
            self.metrics.append(PerformanceMetrics(
                operation=operation,
                duration=time.perf_counter() - start,
                success=True
            ))
        except Exception as e:
            self.metrics.append(PerformanceMetrics(
                operation=operation,
                duration=time.perf_counter() - start,
                success=False,
                error=str(e)
            ))
            raise

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()

    def clear_metrics(self) -> None:
        self.metrics.clear()

class MessageHandler:
    def __init__(self, db_path: str, config_manager):
        self.db_path = db_path
        self.config_manager = config_manager
        self.performance_monitor = PerformanceMonitor(enabled=False)  # disabled by default because its not working yet
        self.message_cache: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self.batch_size = 100  # number of messages before batch update
        self.background_task = None

    async def start_background_task(self):
        self.background_task = tasks.loop(seconds=60)(self.flush_cache)
        self.background_task.start()

    async def process_message(self, message: discord.Message) -> None:
        if message.author.bot or not isinstance(message.channel, discord.TextChannel):
            return

        guild_id = str(message.guild.id)
        member_id = str(message.author.id)

        async with self.performance_monitor.measure(f"process_message_{guild_id}_{member_id}"):
            guild_config = self.config_manager.get_guild_config(int(guild_id))
            if not guild_config:
                return

            if self.member_has_ignore_role(message.author, guild_config):
                return

            async with self.lock:
                member_data = self.message_cache.get(member_id)
                message_time = message.created_at.isoformat()

                if member_data:
                    member_data['message_count'] += 1
                    if message_time < member_data['first_message_date']:
                        member_data['first_message_date'] = message_time
                    if message_time > member_data['last_message_date']:
                        member_data['last_message_date'] = message_time
                else:
                    member_data = {
                        'guild_id': guild_id,
                        'member_id': member_id,
                        'message_count': 1,
                        'join_date': message.author.joined_at.isoformat() if message.author.joined_at else None,
                        'displayname': message.author.display_name,
                        'username': str(message.author),
                        'first_message_date': message_time,
                        'last_message_date': message_time,
                        'category': '',
                        'left': 0
                    }
                self.message_cache[member_id] = member_data

                # flush cache if batch size is reached
                if len(self.message_cache) >= self.batch_size:
                    await self.flush_cache()

    async def flush_cache(self):
        async with self.lock:
            if not self.message_cache:
                return
            member_data_list = list(self.message_cache.values())
            member_data = MemberData(self.db_path)
            await member_data.batch_update_members(member_data_list[0]['guild_id'], member_data_list)
            self.message_cache.clear()
            logger.info(f"Flushed {len(member_data_list)} member records to the database.")

    @staticmethod
    def member_has_ignore_role(member: discord.Member, guild_config: Dict[str, Any]) -> bool:
        ignore_role_id = guild_config.get('ignore_role_id')
        if ignore_role_id:
            try:
                ignore_role_id = int(ignore_role_id)
                role = member.guild.get_role(ignore_role_id)
                return role and role in member.roles
            except ValueError:
                return False
        return False

class PaginationHandler:
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def send_paginated_embed(self, ctx: commands.Context,
                                   items: List[Any],
                                   title: str,
                                   items_per_page: int,
                                   format_item_func) -> None:
        """
        Sends a paginated embed message in the channel where the command was issued.
        """
        if not items:
            await ctx.send(f"No data to display for {title}.")
            return

        pages = [items[i:i + items_per_page] for i in range(0, len(items), items_per_page)]
        current_page = 0

        def create_embed(page_items: List[Any]) -> discord.Embed:
            embed = discord.Embed(
                title=f"{title} (Page {current_page + 1}/{len(pages)})",
                color=discord.Color.blue()
            )
            for item in page_items:
                name, value = format_item_func(item)
                embed.add_field(name=name, value=value, inline=False)
            embed.set_footer(text="Use ⬅️ ➡️ to navigate • Session expires in 60 seconds")
            return embed

        # send the message in the channel where command was issued
        message = await ctx.send(embed=create_embed(pages[current_page]))

        # page stuff
        if len(pages) > 1:
            await message.add_reaction('⬅️')
            await message.add_reaction('➡️')

            def check(reaction, user):
                return (
                    user == ctx.author and
                    str(reaction.emoji) in ['⬅️', '➡️'] and
                    reaction.message.id == message.id
                )

            while True:
                try:
                    reaction, user = await self.bot.wait_for(
                        'reaction_add',
                        timeout=60.0,
                        check=check
                    )

                    if str(reaction.emoji) == '➡️' and current_page < len(pages) - 1:
                        current_page += 1
                        await message.edit(embed=create_embed(pages[current_page]))
                    elif str(reaction.emoji) == '⬅️' and current_page > 0:
                        current_page -= 1
                        await message.edit(embed=create_embed(pages[current_page]))

                    await message.remove_reaction(reaction, user)

                except asyncio.TimeoutError:
                    embed = create_embed(pages[current_page])
                    embed.set_footer(text=f"Page {current_page + 1}/{len(pages)} • Session expired")
                    await message.edit(embed=embed)
                    await message.clear_reactions()
                    break

def format_duration(seconds: float) -> str:
    """Formats a duration in seconds into a human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.1f}h"

def format_timestamp(timestamp: Optional[str]) -> str:
    """Formats an ISO timestamp into a human-readable string."""
    if not timestamp:
        return "Never"
    try:
        dt = datetime.fromisoformat(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return "Invalid timestamp"

# activity check stuff

class ActivityCheckManager:
    def __init__(self, bot: commands.Bot, db_path: str, config_manager):
        self.bot = bot
        self.db_path = db_path
        self.config_manager = config_manager
        self.activity_data = ActivityCheckData(db_path)
        self.reaction_data = ReactionData(db_path)
        self.member_data = MemberData(db_path)
        self.performance_monitor = PerformanceMonitor(enabled=False)
        self.pagination = PaginationHandler(bot)
        self.rate_limiter = RateLimiter(rate=1, per=1)

    async def start_check(self, ctx: commands.Context, hours: int = 48) -> None:
        """Starts an activity check in the given context."""
        guild_id = str(ctx.guild.id)

        async with self.performance_monitor.measure(f"start_activity_check_{guild_id}"):
            guild_config = self.config_manager.get_guild_config(ctx.guild.id)
            if not guild_config:
                await ctx.send("Guild configuration not found.")
                return

            if hours <= 0:
                await ctx.send("Duration must be positive.")
                return

            existing_check = await self.activity_data.get_activity_check(guild_id)
            if existing_check:
                await ctx.send("An activity check is already in progress.")
                return

            try:
                end_time = datetime.now(timezone.utc) + timedelta(hours=hours)
                message = await ctx.send(guild_config['activity_check']['message'])
                await message.add_reaction(guild_config['activity_check']['emoji'])

                await self.activity_data.save_activity_check(
                    guild_id,
                    str(message.id),
                    str(ctx.channel.id),
                    end_time.isoformat()
                )

                await ctx.send(f"Activity check started. Ends in {hours} hours.")

            except Exception as e:
                logger.error(f"Error starting activity check: {e}", exc_info=True)
                await ctx.send("Failed to start activity check.")
                raise

    async def process_check(self, ctx: commands.Context, force: bool = False) -> None:
        """Processes an ongoing activity check."""
        guild_id = str(ctx.guild.id)

        async with self.performance_monitor.measure(f"process_activity_check_{guild_id}"):
            guild_config = self.config_manager.get_guild_config(ctx.guild.id)
            if not guild_config:
                await ctx.send("Guild configuration not found.")
                return

            check_data = await self.activity_data.get_activity_check(guild_id)
            if not check_data:
                await ctx.send("No active check found.")
                return

            end_time = datetime.fromisoformat(check_data['end_time'])
            if datetime.now(timezone.utc) < end_time and not force:
                await ctx.send("Check still ongoing.")
                return

            try:
                # process member reactions and categorize
                await self._categorize_members(ctx.guild, check_data)

                # clean up
                await self.activity_data.delete_activity_check(guild_id)
                await self.reaction_data.delete_reactions(guild_id)

                await ctx.send("Activity check processed successfully.")

            except Exception as e:
                logger.error(f"Error processing activity check: {e}", exc_info=True)
                await ctx.send("Failed to process activity check.")
                raise

    async def _categorize_members(self, guild: discord.Guild, check_data: Dict[str, Any]) -> None:
        """Categorizes members based on their activity and reactions."""
        guild_id = str(guild.id)
        guild_config = self.config_manager.get_guild_config(guild.id)

        # get all data
        all_members = await self.member_data.get_all_members(guild_id)
        reacted_users = set(await self.reaction_data.get_reacted_users(guild_id))

        batch_updates = []

        for member_data in all_members:
            member_id = member_data['member_id']
            member = guild.get_member(int(member_id))

            if not member or member.bot:
                continue

            if self.member_has_ignore_role(member, guild_config):
                continue

            # determine category
            if member_id in reacted_users:
                category = 'lurkers' if member_data['message_count'] == 0 else 'active'
            else:
                category = 'inactive'

            # prepare batch
            member_data['category'] = category
            batch_updates.append(member_data)

            # rate limit
            await self.rate_limiter.wait()

        # now slam that bitch
        if batch_updates:
            await self.member_data.batch_update_members(guild_id, batch_updates)

    @staticmethod
    def member_has_ignore_role(member: discord.Member, guild_config: Dict[str, Any]) -> bool:
        """Checks if a member has the ignore role."""
        ignore_role_id = guild_config.get('ignore_role_id')
        if ignore_role_id:
            try:
                ignore_role_id = int(ignore_role_id)
                role = member.guild.get_role(ignore_role_id)
                return role and role in member.roles
            except ValueError:
                return False
        return False

# main bot stuff

class AGSBot(commands.Bot):
    def __init__(self):
        self.config_manager = ConfigManager()
        intents = discord.Intents(
            guilds=True,
            members=True,
            messages=True,
            reactions=True,
            message_content=True
        )
        super().__init__(
            command_prefix=self.get_prefix,  # yes this needs to be callable, dont try to change it
            intents=intents
        )
        self.token = os.getenv('BOT_TOKEN')
        if not self.token:
            logger.error("Bot token not found in environment variables.")
            sys.exit(1)

        # these dgaf about config.. do not set config stuff up here
        self.performance_monitor = PerformanceMonitor(enabled=False)
        self.rate_limiter = RateLimiter(rate=1, per=1)

        # get rid of the default one
        self.remove_command('help')

        # load command checks
        self.add_check(self.guild_only_check)

        # setup reaction locks
        self.reaction_locks: Dict[str, asyncio.Lock] = {}

    async def get_prefix(self, message):
        if not self.config_manager.config:
            return '~'  # default prefix

        if message and message.guild:
            guild_id = message.guild.id
            guild_config = self.config_manager.get_guild_config(guild_id)
            if guild_config:
                return guild_config.get('bot', {}).get('prefix', '~')
        else:
            # for DMs or messages without a guild, return a default prefix, but this is still kinda dodgy
            return '~'

        # fallback to default
        return self.config_manager.config['bot']['prefix']


    async def setup_hook(self) -> None:
        """Initialize bot components during startup."""
        try:
            # load the config
            await self.loop.run_in_executor(None, self.config_manager._load_config)
            config = self.config_manager.config

            # now you can set your config stuff
            self.db_path = config['database']['filename']
            self.member_data = MemberData(self.db_path)
            await self.member_data.initialize()
            self.activity_data = ActivityCheckData(self.db_path)
            self.reaction_data = ReactionData(self.db_path)
            self.message_handler = MessageHandler(self.db_path, self.config_manager)
            await self.message_handler.start_background_task()
            self.activity_manager = ActivityCheckManager(self, self.db_path, self.config_manager)
            self.pagination = PaginationHandler(self)

            # load any active activity checks
            await self.load_active_checks()

            # add commands
            self.add_commands()

            # events
            self.event(self.on_ready)
            self.event(self.on_message)
            self.event(self.on_member_join)
            self.event(self.on_member_remove)
            self.event(self.on_raw_reaction_add)
            self.event(self.on_raw_reaction_remove)
            self.event(self.on_command_error)

        except Exception as e:
            logger.error(f"Error during bot setup: {e}", exc_info=True)
            await self.close()


    async def guild_only_check(self, ctx: commands.Context) -> bool:
        """Ensures commands are only used in guilds."""
        if not ctx.guild:
            await ctx.send("This command cannot be used in DMs.")
            return False
        return True

    def is_authorized(self) -> commands.check:
        """Check if user is authorized to use admin commands."""
        async def predicate(ctx: commands.Context) -> bool:
            guild_config = self.config_manager.get_guild_config(ctx.guild.id)
            if not guild_config:
                await ctx.send("Guild configuration not found.")
                return False

            authorized_ids = guild_config.get('authorized_ids', [])
            if ctx.author.id not in authorized_ids and not ctx.author.guild_permissions.administrator:
                await ctx.send("You are not authorized to use this command.")
                return False
            return True
        return commands.check(predicate)


    def add_commands(self) -> None:
        """Register bot commands."""
        @self.command(name='scan_members')
        @self.is_authorized()
        async def scan_members(ctx: commands.Context):
            """Scans and updates member activity data."""
            asyncio.create_task(self.cmd_scan_members(ctx))
            await ctx.send("🔄 Member scan started in the background. You will be notified upon completion.")

        @self.command(name='start_activity_check')
        @self.is_authorized()
        async def start_activity_check(ctx: commands.Context, hours: int = 48):
            """Starts an activity check."""
            await self.activity_manager.start_check(ctx, hours)

        @self.command(name='process_activity_check')
        @self.is_authorized()
        async def process_activity_check(ctx: commands.Context, *, force: str = ''):
            """Processes the current activity check."""
            await self.activity_manager.process_check(ctx, force.lower() == 'force')

        @self.command(name='show_category')
        @self.is_authorized()
        async def show_category(ctx: commands.Context, category: str):
            """Shows members in a specific category."""
            await self.cmd_show_category(ctx, category)

        @self.command(name='query_user')
        @self.is_authorized()
        async def query_user(ctx: commands.Context, member: discord.Member):
            """Shows detailed information about a user."""
            await self.cmd_query_user(ctx, member)

        @self.command(name='help')
        async def help(ctx: commands.Context):
            """Shows help information."""
            await self.cmd_help(ctx)

        @self.command(name='update_categories')
        @self.is_authorized()
        async def update_categories(ctx: commands.Context):
            """Updates member categories based on activity."""
            await self.cmd_update_categories(ctx)

        @self.command(name='kick_inactive')
        @self.is_authorized()
        async def kick_inactive(ctx: commands.Context):
            """Kicks inactive members."""
            await self.cmd_kick_inactive(ctx)

        @self.command(name='export')
        @self.is_authorized()
        async def export_data(ctx: commands.Context):
            """Exports member data to CSV."""
            await self.cmd_export_data(ctx)

    async def load_active_checks(self) -> None:
        """Load and synchronize active checks during startup."""
        for guild in self.guilds:
            guild_id = str(guild.id)
            check_data = await self.activity_data.get_activity_check(guild_id)

            if check_data:
                try:
                    channel = self.get_channel(int(check_data['channel_id']))
                    if channel:
                        message = await channel.fetch_message(int(check_data['message_id']))
                        await self._sync_reactions(guild_id, message)
                except Exception as e:
                    logger.error(f"Error loading activity check for guild {guild_id}: {e}", exc_info=True)

    async def _sync_reactions(self, guild_id: str, message: discord.Message) -> None:
        """Synchronize reaction data with message reactions."""
        guild_config = self.config_manager.get_guild_config(int(guild_id))
        if not guild_config:
            return

        reaction_emoji = guild_config['activity_check']['emoji']
        await self.reaction_data.delete_reactions(guild_id)

        for reaction in message.reactions:
            if str(reaction.emoji) == reaction_emoji:
                async for user in reaction.users():
                    if not user.bot:
                        await self.reaction_data.save_reaction(guild_id, str(user.id), 1)

    # event handlers
    async def on_ready(self) -> None:
        """Handle bot ready event."""
        logger.info(f'{self.user} has connected to Discord!')

        # told u this needed to be callable
        prefix = await self.get_prefix(None)

        # custom status
        activity = discord.Game(name=f"{prefix}help")
        await self.change_presence(activity=activity)

        logger.info(f"Status set to '{prefix}help'")


    async def on_message(self, message: discord.Message) -> None:
        """Handle message events."""
        if message.author == self.user:
            return

        await self.message_handler.process_message(message)
        await self.process_commands(message)

    async def on_member_join(self, member: discord.Member) -> None:
        """Handle member join events."""
        if member.bot:
            return

        guild_id = str(member.guild.id)
        member_id = str(member.id)

        member_data = await self.member_data.get_member(guild_id, member_id)
        if member_data:
            member_data['left'] = 0
            member_data['join_date'] = member.joined_at.isoformat()
        else:
            member_data = {
                'member_id': member_id,
                'message_count': 0,
                'join_date': member.joined_at.isoformat(),
                'displayname': member.display_name,
                'username': str(member),
                'first_message_date': None,
                'last_message_date': None,
                'category': '',
                'left': 0
            }

        await self.member_data.update_member(guild_id, member_id, member_data)

    async def on_member_remove(self, member: discord.Member) -> None:
        """Handle member remove events."""
        if member.bot:
            return

        guild_id = str(member.guild.id)
        member_id = str(member.id)

        member_data = await self.member_data.get_member(guild_id, member_id)
        if member_data:
            member_data['left'] = 1
            await self.member_data.update_member(guild_id, member_id, member_data)

    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent) -> None:
        """Handle raw reaction add events."""
        if payload.user_id == self.user.id:
            return
        guild = self.get_guild(payload.guild_id)
        if not guild:
            return

        guild_id = str(payload.guild_id)
        guild_config = self.config_manager.get_guild_config(payload.guild_id)
        if not guild_config:
            return

        check_data = await self.activity_data.get_activity_check(guild_id)
        if (check_data and
            str(payload.message_id) == check_data['message_id'] and
            str(payload.emoji.name) == guild_config['activity_check']['emoji']):

            if guild_id not in self.reaction_locks:
                self.reaction_locks[guild_id] = asyncio.Lock()

            async with self.reaction_locks[guild_id]:
                await self.reaction_data.save_reaction(guild_id, str(payload.user_id), 1)

    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent) -> None:
        """Handle raw reaction remove events."""
        if payload.user_id == self.user.id:
            return
        guild = self.get_guild(payload.guild_id)
        if not guild:
            return

        guild_id = str(payload.guild_id)
        guild_config = self.config_manager.get_guild_config(payload.guild_id)
        if not guild_config:
            return

        check_data = await self.activity_data.get_activity_check(guild_id)
        if (check_data and
            str(payload.message_id) == check_data['message_id'] and
            str(payload.emoji.name) == guild_config['activity_check']['emoji']):

            if guild_id not in self.reaction_locks:
                self.reaction_locks[guild_id] = asyncio.Lock()

            async with self.reaction_locks[guild_id]:
                await self.reaction_data.save_reaction(guild_id, str(payload.user_id), 0)

    async def on_command_error(self, ctx: commands.Context, error: Exception) -> None:
        """Handle command errors."""
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(f"Command on cooldown. Try again in {error.retry_after:.1f}s")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing required argument: {error.param}")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument provided: {error}")
        elif isinstance(error, commands.CommandNotFound):
            pass  # dont think this needs anything else
        elif isinstance(error, commands.CheckFailure):
            pass  # we dont do auth errors here so dw about it
        else:
            logger.error(f"Unhandled error in command {ctx.command}: {error}", exc_info=True)
            await ctx.send("An unexpected error occurred. Please try again later.")

    # commands

    async def cmd_scan_members(self, ctx: commands.Context) -> None:
        """Scan and update member activity data."""
        guild_id = str(ctx.guild.id)
        guild_config = self.config_manager.get_guild_config(ctx.guild.id)

        if not guild_config:
            await ctx.send("❌ Guild configuration not found.")
            return

        await ctx.send("🔄 Starting member scan...")

        channels_to_scan_ids = guild_config.get('channels_to_scan', [])
        days_to_scan = guild_config.get('days_to_scan', 30)
        time_threshold = datetime.now(timezone.utc) - timedelta(days=days_to_scan)
        max_messages_per_channel = guild_config.get('max_messages_per_channel', 10000)
        concurrency_limit = guild_config.get('concurrency_limit', 5)  # how many concurrent channels we can scan

        # get channel objects
        channels_to_scan = []
        for channel_id in channels_to_scan_ids:
            channel = ctx.guild.get_channel(int(channel_id))
            if isinstance(channel, discord.TextChannel):
                channels_to_scan.append(channel)

        total_channels = len(channels_to_scan)
        message_counts = defaultdict(int)
        first_message_dates = {}
        last_message_dates = {}

        # use a semaphore for this... it seems to work anyway but theres probs a better way to do it
        semaphore = asyncio.Semaphore(concurrency_limit)

        # progress tracking stuff
        progress_message = await ctx.send(f"🔄 Scanning {total_channels} channels...")
        channels_processed = 0
        last_progress_update = time.time()

        async def process_channel(channel):
            nonlocal channels_processed, last_progress_update
            async with semaphore:
                try:
                    async for message in channel.history(
                        after=time_threshold,
                        oldest_first=True,
                        limit=max_messages_per_channel
                    ):
                        if message.author.bot:
                            continue

                        member_id = str(message.author.id)
                        message_counts[member_id] += 1
                        message_time = message.created_at.isoformat()

                        # update first and last message dates
                        if member_id not in first_message_dates or message_time < first_message_dates[member_id]:
                            first_message_dates[member_id] = message_time
                        if member_id not in last_message_dates or message_time > last_message_dates[member_id]:
                            last_message_dates[member_id] = message_time

                    logger.info(f"Scanned channel {channel.name}")

                except discord.Forbidden:
                    logger.warning(f"Cannot access channel {channel.name}")
                except Exception as e:
                    logger.error(f"Error scanning channel {channel.id}: {e}", exc_info=True)
                finally:
                    channels_processed += 1
                    # update progress every 5 seconds
                    if time.time() - last_progress_update > 5:
                        await progress_message.edit(
                            content=f"🔄 Scanning channels... ({channels_processed}/{total_channels} completed)"
                        )
                        last_progress_update = time.time()

        # start processing channels
        tasks = [asyncio.create_task(process_channel(channel)) for channel in channels_to_scan]

        await asyncio.gather(*tasks)

        # final progress update
        await progress_message.edit(
            content=f"🔄 Scanning channels... ({channels_processed}/{total_channels} completed)"
        )

        # so i do kind of want to make categorization one in the same eventually but for now, i would like to keep these two seperate so i dont end up with more impossible sql headaches
        existing_members = await self.member_data.get_all_members(guild_id)
        member_data_dict = {member['member_id']: member for member in existing_members}

        for member_id, count in message_counts.items():
            member = ctx.guild.get_member(int(member_id))
            if not member:
                continue

            member_data = member_data_dict.get(member_id, {
                'guild_id': guild_id,
                'member_id': member_id,
                'message_count': 0,
                'join_date': member.joined_at.isoformat() if member.joined_at else None,
                'displayname': member.display_name,
                'username': str(member),
                'first_message_date': None,
                'last_message_date': None,
                'category': '',
                'left': 0
            })

            member_data['message_count'] += count
            member_data['displayname'] = member.display_name
            member_data['username'] = str(member)
            # update first and last message dates if they are earlier or later
            if not member_data['first_message_date'] or first_message_dates[member_id] < member_data['first_message_date']:
                member_data['first_message_date'] = first_message_dates[member_id]
            if not member_data['last_message_date'] or last_message_dates[member_id] > member_data['last_message_date']:
                member_data['last_message_date'] = last_message_dates[member_id]
            member_data_dict[member_id] = member_data

        # now slam that shit 
        if member_data_dict:
            await self.member_data.batch_update_members(guild_id, list(member_data_dict.values()))
            await ctx.send(f"✅ Scan complete. Processed {len(member_data_dict)} members.")
        else:
            await ctx.send("❌ No member data found during scan.")


    async def cmd_show_category(self, ctx: commands.Context, category: str) -> None:
        """Shows members in a specific category in the channel where command was issued."""
        category = category.lower()
        valid_categories = ['lurkers', 'unreacters', 'inactive', 'active', 'members']

        if category not in valid_categories:
            await ctx.send(f"❌ Invalid category. Valid categories: {', '.join(valid_categories)}")
            return

        guild_id = str(ctx.guild.id)
        guild_config = self.config_manager.get_guild_config(ctx.guild.id)

        if not guild_config:
            await ctx.send("❌ Guild configuration not found.")
            return

        if category == 'members':
            members = await self.member_data.get_all_members(guild_id)
        else:
            members = await self.member_data.get_members_by_category(guild_id, category)
        members_per_page = guild_config.get('members_per_page', 10)

        def format_member(member_data):
            name = member_data['displayname']
            value = (
                f"Messages: {member_data['message_count']}\n"
                f"Join Date: {format_timestamp(member_data['join_date'])}\n"
                f"First Message: {format_timestamp(member_data['first_message_date'])}\n"
                f"Last Message: {format_timestamp(member_data['last_message_date'])}\n"
                f"Category: {member_data['category']}"
            )
            return name, value

        await self.pagination.send_paginated_embed(
            ctx,
            members,
            f"{category.capitalize()} Members",
            members_per_page,
            format_member
        )

    async def cmd_query_user(self, ctx: commands.Context, member: discord.Member) -> None:
        """Shows detailed information about a user."""
        guild_id = str(ctx.guild.id)
        member_id = str(member.id)

        member_data = await self.member_data.get_member(guild_id, member_id)
        if not member_data:
            await ctx.send(f"❌ No data found for {member.display_name}")
            return

        embed = discord.Embed(
            title=f"User Information: {member.display_name}",
            color=discord.Color.blue()
        )

        # basic info.. to expand ig
        embed.add_field(
            name="User Details",
            value=(
                f"Display Name: {member_data['displayname']}\n"
                f"Username: {member_data['username']}\n"
                f"ID: {member_id}\n"
                f"Join Date: {format_timestamp(member_data['join_date'])}\n"
                f"Category: {member_data['category']}"
            ),
            inline=False
        )

        # activity
        embed.add_field(
            name="Activity Statistics",
            value=(
                f"Message Count: {member_data['message_count']}\n"
                f"First Message: {format_timestamp(member_data['first_message_date'])}\n"
                f"Last Message: {format_timestamp(member_data['last_message_date'])}"
            ),
            inline=False
        )

        # roles
        roles = [role.mention for role in member.roles[1:]]  # skip @everyone
        embed.add_field(
            name="Current Roles",
            value=", ".join(roles) if roles else "No roles",
            inline=False
        )

        await ctx.send(embed=embed)

    async def cmd_help(self, ctx: commands.Context) -> None:
        """Shows help information on a single page."""
        # get the prefix - handle both string and callable cases, this breaks VERY EASILY SO be careful
        prefix = await self.get_prefix(ctx.message)
        if isinstance(prefix, list):
            prefix = prefix[0]  # take the first prefix if it's a list
        
        guild_config = self.config_manager.get_guild_config(ctx.guild.id) if ctx.guild else None
        is_admin = ctx.guild and (
            ctx.author.guild_permissions.administrator or
            ctx.author.id in guild_config.get('authorized_ids', [])
        )

        # create the embed
        embed = discord.Embed(
            title="AGS Bot Commands",
            description="Activity and Guild Statistics Bot",
            color=discord.Color.blue()
        )

        # add general commands - always visible
        general_commands = {
            "help": "Show this help message",
        }
        cmd_text = "\n".join(f"`{prefix}{cmd}`: {desc}" for cmd, desc in general_commands.items())
        embed.add_field(
            name="📝 General Commands",
            value=cmd_text,
            inline=False
        )

        # add admin commands if user has permission
        if is_admin:
            # activity monitoring commands
            monitoring_commands = {
                "scan_members": "Scan messages in specified channels",
                "query_user @member": "Show detailed user information",
                "show_category <category>": "Show members in category",
            }
            cmd_text = "\n".join(f"`{prefix}{cmd}`: {desc}" for cmd, desc in monitoring_commands.items())
            embed.add_field(
                name="🔍 Activity Monitoring",
                value=cmd_text,
                inline=False
            )

            # activity management commands
            management_commands = {
                "start_activity_check [hours]": "Start an activity check (default: 48h)",
                "process_activity_check [force]": "Process current activity check",
                "update_categories": "Update member categories",
            }
            cmd_text = "\n".join(f"`{prefix}{cmd}`: {desc}" for cmd, desc in management_commands.items())
            embed.add_field(
                name="⚙️ Activity Management",
                value=cmd_text,
                inline=False
            )

            # member management commands
            member_commands = {
                "kick_inactive": "Kick inactive members",
                "export": "Export member data to CSV",
            }
            cmd_text = "\n".join(f"`{prefix}{cmd}`: {desc}" for cmd, desc in member_commands.items())
            embed.add_field(
                name="👥 Member Management",
                value=cmd_text,
                inline=False
            )

        # footer for perms indicator
        embed.set_footer(text=f"{'✅ You have admin access' if is_admin else 'ℹ️ You have regular access'}")

        # send it
        await ctx.send(embed=embed)

    async def cmd_update_categories(self, ctx: commands.Context) -> None:
        """Update member categories based on activity."""
        guild_id = str(ctx.guild.id)
        guild_config = self.config_manager.get_guild_config(ctx.guild.id)

        if not guild_config:
            await ctx.send("❌ Guild configuration not found.")
            return

        await ctx.send("🔄 Updating member categories...")

        try:
            # get current guild members
            guild_members = ctx.guild.members
            current_member_ids = {str(member.id) for member in guild_members if not member.bot}

            # get db members
            db_members = await self.member_data.get_all_members(guild_id)
            db_member_dict = {member['member_id']: member for member in db_members}

            updates = []
            stats = {
                'active': 0,
                'lurkers': 0,
                'inactive': 0,
                'updated': 0,
                'new': 0
            }

            # process current members
            for member in guild_members:
                if member.bot:
                    continue

                member_id = str(member.id)

                # skip members with ignore role
                if self.activity_manager.member_has_ignore_role(member, guild_config):
                    continue

                member_data = db_member_dict.get(member_id, {
                    'member_id': member_id,
                    'message_count': 0,
                    'join_date': member.joined_at.isoformat() if member.joined_at else None,
                    'displayname': member.display_name,
                    'username': str(member),
                    'first_message_date': None,
                    'last_message_date': None,
                    'left': 0
                })

                # determine category based on activity
                if member_data['message_count'] == 0:
                    category = 'lurkers'
                    stats['lurkers'] += 1
                else:
                    category = 'active'
                    stats['active'] += 1

                member_data['category'] = category
                member_data['left'] = 0  # ensure current members are marked as not left
                updates.append(member_data)

                if member_id in db_member_dict:
                    stats['updated'] += 1
                else:
                    stats['new'] += 1

            # process left members
            for member_id, member_data in db_member_dict.items():
                if member_id not in current_member_ids:
                    member_data['category'] = 'inactive'
                    member_data['left'] = 1
                    updates.append(member_data)
                    stats['inactive'] += 1

            # slam that shit again
            await self.member_data.batch_update_members(guild_id, updates)

            # let the user know about it
            embed = discord.Embed(
                title="✅ Category Update Complete",
                color=discord.Color.green(),
                timestamp=datetime.now()
            )

            embed.add_field(name="Active Members", value=stats['active'], inline=True)
            embed.add_field(name="Lurkers", value=stats['lurkers'], inline=True)
            embed.add_field(name="Inactive/Left", value=stats['inactive'], inline=True)
            embed.add_field(name="Updated Records", value=stats['updated'], inline=True)
            embed.add_field(name="New Records", value=stats['new'], inline=True)
            embed.add_field(name="Total Processed", value=len(updates), inline=True)

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error updating categories in guild {guild_id}: {e}", exc_info=True)
            await ctx.send("❌ An error occurred while updating categories. Check the logs for details.")

    async def cmd_kick_inactive(self, ctx: commands.Context) -> None:
        """Kick inactive members."""
        guild_id = str(ctx.guild.id)
        guild_config = self.config_manager.get_guild_config(ctx.guild.id)

        if not guild_config:
            await ctx.send("❌ Guild configuration not found.")
            return

        # get inactive members
        inactive_members = await self.member_data.get_members_by_category(guild_id, 'inactive')
        if not inactive_members:
            await ctx.send("✅ No inactive members to kick.")
            return

        # confirmation message
        confirm_msg = await ctx.send(
            f"⚠️ **Warning**: You are about to kick {len(inactive_members)} inactive members.\n"
            f"Type `CONFIRM` within 60 seconds to proceed."
        )

        def check(m):
            return (
                m.author == ctx.author and
                m.content == 'CONFIRM' and
                m.channel == ctx.channel
            )

        try:
            await self.wait_for('message', timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await ctx.send("❌ Kick operation cancelled due to timeout.")
            return

        progress_msg = await ctx.send("🔄 Processing kicks...")
        kicked_count = 0
        failed_kicks = []

        for member_data in inactive_members:
            await self.rate_limiter.wait()
            member = ctx.guild.get_member(int(member_data['member_id']))

            if not member:
                continue

            if self.activity_manager.member_has_ignore_role(member, guild_config):
                continue

            try:
                # try to DM the member
                try:
                    await member.send(
                        f"You have been removed from {ctx.guild.name} due to inactivity.\n"
                        f"You can rejoin later if you wish to participate."
                    )
                except discord.Forbidden:
                    logger.warning(f"Cannot send DM to {member.display_name}")

                # kick the member
                await member.kick(reason="Inactive member")
                kicked_count += 1

                # update progress every 5 kicks
                if kicked_count % 5 == 0:
                    await progress_msg.edit(
                        content=f"🔄 Progress: {kicked_count}/{len(inactive_members)} members processed..."
                    )

            except discord.Forbidden:
                failed_kicks.append(f"{member.display_name} (Missing Permissions)")
            except discord.HTTPException as e:
                failed_kicks.append(f"{member.display_name} (HTTP Error: {str(e)})")
            except Exception as e:
                logger.error(f"Failed to kick {member.display_name}: {e}", exc_info=True)
                failed_kicks.append(f"{member.display_name} (Unknown Error)")

            await asyncio.sleep(1)  # rate limiting

        # send summary
        embed = discord.Embed(
            title="👢 Kick Operation Complete",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )

        embed.add_field(
            name="Results",
            value=f"Successfully kicked: {kicked_count} members",
            inline=False
        )

        if failed_kicks:
            # if there are many failures, tell us why
            if len(failed_kicks) > 10:
                embed.add_field(
                    name="Failed Kicks",
                    value=f"{len(failed_kicks)} kicks failed. Details attached below.",
                    inline=False
                )

                # create a file with the full list
                file_content = "\n".join(failed_kicks)
                file = discord.File(
                    fp=io.StringIO(file_content),
                    filename=f"failed_kicks_{ctx.guild.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                )
                await ctx.send(embed=embed, file=file)
            else:
                # if we can just embed it instead then lets do that
                embed.add_field(
                    name="Failed Kicks",
                    value="\n".join(failed_kicks),
                    inline=False
                )
                await ctx.send(embed=embed)
        else:
            await ctx.send(embed=embed)

    async def cmd_export_data(self, ctx: commands.Context) -> None:
        """Export member activity data to a CSV file."""
        guild_id = str(ctx.guild.id)

        try:
            members = await self.member_data.get_all_members(guild_id)
            if not members:
                await ctx.send("❌ No member data to export.")
                return

            await ctx.send("🔄 Preparing data export...")

            # create CSV 
            csv_lines = ["Member ID,Display Name,Username,Message Count,Join Date,First Message,Last Message,Category,Left"]
            for member in members:
                csv_lines.append(
                    f"{member['member_id']},{member['displayname']},{member['username']},"
                    f"{member['message_count']},{member['join_date'] or ''},"
                    f"{member['first_message_date'] or ''},{member['last_message_date'] or ''},"
                    f"{member['category']},{member['left']}"
                )

            # create and send file
            file_content = "\n".join(csv_lines)
            file = discord.File(
                fp=io.StringIO(file_content),
                filename=f"member_activity_{ctx.guild.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )

            await ctx.send("✅ Here's your exported member data:", file=file)

        except Exception as e:
            logger.error(f"Error exporting data for guild {guild_id}: {e}", exc_info=True)
            await ctx.send("❌ An error occurred while exporting data. Check the logs for details.")

# main

def main():
    bot = AGSBot()
    try:
        bot.run(bot.token)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
    finally:
        logger.info("Bot has been shut down.")

if __name__ == "__main__":
    main()
