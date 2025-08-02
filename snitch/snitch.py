import asyncio
import discord
import logging
import re
import time
from datetime import timezone
from typing import Optional, Union, Dict, Callable, Any
from redbot.core import checks, Config, commands
from redbot.core.utils.chat_formatting import pagify


class RateLimiter:
    
    def __init__(self, max_requests_per_second: int = 35, max_concurrent: int = 20):
        self.max_requests_per_second = max_requests_per_second
        self.max_concurrent = max_concurrent
        self.request_times = []
        self.lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        async with self.semaphore:
            await self._wait_for_rate_limit()
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = await func(*args, **kwargs)
                    return result
                except discord.RateLimited as e:
                    if attempt == max_retries - 1:
                        raise
                    logging.warning(f"Rate limited, waiting {e.retry_after:.2f}s (attempt {attempt + 1})")
                    await asyncio.sleep(e.retry_after)
                except Exception:
                    raise
    
    async def _wait_for_rate_limit(self):
        async with self.lock:
            now = time.time()
            
            # Clean old timestamps
            self.request_times = [t for t in self.request_times if now - t < 1.0]
            
            # Check if we need to wait
            if len(self.request_times) >= self.max_requests_per_second:
                sleep_time = 1.0 - (now - self.request_times[0])
                if sleep_time > 0:
                    logging.info(f"Rate limiting: waiting {sleep_time:.2f} seconds ({len(self.request_times)}/{self.max_requests_per_second} requests)")
                    await asyncio.sleep(sleep_time)
                    now = time.time()
            
            # Record this request
            self.request_times.append(now)
    
    def get_stats(self) -> Dict[str, Any]:
        now = time.time()
        recent_requests = len([t for t in self.request_times if now - t < 1.0])
        return {
            "recent_requests": recent_requests,
            "max_requests_per_second": self.max_requests_per_second,
            "max_concurrent": self.max_concurrent,
            "available_requests": max(0, self.max_requests_per_second - recent_requests)
        }


class Snitch(commands.Cog):
    """
    Cog to notify groups of users, roles, or channels when certain phrases are said.
    Modified from the default Filter module here: https://github.com/Cog-Creators/Red-DiscordBot/blob/V3/develop/redbot/cogs/words/words.py
    """

    def __init__(self, bot):
        super().__init__()
        self.bot = bot
        self.config = Config.get_conf(self, identifier=586925412)
        default_guild_settings = {"notifygroups": {}}
        self.config.register_guild(**default_guild_settings)
        self.rate_limiter = RateLimiter()

    @commands.group("snitch")
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def _snitch(self, ctx: commands.Context):
        """Base command to manage snitch settings."""
        pass

    def _identify_target(
        self, ctx: commands.Context, target: str
    ) -> Union[discord.abc.Messageable, None]:
        """Try to convert a potential target into a messageable interface.

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param target: The potential target.
        :type target: str
        :return: If the target can be mapped, a Messagable. Otherwise None.
        :rtype: Union[discord.abc.Messageable, None]
        """
        coerced = None
        server = ctx.guild
        # We need to figure out what was passed in. If they're passed in as their ID, it's relatively easy, just
        # try to coerce the value into an appropriate object and if it works bail out. As a bonus, these aren't
        # async so we can just fudge it like so.
        maybe_id = target.strip("!<#>@&")
        if maybe_id.isnumeric():
            if coerced := server.get_member(int(maybe_id)):
                pass
            elif coerced := server.get_role(int(maybe_id)):
                pass
            elif coerced := server.get_channel(int(maybe_id)):
                pass
        # If that doesn't work we need to filter through a bunch of object names to find a match.
        elif not coerced:
            # Check roles for matches.
            matches = [
                role for role in ctx.guild.roles if role.name.lower() == target.lower()
            ]
            # Grab the first match if one exists.
            coerced = matches.pop(0) if any(matches) else None
            # If no match do the same for members.
            if not coerced:
                matches = [
                    member
                    for member in ctx.guild.members
                    if member.name.lower() == target.lower()
                    or member.display_name.lower() == target.lower()
                ]
                coerced = matches.pop(0) if any(matches) else None
            # And channels.
            if not coerced:
                matches = [
                    channel
                    for channel in ctx.guild.channels
                    if channel.name.lower() == target.lower()
                    and isinstance(channel, discord.TextChannel)
                ]
                coerced = matches.pop(0) if any(matches) else None
        return coerced

    @_snitch.command(name="to")
    async def _snitch_add(self, ctx: commands.Context, group: str, *targets: str):
        """Add people, roles, or channels to a notification group. IDs can be passed in using # or @ as appropriate.
        Text input will be evaluated checking roles, then members, then channels. @everyone also works.

        Example:
            `[p]snitch to tech #tech-general @Site.Tech Brenticus`

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param group: The notification group to modify.
        :type group: str
        :param targets: The list of targets to notify.
        :type targets: List[str]
        """
        server = ctx.guild
        async with self.config.guild(server).notifygroups() as notifygroups:
            notifygroup = notifygroups.get(group)
            if not notifygroup:
                notifygroup = {"words": [], "targets": {}}
            for target in targets:
                coerced = self._identify_target(ctx, target)
                # We store the coerced value so things are easier later.
                if coerced:
                    target_type = type(coerced).__name__
                    notifygroup["targets"][target] = {
                        "id": coerced.id,
                        "type": target_type,
                    }
                    await ctx.channel.send(f"{target_type} {target} will be notified.")
                else:
                    await ctx.channel.send(f"Could not identify {target}.")
            notifygroups[group] = notifygroup

    @_snitch.command(name="notto")
    async def _snitch_del(self, ctx: commands.Context, group: str, *targets: str):
        """Remove people, roles, or channels from a notification group.

        Example:
            `[p]snitch notto tech #tech-general`

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param group: The notification group to modify.
        :type group: str
        :param targets: The list of targets to notify.
        :type targets: List[str]
        """
        server = ctx.guild
        async with self.config.guild(server).notifygroups() as notifygroups:
            notifygroup = notifygroups.get(group)
            if not notifygroup:
                await ctx.channel.send(f"Group doesn't exist.")
            for target in targets:
                if target in notifygroup["targets"]:
                    notifygroup["targets"].pop(target)
                    await ctx.channel.send(f"Removed {target}.")
                else:
                    await ctx.channel.send(f"Couldn't find {target}.")

    @_snitch.command(name="on", require_var_positional=True)
    async def _words_add(self, ctx: commands.Context, group: str, *words: str):
        """Add trigger words to the notification group. Use double quotes to add sentences.

        Example:
            `[p]snitch on tech computer wifi it`

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param group: The notification group to modify.
        :type group: str
        :param words: The list of trigger words to notify on.
        :type words: List[str]
        """
        server = ctx.guild
        async with self.config.guild(server).notifygroups() as notifygroups:
            notifygroup = notifygroups.get(group)
            if not notifygroup:
                notifygroup = {"words": [], "targets": {}}
            for word in words:
                if not word in notifygroup["words"]:
                    notifygroup["words"].append(word)
                await ctx.channel.send(f"{word} will trigger a notification.")
            notifygroups[group] = notifygroup

    @_snitch.command(name="noton", require_var_positional=True)
    async def _words_remove(self, ctx: commands.Context, group: str, *words: str):
        """Remove trigger words from the notification group. Use double quotes to remove sentences.

        Examples:
            - `[p]snitch noton tech wifi`

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param group: The notification group to modify.
        :type group: str
        :param words: The list of trigger words to notify on.
        :type group: List[str]
        """
        server = ctx.guild
        async with self.config.guild(server).notifygroups() as notifygroups:
            notifygroup = notifygroups.get(group)
            if not notifygroup:
                notifygroup = {"words": [], "targets": {}}
            for word in words:
                notifygroup["words"].remove(word)
                await ctx.channel.send(f"{word} will no longer trigger a notification.")
            notifygroups[group] = notifygroup

    @_snitch.command(name="with", require_var_positional=True)
    async def _message_change(self, ctx: commands.Context, group: str, message: str):
        """Change the message sent with your snitch. Use double quotes around the message.

        Example:
            `[p]snitch with tech "{{author}} needs IT assistance in {{channel}}."

        Tokens:
            {{author}} - The display name of the message author.
            {{channel}} - The channel name the message originated in.
            {{server}} - The server name the message originated in.
            {{words}} - The list of words that triggered the message.

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param group: The notification group to modify.
        :type group: str
        :param message: The message to send with any notifications for this group.
        :type message: str
        """
        server = ctx.guild
        async with self.config.guild(server).notifygroups() as notifygroups:
            notifygroup = notifygroups.get(group)
            if not notifygroup:
                notifygroup = {"words": [], "targets": {}}
            notifygroup["message"] = message
            notifygroups[group] = notifygroup
            await ctx.channel.send(f"Message for {group} updated.")

    @_snitch.command(name="clear")
    async def _clear_list(self, ctx: commands.Context, group: str = None):
        """Remove all config data for the group. Omit the group to clear all config data for this cog.

        Example:
            [p]snitch clear
            [p]snitch clear tech

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param group: The notification group to modify.
        :type group: Optional[str]
        """
        server = ctx.guild
        # If group isn't identified clear everything.
        if not group:
            await self.config.guild(ctx.guild).notifygroups.clear()
            await ctx.channel.send("Cleared all snitch settings.")
            return
        async with self.config.guild(server).notifygroups() as notifygroups:
            if notifygroups.get(group):
                notifygroups.pop(group)
                await ctx.channel.send(f"Removed {group} from snitch settings.")
            else:
                await ctx.channel.send(f"Could not find {group} in snitch settings.")

    @_snitch.command(name="list")
    async def _global_list(self, ctx: commands.Context):
        """Send a list of this server's people and words involved in snitching.

        Example:
            [p]snitch list

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        """
        server = ctx.guild
        author = ctx.author
        group_list = await self.config.guild(server).notifygroups()
        if not group_list:
            await ctx.send(
                "There are no current notification groups set up in this server."
            )
            return
        group_text = "Filtered in this server:" + "\n"
        for name, vals in group_list.items():
            people = ", ".join(vals["targets"].keys())
            words = ", ".join(vals["words"])
            group_text += f"\t{name} tells {people} about {words}\n"
        try:
            for page in pagify(group_text, delims=[" ", "\n"], shorten_by=8):
                await ctx.channel.send(page)
        except Exception as e:
            logging.error(
                f"EXCEPTION {e}\n  Can't send message to channel.\n  Triggered on {ctx.message.clean_content} by {author}"
            )
            await ctx.send("I can't send direct messages to you.")

    @_snitch.command(name="rate")
    async def _rate_status(self, ctx: commands.Context):
        """Show current rate limiting status and statistics."""
        stats = self.rate_limiter.get_stats()
        
        embed = discord.Embed(
            title="Snitch Rate Limiting Status",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Current Usage",
            value=f"{stats['recent_requests']}/{stats['max_requests_per_second']} requests/second",
            inline=True
        )
        embed.add_field(
            name="Available Capacity",
            value=f"{stats['available_requests']} requests",
            inline=True
        )
        embed.add_field(
            name="Max Concurrent",
            value=f"{stats['max_concurrent']} operations",
            inline=True
        )
        
        await ctx.send(embed=embed)

    @_snitch.command(name="setrate")
    async def _set_rate_limit(self, ctx: commands.Context, max_requests_per_second: int):
        """Set the maximum requests per second for rate limiting.
        
        Recommended: 30-40 for safety margin below Discord's 50/sec limit.
        """
        if max_requests_per_second < 1 or max_requests_per_second > 50:
            await ctx.send("Rate limit must be between 1 and 50 requests per second.")
            return
            
        self.rate_limiter.max_requests_per_second = max_requests_per_second
        await ctx.send(f"Rate limit set to {max_requests_per_second} requests per second.")

    async def _send_to_member(
        self,
        member: discord.Member,
        message: str,
        embed: Optional[discord.Embed] = None,
    ):
        """DM a member.

        Note that there are a lot of failure cases here based on permissions of the bot and privacy settings of server
        members. These get logged in case the bot owner needs to investigate.

        :param member: The member who the bot will DM.
        :type member: discord.Member
        :param message: The message to send.
        :type message: str
        :param embed: The embed to include with the message.
        :type embed: discord.Embed
        """
        if member.bot:
            return
            
        try:
            await self.rate_limiter.execute(member.send, content=message, embed=embed)
            logging.info(f"Sent message to {member.display_name}")
        except Exception as e:
            logging.error(f"Failed to send message to {member.display_name}: {e}")

    async def _notify_words(
        self,
        message: discord.Message,
        targets: list,
        words: list,
        base_msg: Optional[str] = None,
    ):
        """Notify the targets configured to be notifies.

        :param message: The message that triggered this notification.
        :type message: discord.Message
        :param targets: The list of targets to be notified.
        :type targets: list
        :param words: The list of words that triggered this notification.
        :type words: list
        :param base_msg: The base message to send with the notification. See _message_change() for more info.
        :type base_msg: Optional[str]
        """
        word_msg = " and ".join(words)
        base_msg = base_msg or "Snitching on {{author}} for saying {{words}}"
        base_msg = (
            base_msg.replace("{{author}}", message.author.display_name)
            .replace("{{words}}", word_msg)
            .replace("{{server}}", message.guild.name)
            .replace("{{channel}}", message.channel.name)
        )

        embed = discord.Embed(
            title=f"{message.author.display_name} in {message.channel}",
            type="link",
            description=message.content,
            url=message.jump_url,
            colour=discord.Color.red(),
        ).set_thumbnail(url=message.author.display_avatar.url)
        
        # Collect all send operations to execute
        send_tasks = []
        
        for target in targets:
            try:
                target_id = target["id"]
                target_type = target["type"]
                
                if target_type == "TextChannel":
                    chan = message.guild.get_channel(target_id)
                    if chan:
                        async def send_to_channel(channel, msg, embed_obj):
                            result = await self.rate_limiter.execute(
                                channel.send, f"@everyone {msg}", embed=embed_obj
                            )
                            logging.info(f"Sent {msg} to {channel.name}")
                            return result
                        
                        send_tasks.append(send_to_channel(chan, base_msg, embed))
                        
                elif target_type == "Member":
                    member = message.guild.get_member(target_id)
                    if member:
                        send_tasks.append(
                            self._send_to_member(member, base_msg, embed)
                        )
                        
                elif target_type == "Role":
                    role = message.guild.get_role(target_id)
                    if role:
                        for member in role.members:
                            send_tasks.append(
                                self._send_to_member(member, base_msg, embed)
                            )
                            
            except Exception as e:
                logging.error(
                    f"Error preparing notification for {target}: {e}"
                )
        
        # Execute all send operations
        if send_tasks:
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            successful = sum(1 for r in results if not isinstance(r, Exception))
            failed = len(results) - successful
            
            if failed > 0:
                logging.warning(f"Notification summary: {successful} successful, {failed} failed out of {len(send_tasks)} total")
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logging.error(f"Failed notification {i+1}: {result}")
            else:
                logging.info(f"Successfully sent all {successful} notifications")

    async def _check_words(self, message: discord.Message):
        """Check whether we really should notify people.

        :param message: The message to check for trigger words.
        :type message: discord.Message
        """
        server = message.guild

        async with self.config.guild(server).notifygroups() as notifygroups:
            for notifygroup in notifygroups.values():
                word_list = notifygroup.get("words")
                # Escape and combine the words into a regex matching string.
                if word_list:
                    pattern = re.compile(
                        "|".join(rf"\b{re.escape(w)}\b" for w in word_list), flags=re.I
                    )
                else:
                    pattern = None
                matches = None
                if pattern:
                    # See if there are any hits.
                    matches = set(pattern.findall(message.content))
                if matches:
                    # If there are, tell the targets.
                    await self._notify_words(
                        message,
                        notifygroup["targets"].values(),
                        matches,
                        base_msg=notifygroup.get("message"),
                    )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Check every message the bot can see for trigger words.

        :param message: The message.
        :type message: discord.Message
        """
        # This can only run in servers.
        if message.guild is None:
            return
        # Make sure the bot is allowed in the server.
        if await self.bot.cog_disabled_in_guild(self, message.guild):
            return
        # Check if the message starts with a prefix, indicating it's a command.
        prefixes = await self.bot.get_prefix(message)
        prefix_check = (
            isinstance(prefixes, str) and message.clean_content.startswith(prefixes)
        ) or (
            isinstance(prefixes, list)
            and any([True for y in prefixes if message.clean_content.startswith(y)])
        )
        if prefix_check:
            return
        # Check if the message was sent by an actual person.
        author = message.author
        valid_user = isinstance(author, discord.Member) and not author.bot
        if not valid_user:
            return
        # Check if automod contexts would normally ignore this message.
        if await self.bot.is_automod_immune(message):
            return
        # Now we shuffle the work off to another method.
        await self._check_words(message)

    @commands.Cog.listener()
    async def on_message_edit(self, _prior, message: discord.Message):
        """Check every edit the bot can see for trigger words.

        :param _prior: The message prior to editing.
        :type _prior: None
        :param message: The message post edit.
        :type message: dicord.Message
        """
        await self.on_message(message)