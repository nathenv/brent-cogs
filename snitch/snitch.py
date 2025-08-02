import asyncio
import discord
import logging
import re
import time
from typing import Optional, Union, Dict
from redbot.core import checks, Config, commands
from redbot.core.utils.chat_formatting import pagify


class RateLimiter:
    """Rate limiter that respects Discord's rate limit headers and maintains headroom."""

    def __init__(self, max_messages_per_second: int = 35):
        self.max_messages_per_second = max_messages_per_second
        self.message_timestamps = []
        self.lock = asyncio.Lock()
        self.remaining_requests = 50  # Default Discord limit
        self.rate_limit_reset_time = 0
        self.last_request_time = 0

    async def wait_if_needed(self):
        """Wait if we're approaching rate limits."""
        async with self.lock:
            current_time = time.time()
            
            # Clean old timestamps (older than 1 second)
            self.message_timestamps = [ts for ts in self.message_timestamps
                                     if current_time - ts < 1.0]

            # Calculate minimum delay between messages
            min_delay = 1.0 / self.max_messages_per_second
            
            # Check if we need to wait
            if len(self.message_timestamps) >= self.max_messages_per_second:
                # Wait until we can send another message
                wait_time = 1.0 - (current_time - self.message_timestamps[0])
                if wait_time > 0:
                    logging.info(f"Rate limiting: waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
                    current_time = time.time()
            elif current_time - self.last_request_time < min_delay:
                # Ensure minimum delay between messages
                wait_time = min_delay - (current_time - self.last_request_time)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    current_time = time.time()

            # Add current timestamp and update last request time
            self.message_timestamps.append(current_time)
            self.last_request_time = current_time

    async def send_message(self, send_func, *args, **kwargs):
        """Send a single message with proper rate limiting and retry logic."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                await self.wait_if_needed()
                response = await send_func(*args, **kwargs)
                
                # Update rate limiter with headers if available
                if hasattr(response, '_response') and hasattr(response._response, 'headers'):
                    self.update_from_headers(dict(response._response.headers))
                
                return response
                
            except discord.RateLimited as e:
                retry_count += 1
                logging.error(f"RATE LIMITED: {e} (attempt {retry_count}/{max_retries})")
                
                # Use longer retry delay to be more conservative
                retry_delay = max(e.retry_after, 2.0)
                await asyncio.sleep(retry_delay)
                
                if retry_count >= max_retries:
                    logging.error(f"Max retries reached for message send")
                    raise e
                    
            except Exception as e:
                logging.error(f"Failed to send message: {e}")
                raise e

    async def send_messages_sequentially(self, messages_to_send):
        """Send messages one by one with proper rate limiting.
        
        :param messages_to_send: List of (send_func, args, kwargs) tuples
        :return: List of results from the send operations
        """
        results = []
        
        for i, (send_func, args, kwargs) in enumerate(messages_to_send):
            try:
                result = await self.send_message(send_func, *args, **kwargs)
                results.append(result)
                logging.info(f"Successfully sent message {i+1}/{len(messages_to_send)}")
                
            except Exception as e:
                logging.error(f"Failed to send message {i+1}: {e}")
                results.append(e)
        
        return results

    def update_from_headers(self, headers: Dict[str, str]):
        """Update rate limit info from Discord response headers."""
        try:
            if 'X-RateLimit-Remaining' in headers:
                self.remaining_requests = int(headers['X-RateLimit-Remaining'])

            if 'X-RateLimit-Reset' in headers:
                self.rate_limit_reset_time = float(headers['X-RateLimit-Reset'])

            logging.info(f"Rate limit headers: remaining={self.remaining_requests}, reset={self.rate_limit_reset_time}")
        except (ValueError, KeyError) as e:
            logging.info(f"Failed to parse rate limit headers: {e}")


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
    async def _rate_limit_status(self, ctx: commands.Context):
        """Show current rate limiting status and statistics.

        Example:
            [p]snitch rate

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        """
        current_time = time.time()
        recent_messages = len([ts for ts in self.rate_limiter.message_timestamps 
                             if current_time - ts < 1.0])
        
        embed = discord.Embed(
            title="Snitch Rate Limiting Status",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="Current Rate", 
            value=f"{recent_messages}/{self.rate_limiter.max_messages_per_second} messages per second",
            inline=True
        )
        embed.add_field(
            name="Remaining Requests", 
            value=f"{self.rate_limiter.remaining_requests}",
            inline=True
        )
        embed.add_field(
            name="Rate Limit Reset", 
            value=f"<t:{int(self.rate_limiter.rate_limit_reset_time)}:R>" if self.rate_limiter.rate_limit_reset_time > 0 else "Unknown",
            inline=True
        )
        embed.add_field(
            name="Headroom", 
            value=f"{self.rate_limiter.max_messages_per_second - recent_messages} messages available",
            inline=True
        )
        
        await ctx.send(embed=embed)

    @_snitch.command(name="setrate")
    async def _set_rate_limit(self, ctx: commands.Context, max_messages_per_second: int):
        """Set the maximum messages per second for rate limiting.

        Example:
            [p]snitch setrate 15

        :param ctx: The Discord Red command context.
        :type ctx: commands.Context
        :param max_messages_per_second: Maximum messages per second (recommended: 30-35)
        :type max_messages_per_second: int
        """
        if max_messages_per_second < 1 or max_messages_per_second > 50:
            await ctx.send("Rate limit must be between 1 and 50 messages per second.")
            return
            
        self.rate_limiter.max_messages_per_second = max_messages_per_second
        await ctx.send(f"Rate limit set to {max_messages_per_second} messages per second.")



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
        
        # Collect all messages to send in batches
        messages_to_send = []
        
        for target in targets:
            target_id = target["id"]
            target_type = target["type"]
            
            try:
                if target_type == "TextChannel":
                    chan = message.guild.get_channel(target_id)
                    if chan:
                        # Add channel message to batch
                        messages_to_send.append((
                            chan.send,
                            (f"@everyone {base_msg}",),
                            {"embed": embed}
                        ))
                        
                elif target_type == "Member":
                    member = message.guild.get_member(target_id)
                    if member and not member.bot:
                        # Add member message to batch
                        messages_to_send.append((
                            member.send,
                            (base_msg,),
                            {"embed": embed}
                        ))
                        
                elif target_type == "Role":
                    role = message.guild.get_role(target_id)
                    if role:
                        # Add role member messages to batch
                        for member in role.members:
                            if not member.bot:
                                messages_to_send.append((
                                    member.send,
                                    (base_msg,),
                                    {"embed": embed}
                                ))
                                
            except Exception as e:
                logging.error(
                    f"EXCEPTION {e}\n  Trying to prepare message for {target}\n  Triggered on {message.clean_content} by {message.author}"
                )
        
        # Send all messages sequentially with proper rate limiting
        if messages_to_send:
            try:
                results = await self.rate_limiter.send_messages_sequentially(messages_to_send)
                
                # Log successful sends
                successful_sends = sum(1 for result in results if not isinstance(result, Exception))
                logging.info(f"Successfully sent {successful_sends}/{len(messages_to_send)} notifications.")
                
                # Log any exceptions
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logging.error(f"Failed to send message {i}: {result}")
                        
            except Exception as e:
                logging.error(f"Failed to send notifications: {e}")

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
