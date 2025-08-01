import asyncio
import discord
import logging
import re
import time
from datetime import timezone
from typing import Optional, Union, Dict, Tuple
from redbot.core import checks, Config, commands
from redbot.core.utils.chat_formatting import pagify


class RateLimiter:
    """Rate limiter that respects Discord's rate limit headers and maintains headroom."""

    def __init__(self, max_messages_per_second: int = 35):  # Higher default for better throughput
        self.max_messages_per_second = max_messages_per_second
        self.message_timestamps = []
        self.lock = asyncio.Lock()
        self.last_rate_limit_reset = 0
        self.remaining_requests = 50  # Default Discord limit
        self.rate_limit_reset_time = 0

    async def wait_if_needed(self):
        """Wait if we're approaching rate limits."""
        async with self.lock:
            current_time = time.time()

            # Clean old timestamps (older than 1 second)
            self.message_timestamps = [ts for ts in self.message_timestamps
                                     if current_time - ts < 1.0]

            # Check if we're at the limit
            if len(self.message_timestamps) >= self.max_messages_per_second:
                # Wait until we can send another message
                wait_time = 1.0 - (current_time - self.message_timestamps[0])
                if wait_time > 0:
                    logging.info(f"Rate limiting: waiting {wait_time:.2f} seconds")
                    await asyncio.sleep(wait_time)
                    current_time = time.time()

            # Add current timestamp
            self.message_timestamps.append(current_time)

    def update_from_headers(self, headers: Dict[str, str]):
        """Update rate limit info from Discord response headers."""
        try:
            if 'X-RateLimit-Remaining' in headers:
                self.remaining_requests = int(headers['X-RateLimit-Remaining'])

            if 'X-RateLimit-Reset' in headers:
                self.rate_limit_reset_time = float(headers['X-RateLimit-Reset'])

            logging.debug(f"Rate limit headers: remaining={self.remaining_requests}, reset={self.rate_limit_reset_time}")
        except (ValueError, KeyError) as e:
            logging.warning(f"Failed to parse rate limit headers: {e}")


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
        try:
            if member.bot:
                return
                
            # Wait for rate limiter before sending
            await self.rate_limiter.wait_if_needed()
            
            # Send the message and capture response for headers
            response = await member.send(content=message, embed=embed)
            
            # Update rate limiter with headers if available
            if hasattr(response, '_response') and hasattr(response._response, 'headers'):
                self.rate_limiter.update_from_headers(dict(response._response.headers))
            
            logging.info(f"Sent {message} to {member.display_name}.")
        except discord.RateLimited as e:
            logging.error(
                f'RATE LIMITED {e}\n  Hit rate limit while sending to {member.display_name}. Waiting {e.retry_after} seconds.'
            )
            await asyncio.sleep(e.retry_after)
            # Retry once after waiting
            try:
                await self.rate_limiter.wait_if_needed()
                await member.send(content=message, embed=embed)
                logging.info(f"Retry successful: sent {message} to {member.display_name}.")
            except Exception as retry_e:
                logging.error(
                    f'RETRY FAILED {retry_e}\n  Failed retry sending "{message}" to {member.display_name}.'
                )
        except Exception as e:
            logging.error(
                f'EXCEPTION {e}\n  Failed in sending "{message}" to {member.display_name}.'
            )

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
        
        # Loop over all the targets identified in the config and send them a message.
        # Use rate limiter to stay under Discord's limits with headroom.
        waitlist = []
        sem = asyncio.Semaphore(10)  # Reduced from 20 to be more conservative
        
        async def send_to_target(target):
            """Send notification to a single target with proper rate limiting."""
            async with sem:
                try:
                    target_id = target["id"]
                    target_type = target["type"]
                    
                    if target_type == "TextChannel":
                        chan = message.guild.get_channel(target_id)
                        if chan:
                            # Wait for rate limiter before sending
                            await self.rate_limiter.wait_if_needed()
                            
                            # Send message and capture response for headers
                            response = await chan.send(f"@everyone {base_msg}", embed=embed)
                            
                            # Update rate limiter with headers if available
                            if hasattr(response, '_response') and hasattr(response._response, 'headers'):
                                self.rate_limiter.update_from_headers(dict(response._response.headers))
                            
                            logging.info(f"Sent notification to channel {chan.name}.")
                            
                    elif target_type == "Member":
                        member = message.guild.get_member(target_id)
                        if member:
                            await self._send_to_member(member, base_msg, embed)
                            
                    elif target_type == "Role":
                        role = message.guild.get_role(target_id)
                        if role:
                            for member in role.members:
                                await self._send_to_member(member, base_msg, embed)
                                
                except discord.RateLimited as e:
                    logging.error(
                        f"RATE LIMITED {e}\n  Hit rate limit while sending to {target_type} {target_id}. Waiting {e.retry_after} seconds."
                    )
                    await asyncio.sleep(e.retry_after)
                    # Retry once after waiting
                    try:
                        await self.rate_limiter.wait_if_needed()
                        if target_type == "TextChannel":
                            chan = message.guild.get_channel(target_id)
                            if chan:
                                await chan.send(f"@everyone {base_msg}", embed=embed)
                        elif target_type == "Member":
                            member = message.guild.get_member(target_id)
                            if member:
                                await member.send(content=base_msg, embed=embed)
                        elif target_type == "Role":
                            role = message.guild.get_role(target_id)
                            if role:
                                for member in role.members:
                                    await member.send(content=base_msg, embed=embed)
                        logging.info(f"Retry successful for {target_type} {target_id}.")
                    except Exception as retry_e:
                        logging.error(
                            f"RETRY FAILED {retry_e}\n  Failed retry for {target_type} {target_id}."
                        )
                except Exception as e:
                    logging.error(
                        f"EXCEPTION {e}\n  Trying to message {target}\n  Triggered on {message.clean_content} by {message.author}"
                    )
        
        # Create tasks for all targets
        for target in targets:
            waitlist.append(asyncio.create_task(send_to_target(target)))
        
        # Wait for all tasks to complete
        if waitlist:
            await asyncio.wait(waitlist, return_when=asyncio.ALL_COMPLETED)

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
