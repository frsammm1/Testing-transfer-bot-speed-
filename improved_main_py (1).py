import os
import asyncio
import logging
import time
import math
import mimetypes
from telethon import TelegramClient, events, utils, errors
from telethon.sessions import StringSession
from telethon.network import connection
from telethon.tl.types import (
    DocumentAttributeFilename, 
    DocumentAttributeVideo, 
    DocumentAttributeAudio,
    MessageMediaWebPage,
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaContact,
    MessageMediaGeo,
    MessageMediaVenue,
    MessageMediaPoll,
    MessageMediaDice,
    MessageMediaGame,
    MessageMediaInvoice,
    MessageMediaGeoLive,
    InputMessagesFilterPhotos,
    InputMessagesFilterVideo,
    InputMessagesFilterDocument
)
from aiohttp import web

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH")
STRING_SESSION = os.environ.get("STRING_SESSION") 
BOT_TOKEN = os.environ.get("BOT_TOKEN")           
PORT = int(os.environ.get("PORT", 8080))

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CLIENT SETUP (SPEED OPTIMIZED) ---
user_client = TelegramClient(
    StringSession(STRING_SESSION), 
    API_ID, 
    API_HASH,
    connection=connection.ConnectionTcpFull,
    use_ipv6=False,
    connection_retries=None, 
    flood_sleep_threshold=60,
    request_retries=10,
    auto_reconnect=True,
    timeout=45,
    receive_updates=False
)

bot_client = TelegramClient(
    'bot_session', 
    API_ID, 
    API_HASH,
    connection=connection.ConnectionTcpFull,
    use_ipv6=False,
    connection_retries=None, 
    flood_sleep_threshold=60,
    request_retries=10,
    auto_reconnect=True,
    timeout=45,
    receive_updates=False
)

# --- GLOBAL STATE ---
pending_requests = {} 
current_task = None
is_running = False
status_message = None
last_update_time = 0

# --- WEB SERVER ---
async def handle(request):
    return web.Response(text="🔥 Ultra Transfer Bot Running - All Content Types Supported!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Web server started on port {PORT}")

# --- HELPER FUNCTIONS ---
def human_readable_size(size):
    if not size: return "0B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0: return f"{size:.2f}{unit}"
        size /= 1024.0
    return f"{size:.2f}TB"

def time_formatter(seconds):
    if seconds is None or seconds < 0: return "..."
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0: return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"

# --- PROGRESS CALLBACK ---
async def progress_callback(current, total, start_time, file_name):
    global last_update_time, status_message
    now = time.time()
    
    if now - last_update_time < 5: return 
    last_update_time = now
    
    percentage = current * 100 / total if total > 0 else 0
    time_diff = now - start_time
    speed = current / time_diff if time_diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0
    
    filled = math.floor(percentage / 10)
    bar = "█" * filled + "░" * (10 - filled)
    
    try:
        await status_message.edit(
            f"📦 **Transferring Content**\n"
            f"📂 `{file_name}`\n"
            f"**{bar} {round(percentage, 1)}%**\n"
            f"🚀 `{human_readable_size(speed)}/s` | ⏳ `{time_formatter(eta)}`\n"
            f"💾 `{human_readable_size(current)} / {human_readable_size(total)}`"
        )
    except Exception: pass

# --- ULTRA BUFFERED STREAM (OPTIMIZED) ---
class UltraBufferedStream:
    def __init__(self, client, location, file_size, file_name, start_time):
        self.client = client
        self.location = location
        self.file_size = file_size
        self.name = file_name
        self.start_time = start_time
        self.current_bytes = 0
        self.chunk_size = 12 * 1024 * 1024  # 12MB chunks
        self.queue = asyncio.Queue(maxsize=6)
        self.downloader_task = asyncio.create_task(self._worker())
        self.buffer = b""

    async def _worker(self):
        try:
            async for chunk in self.client.iter_download(self.location, chunk_size=self.chunk_size):
                await self.queue.put(chunk)
            await self.queue.put(None) 
        except Exception as e:
            logger.error(f"Stream Worker Error: {e}")
            await self.queue.put(None)

    def __len__(self):
        return self.file_size

    async def read(self, size=-1):
        if size == -1: size = self.chunk_size
        while len(self.buffer) < size:
            chunk = await self.queue.get()
            if chunk is None: 
                if self.current_bytes < self.file_size:
                    raise errors.RpcCallFailError("Incomplete Stream")
                break
            self.buffer += chunk
            self.current_bytes += len(chunk)
            asyncio.create_task(progress_callback(self.current_bytes, self.file_size, self.start_time, self.name))
        data = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return data

# --- SMART MESSAGE HANDLER ---
async def handle_message_transfer(message, dest_id, retry_count=3):
    """
    Universal message handler - supports ALL Telegram content types
    """
    
    # Handle Text-Only Messages (including links)
    if not message.media and message.text:
        try:
            await bot_client.send_message(
                dest_id, 
                message.text,
                formatting_entities=message.entities,
                link_preview=False
            )
            return True, "Text Message"
        except Exception as e:
            logger.error(f"Text message failed: {e}")
            return False, f"Text Error: {e}"
    
    # Handle Photos
    if isinstance(message.media, MessageMediaPhoto):
        try:
            start_time = time.time()
            photo = message.media.photo
            file_size = max([size.size for size in photo.sizes if hasattr(size, 'size')], default=0)
            
            stream_file = UltraBufferedStream(
                user_client,
                photo,
                file_size,
                f"Photo_{message.id}.jpg",
                start_time
            )
            
            await bot_client.send_file(
                dest_id,
                file=stream_file,
                caption=message.text or "",
                force_document=False,  # Send as photo
                file_size=file_size,
                part_size_kb=10240
            )
            return True, "Photo"
        except Exception as e:
            logger.error(f"Photo transfer failed: {e}")
            return False, f"Photo Error: {e}"
    
    # Handle Documents (Videos, Audio, Files, Stickers, GIFs, etc.)
    if isinstance(message.media, MessageMediaDocument):
        try:
            document = message.media.document
            start_time = time.time()
            
            # Get original filename and MIME type
            file_name = "Unknown_File"
            mime_type = document.mime_type
            is_voice = False
            is_video_note = False
            is_sticker = False
            is_gif = False
            is_audio = False
            is_video = False
            
            # Parse attributes to get file info
            attributes = []
            for attr in document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    file_name = attr.file_name
                    
                if isinstance(attr, DocumentAttributeVideo):
                    is_video = True
                    if attr.round_message:
                        is_video_note = True
                    attributes.append(attr)
                    
                if isinstance(attr, DocumentAttributeAudio):
                    is_audio = True
                    if attr.voice:
                        is_voice = True
                    attributes.append(attr)
            
            # Detect stickers and GIFs
            if mime_type == "application/x-tgsticker" or "sticker" in mime_type.lower():
                is_sticker = True
            if mime_type == "image/gif" or file_name.lower().endswith('.gif'):
                is_gif = True
            
            # If no filename found, generate one based on type
            if file_name == "Unknown_File":
                ext = mimetypes.guess_extension(mime_type) or ""
                if is_video:
                    file_name = f"Video_{message.id}{ext or '.mp4'}"
                elif is_audio:
                    file_name = f"Audio_{message.id}{ext or '.mp3'}"
                elif is_voice:
                    file_name = f"Voice_{message.id}.ogg"
                elif is_sticker:
                    file_name = f"Sticker_{message.id}.webp"
                elif is_gif:
                    file_name = f"Animation_{message.id}.gif"
                else:
                    file_name = f"Document_{message.id}{ext}"
            
            # Add filename attribute if not present
            has_filename_attr = any(isinstance(a, DocumentAttributeFilename) for a in attributes)
            if not has_filename_attr:
                attributes.insert(0, DocumentAttributeFilename(file_name=file_name))
            
            # Get thumbnail
            thumb = None
            try:
                thumb = await user_client.download_media(message, thumb=-1)
            except:
                pass
            
            # Create optimized stream
            stream_file = UltraBufferedStream(
                user_client,
                document,
                document.size,
                file_name,
                start_time
            )
            
            # Determine force_document flag (send as file vs media)
            # Force document for: HTML, TXT, ZIP, APK, EXE, and other file types
            force_doc = True
            if is_video or is_gif or is_audio or is_voice or is_video_note or is_sticker:
                force_doc = False
            
            # Send the file with original attributes
            await bot_client.send_file(
                dest_id,
                file=stream_file,
                caption=message.text or "",
                attributes=attributes,
                thumb=thumb,
                supports_streaming=is_video and not is_video_note,
                force_document=force_doc,
                file_size=document.size,
                part_size_kb=10240,
                voice_note=is_voice,
                video_note=is_video_note
            )
            
            # Cleanup thumbnail
            if thumb and os.path.exists(thumb):
                try:
                    os.remove(thumb)
                except:
                    pass
            
            content_type = "Voice" if is_voice else "Video Note" if is_video_note else "Sticker" if is_sticker else "GIF" if is_gif else "Audio" if is_audio else "Video" if is_video else "Document"
            return True, content_type
            
        except Exception as e:
            logger.error(f"Document transfer failed: {e}")
            return False, f"Document Error: {e}"
    
    # Handle Web Pages (Links with preview)
    if isinstance(message.media, MessageMediaWebPage):
        try:
            await bot_client.send_message(
                dest_id,
                message.text or "",
                formatting_entities=message.entities,
                link_preview=True
            )
            return True, "Link with Preview"
        except Exception as e:
            logger.error(f"WebPage failed: {e}")
            return False, f"Link Error: {e}"
    
    # Handle Contacts
    if isinstance(message.media, MessageMediaContact):
        try:
            await bot_client.forward_messages(dest_id, message.id, message.peer_id)
            return True, "Contact"
        except Exception as e:
            logger.error(f"Contact forward failed: {e}")
            return False, f"Contact Error: {e}"
    
    # Handle Locations
    if isinstance(message.media, (MessageMediaGeo, MessageMediaVenue, MessageMediaGeoLive)):
        try:
            await bot_client.forward_messages(dest_id, message.id, message.peer_id)
            return True, "Location"
        except Exception as e:
            logger.error(f"Location forward failed: {e}")
            return False, f"Location Error: {e}"
    
    # Handle Polls
    if isinstance(message.media, MessageMediaPoll):
        try:
            await bot_client.forward_messages(dest_id, message.id, message.peer_id)
            return True, "Poll"
        except Exception as e:
            logger.error(f"Poll forward failed: {e}")
            return False, f"Poll Error: {e}"
    
    # Handle Dice/Darts
    if isinstance(message.media, MessageMediaDice):
        try:
            await bot_client.forward_messages(dest_id, message.id, message.peer_id)
            return True, "Dice/Dart"
        except Exception as e:
            logger.error(f"Dice forward failed: {e}")
            return False, f"Dice Error: {e}"
    
    # Handle Games
    if isinstance(message.media, MessageMediaGame):
        try:
            await bot_client.forward_messages(dest_id, message.id, message.peer_id)
            return True, "Game"
        except Exception as e:
            logger.error(f"Game forward failed: {e}")
            return False, f"Game Error: {e}"
    
    # Handle Invoices/Payments
    if isinstance(message.media, MessageMediaInvoice):
        try:
            await bot_client.send_message(dest_id, f"⚠️ Payment Invoice (Cannot Transfer)\nDescription: {message.media.description}")
            return True, "Invoice (Info Only)"
        except Exception as e:
            return False, f"Invoice Error: {e}"
    
    # Fallback - Unknown media type
    logger.warning(f"Unknown media type: {type(message.media)}")
    return False, f"Unknown Media: {type(message.media)}"

# --- TRANSFER PROCESS (ALL CONTENT TYPES) ---
async def transfer_process(event, source_id, dest_id, start_msg, end_msg):
    global is_running, status_message
    
    status_message = await event.respond(
        f"🚀 **Transfer Started!**\n"
        f"📥 Source: `{source_id}`\n"
        f"📤 Destination: `{dest_id}`\n"
        f"📊 Range: `{start_msg}` to `{end_msg}`"
    )
    
    total_processed = 0
    total_success = 0
    total_failed = 0
    stats = {}
    
    try:
        async for message in user_client.iter_messages(
            source_id, 
            min_id=start_msg-1, 
            max_id=end_msg+1, 
            reverse=True
        ):
            if not is_running:
                await status_message.edit("🛑 **Transfer Stopped by User!**")
                break

            # Skip service messages (channel updates, etc.)
            if getattr(message, 'action', None):
                continue
            
            # Skip empty messages
            if not message.text and not message.media:
                continue

            # --- RETRY MECHANISM ---
            max_retries = 3
            success = False
            content_type = "Unknown"
            
            for attempt in range(max_retries):
                try:
                    # Refresh message to avoid file reference expiry
                    fresh_msg = await user_client.get_messages(source_id, ids=message.id)
                    if not fresh_msg:
                        logger.warning(f"Message {message.id} not found")
                        break
                    
                    # Update status
                    await status_message.edit(
                        f"📦 **Processing Message {message.id}**\n"
                        f"📊 Progress: {total_processed}/{end_msg-start_msg+1}\n"
                        f"✅ Success: {total_success} | ❌ Failed: {total_failed}\n"
                        f"🔄 Attempt: {attempt + 1}/{max_retries}"
                    )
                    
                    # Transfer the message
                    success, content_type = await handle_message_transfer(fresh_msg, dest_id)
                    
                    if success:
                        total_success += 1
                        stats[content_type] = stats.get(content_type, 0) + 1
                        break
                    
                except errors.FileReferenceExpiredError:
                    logger.warning(f"File reference expired for {message.id}, retrying...")
                    await asyncio.sleep(2)
                    continue
                    
                except errors.FloodWaitError as e:
                    logger.warning(f"FloodWait {e.seconds}s on message {message.id}")
                    await asyncio.sleep(e.seconds)
                    continue
                
                except Exception as e:
                    logger.error(f"Error processing message {message.id}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(3)
                    continue
            
            if not success:
                total_failed += 1
                try:
                    await bot_client.send_message(
                        event.chat_id, 
                        f"❌ **Failed to transfer message {message.id}** after {max_retries} attempts"
                    )
                except:
                    pass
            
            total_processed += 1
            
            # Periodic status update
            if total_processed % 10 == 0:
                stats_text = "\n".join([f"• {k}: {v}" for k, v in stats.items()])
                await status_message.edit(
                    f"📊 **Transfer Progress**\n"
                    f"Processed: {total_processed}\n"
                    f"✅ Success: {total_success}\n"
                    f"❌ Failed: {total_failed}\n\n"
                    f"**Content Types:**\n{stats_text}"
                )

        # Final summary
        if is_running:
            stats_text = "\n".join([f"• {k}: {v}" for k, v in stats.items()])
            await status_message.edit(
                f"✅ **Transfer Complete!**\n\n"
                f"📊 **Summary:**\n"
                f"Total Processed: {total_processed}\n"
                f"✅ Success: {total_success}\n"
                f"❌ Failed: {total_failed}\n\n"
                f"**Content Types Transferred:**\n{stats_text}"
            )

    except Exception as e:
        await status_message.edit(f"❌ **Critical Error:** {e}")
        logger.error(f"Transfer process error: {e}", exc_info=True)
    finally:
        is_running = False

# --- COMMANDS ---
@bot_client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.respond(
        "🔥 **Ultra Transfer Bot - All Content Supported!**\n\n"
        "**Supported Content:**\n"
        "📝 Text Messages\n"
        "🖼️ Photos\n"
        "🎥 Videos\n"
        "🎵 Audio/Music\n"
        "🎤 Voice Messages\n"
        "📄 Documents (PDF, ZIP, TXT, etc.)\n"
        "🎭 Stickers\n"
        "🎬 GIFs/Animations\n"
        "🔗 Links\n"
        "📍 Locations\n"
        "👤 Contacts\n"
        "📊 Polls\n"
        "🎲 Dice/Darts\n\n"
        "**Usage:**\n"
        "`/clone -100SourceID -100DestID`\n"
        "Then send message range links"
    )

@bot_client.on(events.NewMessage(pattern='/clone'))
async def clone_init(event):
    global is_running
    if is_running:
        return await event.respond("⚠️ Already processing another transfer! Use /stop first.")
    
    try:
        args = event.text.split()
        if len(args) < 3:
            return await event.respond("❌ Usage: `/clone -100SourceID -100DestID`")
        
        source_id = int(args[1])
        dest_id = int(args[2])
        
        pending_requests[event.chat_id] = {
            'source': source_id, 
            'dest': dest_id
        }
        
        await event.respond(
            "✅ **Settings Saved!**\n\n"
            f"📥 Source: `{source_id}`\n"
            f"📤 Destination: `{dest_id}`\n\n"
            "Now send the message range:\n"
            "Example: `https://t.me/c/xxx/10 - https://t.me/c/xxx/20`\n"
            "Or: `10-20`"
        )
    except ValueError:
        await event.respond("❌ Invalid IDs! Use format: `/clone -1001234567 -1007654321`")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")

@bot_client.on(events.NewMessage())
async def range_listener(event):
    global current_task, is_running
    
    if event.chat_id not in pending_requests:
        return
    
    # Check if message contains range info
    text = event.text.strip()
    if not text or event.text.startswith('/'):
        return
    
    try:
        # Parse range - support multiple formats
        if "t.me" in text:
            # Format: https://t.me/c/xxx/10 - https://t.me/c/xxx/20
            links = text.split("-")
            msg1 = int(links[0].strip().split("/")[-1])
            msg2 = int(links[1].strip().split("/")[-1])
        elif "-" in text:
            # Format: 10-20 or 10 - 20
            parts = text.split("-")
            msg1 = int(parts[0].strip())
            msg2 = int(parts[1].strip())
        elif " " in text:
            # Format: 10 20
            parts = text.split()
            msg1 = int(parts[0])
            msg2 = int(parts[1])
        else:
            return
        
        # Ensure correct order
        if msg1 > msg2:
            msg1, msg2 = msg2, msg1
        
        data = pending_requests.pop(event.chat_id)
        is_running = True
        
        await event.respond(f"🚀 Starting transfer of {msg2-msg1+1} messages...")
        
        current_task = asyncio.create_task(
            transfer_process(event, data['source'], data['dest'], msg1, msg2)
        )
        
    except ValueError:
        await event.respond("❌ Invalid range format! Use: `10-20` or `https://t.me/c/xxx/10 - https://t.me/c/xxx/20`")
    except Exception as e:
        await event.respond(f"❌ Error: {e}")
        is_running = False

@bot_client.on(events.NewMessage(pattern='/stop'))
async def stop_handler(event):
    global is_running, current_task
    
    if not is_running:
        return await event.respond("ℹ️ No active transfer to stop.")
    
    is_running = False
    
    if current_task and not current_task.done():
        current_task.cancel()
    
    await event.respond("🛑 **Transfer Stopped!**")

@bot_client.on(events.NewMessage(pattern='/status'))
async def status_handler(event):
    status = "🟢 Running" if is_running else "🔴 Idle"
    await event.respond(f"**Bot Status:** {status}")

# --- MAIN ---
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    
    # Start user client
    user_client.start()
    logger.info("✅ User client started")
    
    # Start web server
    loop.create_task(start_web_server())
    
    # Start bot
    bot_client.start(bot_token=BOT_TOKEN)
    logger.info("🚀 Bot is running and ready to transfer ALL content types!")
    
    # Run forever
    bot_client.run_until_disconnected()
