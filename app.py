import threading
import time
from datetime import datetime, timedelta
import asyncio
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
import json
import io
import os
import sqlite3
from dotenv import load_dotenv
import random
import re

from flask import Flask, render_template, Response, request, jsonify, send_file, url_for
from flask import redirect, flash, session
from flask_bcrypt import Bcrypt
from telethon import TelegramClient
from telethon.tl.types import Message, DocumentAttributeVideo, InputMessagesFilterPhotoVideo

import cv2
import tempfile
import os
import json
from werkzeug.utils import secure_filename
from telethon.tl.types import InputMediaUploadedPhoto, InputMediaUploadedDocument
from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeFilename

# Configuration
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
DATABASE_FILE = os.getenv("DATABASE_FILE")

# Flask app setup
app = Flask(__name__)
app.secret_key = os.urandom(24)
bcrypt = Bcrypt(app)

# Global variables for client management
client = None
loop = None
executor = None


def init_app():
    global client, loop, executor

    # Create event loop and executor
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=5)

    # Initialize Telethon client
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH, loop=loop)

    # Start the event loop in a separate thread
    def run_event_loop():
        asyncio.set_event_loop(loop)
        loop.run_forever()

    threading.Thread(target=run_event_loop, daemon=True).start()


# Database initialization
def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usr (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        is_admin INTEGER DEFAULT 0
    )
    ''')

    # Check if admin user exists
    cursor.execute("SELECT * FROM usr WHERE username = 'admin'")
    if not cursor.fetchone():
        # Create default admin user
        hashed_password = bcrypt.generate_password_hash('Welcome1').decode('utf-8')
        cursor.execute("INSERT INTO usr (username, password, is_admin) VALUES (?, ?, ?)",
                       ('admin', hashed_password, 1))

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS med (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        msg_id INTEGER,
        title TEXT,
        tags TEXT,
        file_size INTEGER,
        mime_type TEXT,
        duration INTEGER,
        width INTEGER,
        height INTEGER,
        thumbnail BLOB
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fav (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usr_id INTEGER,
        med_id INTEGER    
    )               
    ''')

    conn.commit()
    conn.close()


# Helper to run async functions in the shared event loop
def run_async(coro):
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result()


async def connect_client_if_needed():
    """Connect the client if not already connected"""
    if not client.is_connected():
        await client.connect()

    if not await client.is_user_authorized():
        print("You need to login to Telegram. Check your terminal for instructions.")
        phone = input("Enter your phone number with country code: ")
        await client.send_code_request(phone)
        code = input("Enter the code you received: ")
        await client.sign_in(phone, code)
        print("Telegram client initialized!")



#Fetch videos from Telegram channel and store metadata in SQLite
async def fetch_videos_from_channel():
    """Fetch videos from the Telegram channel and store metadata with thumbnail in SQLite"""
    # Ensure client is connected
    await connect_client_if_needed()

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    try:
        channel = await client.get_entity(CHANNEL_USERNAME)
        videos = []
        messages = []
        processed_group_ids = set()  # Track processed groups to avoid duplicates
        
        cursor.execute("SELECT MAX(msg_id) FROM med")
        last_msg_id = cursor.fetchone()[0] or 0
        
        async for msg in client.iter_messages(channel, min_id=last_msg_id):
        # async for msg in client.iter_messages(channel):
            messages.append(msg)
            print(f'Fetched {len(messages)} messages from tg')
        
        print(f'Starting message processing...')
        
        # Process all messages first
        for msg in messages:
            print(f'Processing message {msg.id}')
            
            # Check if the message contains media
            if not msg.media:
                continue

            # Handle grouped media
            if hasattr(msg, 'grouped_id') and msg.grouped_id:
                # Skip if we've already processed this group
                if msg.grouped_id in processed_group_ids:
                    continue
                
                processed_group_ids.add(msg.grouped_id)
                
                group_messages = [m for m in messages if
                                  hasattr(m, 'grouped_id') and m.grouped_id == msg.grouped_id]

                if msg not in group_messages:
                    group_messages.append(msg)

                # Process the entire group together
                await process_group_messages(group_messages, cursor, conn, videos, messages)

            # Handle single video message
            elif hasattr(msg.media, 'document') and msg.media.document.mime_type.startswith('video/'):
                video_doc = msg.media.document
                video_msg = msg
                json_data = {"media_name": None, "tags": None}

                # Check for JSON in the message text
                if msg.message:
                    try:
                        possible_json = msg.message.strip()
                        if possible_json.startswith('{') and possible_json.endswith('}'):
                            parsed_data = json.loads(possible_json)
                            if 'media_name' in parsed_data or 'tags' in parsed_data:
                                json_data = parsed_data
                    except json.JSONDecodeError:
                        pass

                # Check for adjacent thumbnail for single video messages
                adjacent_thumbnail = await find_adjacent_thumbnail(video_msg, messages, json_data)
                
                await process_single_video(
                    video_doc, video_msg, json_data, None, 
                    cursor, conn, videos, custom_thumbnail=adjacent_thumbnail
                )
        
        # Remove duplicates after processing all messages
        await remove_duplicate_videos(cursor, conn)

    except Exception as e:
        print(f"Error fetching videos: {e}")
    finally:
        conn.close()

    return videos


async def process_group_messages(group_messages, cursor, conn, videos, all_messages=None):
    """Process all messages in a group together"""
    video_docs = []
    image_msgs = []
    video_msgs = []
    json_data = {"media_name": None, "tags": None}
    
    # Collect all media from the group
    for group_msg in group_messages:
        if group_msg.message:
            try:
                possible_json = group_msg.message.strip()
                if possible_json.startswith('{') and possible_json.endswith('}'):
                    parsed_data = json.loads(possible_json)
                    if 'media_name' in parsed_data or 'tags' in parsed_data:
                        json_data = parsed_data
            except json.JSONDecodeError:
                pass
        
        if group_msg.photo:
            image_msgs.append(group_msg)
        elif group_msg.media and hasattr(group_msg.media, 'document'):
            doc = group_msg.media.document
            if doc.mime_type.startswith('video/'):
                video_docs.append(doc)
                video_msgs.append(group_msg)
    
    # Apply group processing logic
    if len(group_messages) == 1 and len(video_msgs) == 1:
        # Single group message with video - check adjacent messages for images
        video_doc = video_docs[0]
        video_msg = video_msgs[0]
        
        adjacent_thumbnail = await find_adjacent_thumbnail(video_msg, all_messages, json_data)
        
        await process_single_video(
            video_doc, video_msg, json_data, None, 
            cursor, conn, videos, custom_thumbnail=adjacent_thumbnail
        )
    
    elif len(group_messages) > 2:
        # More than 2 media files
        if len(image_msgs) == 1 and len(video_msgs) >= 1:
            # One photo and rest are videos - use same photo as thumbnail for all videos
            shared_thumbnail = await get_thumbnail_data(image_msgs[0])
            for i, (video_doc, video_msg) in enumerate(zip(video_docs, video_msgs)):
                await process_single_video(
                    video_doc, video_msg, json_data, None, 
                    cursor, conn, videos, group_index=i+1, total_in_group=len(video_docs),
                    custom_thumbnail=shared_thumbnail
                )
        else:
            # For any other condition, fetch thumbnail for each individual video
            for i, (video_doc, video_msg) in enumerate(zip(video_docs, video_msgs)):
                await process_single_video(
                    video_doc, video_msg, json_data, None, 
                    cursor, conn, videos, group_index=i+1, total_in_group=len(video_docs)
                )
    
    elif len(group_messages) == 2:
        # Exactly 2 media files
        if len(image_msgs) == 1 and len(video_msgs) == 1:
            # One photo and one video - use photo as thumbnail for video
            thumbnail_data = await get_thumbnail_data(image_msgs[0])
            await process_single_video(
                video_docs[0], video_msgs[0], json_data, None, 
                cursor, conn, videos, custom_thumbnail=thumbnail_data
            )
        elif len(video_msgs) == 2:
            # Both are videos
            video1_doc, video1_msg = video_docs[0], video_msgs[0]
            video2_doc, video2_msg = video_docs[1], video_msgs[1]
            
            # Get durations
            video1_attr = next((attr for attr in video1_doc.attributes
                               if isinstance(attr, DocumentAttributeVideo)), None)
            video2_attr = next((attr for attr in video2_doc.attributes
                               if isinstance(attr, DocumentAttributeVideo)), None)
            
            duration1 = video1_attr.duration if video1_attr else 0
            duration2 = video2_attr.duration if video2_attr else 0
            
            # Check if one video is less than 30 seconds and other is more than 5 minutes
            if ((duration1 < 30 and duration2 > 300) or (duration2 < 30 and duration1 > 300)):
                if duration1 < 30 and duration2 > 300:
                    # Use thumbnail of short video for the long video
                    short_thumbnail = await get_video_thumbnail(video1_doc, video1_msg)
                    await process_single_video(
                        video2_doc, video2_msg, json_data, None, 
                        cursor, conn, videos, custom_thumbnail=short_thumbnail
                    )
                    # Don't insert the short video
                else:
                    # Use thumbnail of short video for the long video
                    short_thumbnail = await get_video_thumbnail(video2_doc, video2_msg)
                    await process_single_video(
                        video1_doc, video1_msg, json_data, None, 
                        cursor, conn, videos, custom_thumbnail=short_thumbnail
                    )
                    # Don't insert the short video
            else:
                # For any other condition, fetch thumbnail for both videos separately
                await process_single_video(
                    video1_doc, video1_msg, json_data, None, 
                    cursor, conn, videos, group_index=1, total_in_group=2
                )
                await process_single_video(
                    video2_doc, video2_msg, json_data, None, 
                    cursor, conn, videos, group_index=2, total_in_group=2
                )
        else:
            # Process all videos normally
            for i, (video_doc, video_msg) in enumerate(zip(video_docs, video_msgs)):
                await process_single_video(
                    video_doc, video_msg, json_data, None, 
                    cursor, conn, videos, group_index=i+1, total_in_group=len(video_docs)
                )
    else:
        # Single media or other cases
        for i, (video_doc, video_msg) in enumerate(zip(video_docs, video_msgs)):
            await process_single_video(
                video_doc, video_msg, json_data, None, 
                cursor, conn, videos, group_index=i+1, total_in_group=len(video_docs)
            )


async def find_adjacent_thumbnail(video_msg, all_messages, json_data):
    """Find thumbnail from adjacent messages for single group video"""
    if not all_messages:
        return None
    
    # Find the index of the video message
    video_msg_index = -1
    for i, msg in enumerate(all_messages):
        if msg.id == video_msg.id:
            video_msg_index = i
            break
    
    if video_msg_index == -1:
        return None
    
    # Check previous and next messages for single image
    prev_msg = all_messages[video_msg_index - 1] if video_msg_index > 0 else None
    next_msg = all_messages[video_msg_index + 1] if video_msg_index < len(all_messages) - 1 else None
    
    # Helper function to check if message contains only an image
    def is_single_image_message(msg):
        if not msg or not msg.photo:
            return False
        # Check if it's not part of a group or has other media
        if hasattr(msg, 'grouped_id') and msg.grouped_id:
            return False
        return True
    
    prev_is_image = is_single_image_message(prev_msg)
    next_is_image = is_single_image_message(next_msg)
    
    # If no adjacent image messages found
    if not prev_is_image and not next_is_image:
        return None
    
    # If only one adjacent image message
    if prev_is_image and not next_is_image:
        return await get_thumbnail_data(prev_msg)
    elif next_is_image and not prev_is_image:
        return await get_thumbnail_data(next_msg)
    
    # If both previous and next contain single image messages
    if prev_is_image and next_is_image:
        # Get first word from video message text
        video_text = video_msg.message or ""
        
        # Try to parse JSON first to get media_name
        first_word = None
        try:
            if video_text.strip().startswith('{') and video_text.strip().endswith('}'):
                parsed_data = json.loads(video_text.strip())
                media_name = parsed_data.get('media_name', '')
                if media_name:
                    # Get first word from media_name
                    first_word = extract_first_word(media_name)
        except json.JSONDecodeError:
            pass
        
        # If no first word from JSON, get from raw text
        if not first_word:
            first_word = extract_first_word(video_text)
        
        if first_word:
            # Check if the word matches in previous message text
            prev_text = prev_msg.message or ""
            next_text = next_msg.message or ""
            
            if word_matches_in_text(first_word, prev_text):
                return await get_thumbnail_data(prev_msg)
            elif word_matches_in_text(first_word, next_text):
                return await get_thumbnail_data(next_msg)
        
        # If no match found, use previous message image
        return await get_thumbnail_data(prev_msg)
    
    return None


async def get_thumbnail_data(image_msg):
    """Get thumbnail data from an image message"""
    try:
        thumbnail_buffer = io.BytesIO()
        await client.download_media(image_msg, thumbnail_buffer)
        return thumbnail_buffer.getvalue()
    except Exception as e:
        print(f"Error downloading thumbnail from image: {e}")
        os._exit(1)
    

def extract_first_word(text):
    """Extract the first meaningful word from text in any language"""
    if not text:
        return None
    
    # Remove JSON formatting and clean the text
    text = text.strip()
    
    # Remove common punctuation and split by various separators
    import string
    # Extended punctuation for various languages
    punctuation = string.punctuation + '，。！？；：「」『』（）［］｛｝〈〉《》【】〔〕'
    
    # Replace punctuation with spaces
    for char in punctuation:
        text = text.replace(char, ' ')
    
    # Split by whitespace and get first non-empty word
    words = text.split()
    for word in words:
        word = word.strip()
        if word and len(word) > 1:  # Ignore single characters
            return word.lower()
    
    return None


def word_matches_in_text(word, text):
    """Check if word matches in text (case-insensitive, supports multiple languages)"""
    if not word or not text:
        return False
    
    # Convert to lowercase for comparison
    word_lower = word.lower()
    text_lower = text.lower()
    
    # Simple contains check
    if word_lower in text_lower:
        return True
    
    # Check as separate word (with word boundaries)
    import re
    
    # Create pattern that works with various languages
    # Use word boundaries where possible, fallback to space/punctuation boundaries
    patterns = [
        rf'\b{re.escape(word_lower)}\b',  # Standard word boundaries
        rf'(?:^|\s){re.escape(word_lower)}(?:\s|$)',  # Space boundaries
        rf'(?:^|[^\w]){re.escape(word_lower)}(?:[^\w]|$)',  # Non-word char boundaries
    ]
    
    for pattern in patterns:
        try:
            if re.search(pattern, text_lower, re.IGNORECASE | re.UNICODE):
                return True
        except re.error:
            continue
    
    return False


async def get_video_thumbnail(video_doc, video_msg):
    """Get thumbnail data from a video"""
    try:
        if video_doc.thumbs:
            best_thumb = max(video_doc.thumbs, key=lambda t: getattr(t, 'w', 0) * getattr(t, 'h', 0))
            thumbnail_buffer = io.BytesIO()
            await client.download_media(
                message=video_msg,
                file=thumbnail_buffer,
                thumb=video_doc.thumbs.index(best_thumb)
            )
            return thumbnail_buffer.getvalue()
    except Exception as e:
        print(f"Error downloading video thumbnail: {e}")
    return None


async def process_single_video(video_doc, video_msg, json_data, image_msg, cursor, conn, videos, 
                              group_index=None, total_in_group=None, custom_thumbnail=None):
    """Process a single video and insert into database"""
    
    # Get video attributes
    video_attr = next((attr for attr in video_doc.attributes
                       if isinstance(attr, DocumentAttributeVideo)), None)

    # Get video ID
    video_id = video_msg.id
    
    # Check if video already exists in database
    cursor.execute("SELECT id FROM med WHERE msg_id = ?", (video_id,))
    if cursor.fetchone():
        print(f"Video {video_id} already in database, skipping")
        return
    
    try:
        video_name = video_msg.document.attributes[1].file_name
        words = re.split(r'[^a-zA-Z0-9]+', video_name)
        cleaned = [word for word in words if word]
        tags_from_title = ', '.join(cleaned)
    except:
        return

    # Get duration for thumbnail logic
    duration = video_attr.duration if video_attr else 0

    # Prepare video info for database
    base_title = json_data.get('media_name', None) or f'{video_name}'
    
    # Add group index to title if it's part of a group with multiple videos
    if group_index and total_in_group > 1:
        title = f"{base_title} (Part {group_index}/{total_in_group})"
    else:
        title = base_title
        
    tags = json_data.get('tags', "") or f'{tags_from_title}'
    
    video_info = {
        'msg_id': video_id,
        'file_size': video_doc.size,
        'mime_type': video_doc.mime_type,
        'duration': duration,
        'width': video_attr.w if video_attr else 0,
        'height': video_attr.h if video_attr else 0,
        'title': title,
        'tags': tags
    }
    
    # Check for repeated messages in database
    cursor.execute("SELECT id FROM med WHERE title = ? and file_size = ?", (title, video_doc.size))
    if cursor.fetchone():
        print(f"Video {video_id} already in database, skipping")
        return

    # Download thumbnail as binary data
    thumbnail_data = None
    
    # Only download thumbnail for videos longer than 3 minutes (180 seconds)
    if duration >= 180:
        if custom_thumbnail:
            # Use provided custom thumbnail
            thumbnail_data = custom_thumbnail
        else:
            # Download thumbnail normally
            try:
                if image_msg:
                    # Use BytesIO to capture the image data directly
                    thumbnail_buffer = io.BytesIO()
                    await client.download_media(image_msg, thumbnail_buffer)
                    thumbnail_data = thumbnail_buffer.getvalue()

                elif video_doc.thumbs:
                    # Fallback to video's built-in thumbnail
                    thumbnail_data = await get_video_thumbnail(video_doc, video_msg)
            except Exception as e:
                print(f"Error downloading thumbnail for video {video_id}: {e}")

    # Insert data into SQLite
    try:
        cursor.execute('''
        INSERT INTO med (msg_id, title, tags, file_size, mime_type, duration, width, height, thumbnail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video_info['msg_id'],
            video_info['title'],
            video_info['tags'],
            video_info['file_size'],
            video_info['mime_type'],
            video_info['duration'],
            video_info['width'],
            video_info['height'],
            thumbnail_data
        ))
        conn.commit()
        print(f'Inserted {title} to db')
        # Add to return list (without the binary data for cleaner output)
        videos.append(video_info)
        # time.sleep(10)
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")


async def remove_duplicate_videos(cursor, conn):
    """Remove duplicate videos based on media name and file size"""
    print("Removing duplicate videos...")
    
    # Find duplicates based on title and file_size
    cursor.execute('''
        SELECT title, file_size, COUNT(*) as count, GROUP_CONCAT(id) as ids
        FROM med 
        GROUP BY title, file_size 
        HAVING COUNT(*) > 1
    ''')
    
    duplicates = cursor.fetchall()
    
    for title, file_size, count, ids_str in duplicates:
        ids = ids_str.split(',')
        # Keep the first record (lowest ID) and delete the rest
        ids_to_delete = ids[1:]  # Skip the first ID
        
        for id_to_delete in ids_to_delete:
            cursor.execute("DELETE FROM med WHERE id = ?", (id_to_delete,))
            print(f"Deleted duplicate video with ID {id_to_delete} (title: {title}, size: {file_size})")
    
    conn.commit()
    print(f"Removed {sum(len(ids.split(',')) - 1 for _, _, _, ids in duplicates)} duplicate videos")


def schedule_fetch(interval_seconds=86400):  # Default 2 days
    """Schedule periodic fetches of videos from the channel"""
    while True:
        try:
            print(f"Starting video fetch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            run_async(fetch_videos_from_channel())
            print(
                f"Next fetch scheduled at {(datetime.now() + timedelta(seconds=interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"Error in scheduled fetch: {e}")

        time.sleep(interval_seconds)


# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please login first', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated_function


# Admin required decorator
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'is_admin' not in session or not session['is_admin']:
            flash('Admin access required', 'error')
            return redirect(url_for('home'))
        return f(*args, **kwargs)

    return decorated_function


@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('home'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = sqlite3.connect(DATABASE_FILE)  # Use consistent database file
        cursor = conn.cursor()
        cursor.execute("SELECT id, password, is_admin FROM usr WHERE username = ?", (username,))
        user = cursor.fetchone()
        conn.close()

        if user and bcrypt.check_password_hash(user[1], password):
            session['user_id'] = user[0]
            session['username'] = username
            session['is_admin'] = user[2]
            flash('Login successful', 'success')
            return redirect(url_for('home'))
        else:
            flash('Invalid username or password', 'error')

    return render_template('login.html')


@app.route('/home')
@login_required
def home():
    page = request.args.get('page', 1, type=int)
    refresh = request.args.get('refresh', 0, type=int)
    per_page = 100  # Videos per page
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Get total count of videos
    cursor.execute("SELECT COUNT(*) FROM med WHERE duration > 180")
    total_videos = cursor.fetchone()[0]
    
    # Calculate total pages
    total_pages = (total_videos + per_page - 1) // per_page  # Ceiling division
    
    # Ensure page is within valid range
    if page < 1:
        page = 1
    elif page > total_pages and total_pages > 0:
        page = total_pages
    
    # Get all videos to shuffle
    cursor.execute("SELECT id, msg_id, title, tags, duration FROM med WHERE duration > 180 ORDER BY id DESC")
    all_videos = cursor.fetchall()
    conn.close()
    
    # Store only a single int in session — no cookie overflow
    if refresh or 'shuffle_seed' not in session:
        session['shuffle_seed'] = random.randint(0, 2**31)

    # Reproduce the same shuffle consistently using the seed
    rng = random.Random(session['shuffle_seed'])
    rng.shuffle(all_videos)

    start_idx = (page - 1) * per_page
    videos = all_videos[start_idx : start_idx + per_page]
    
    return render_template('home.html', 
                           username=session.get('username'), 
                           videos=videos, 
                           page=page, 
                           total_pages=total_pages)


@app.route('/admin')
@login_required
@admin_required
def admin():
    conn = sqlite3.connect(DATABASE_FILE)  # Use consistent database file
    cursor = conn.cursor()
    cursor.execute("SELECT id, username, is_admin FROM usr")
    users = cursor.fetchall()
    conn.close()

    return render_template('admin.html', users=users)


@app.route('/admin/create_user', methods=['POST'])
@login_required
@admin_required
def create_user():
    username = request.form['username']
    password = request.form['password']
    is_admin = 1 if request.form.get('is_admin') else 0

    if not username or not password:
        flash('Username and password are required', 'error')
        return redirect(url_for('admin'))

    try:
        conn = sqlite3.connect(DATABASE_FILE)  # Use consistent database file
        cursor = conn.cursor()
        hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
        cursor.execute("INSERT INTO usr (username, password, is_admin) VALUES (?, ?, ?)",
                       (username, hashed_password, is_admin))
        conn.commit()
        conn.close()
        flash('User created successfully', 'success')
    except sqlite3.IntegrityError:
        flash('Username already exists', 'error')

    return redirect(url_for('admin'))


@app.route('/admin/delete_user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    if session.get('user_id') == user_id:
        flash('Cannot delete your own account', 'error')
        return redirect(url_for('admin'))

    conn = sqlite3.connect(DATABASE_FILE)  # Use consistent database file
    cursor = conn.cursor()
    cursor.execute("DELETE FROM usr WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    flash('User deleted successfully', 'success')
    return redirect(url_for('admin'))


@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out', 'success')
    return redirect(url_for('login'))


@app.route('/video/<int:msg_id>')
@login_required
def view_video(msg_id):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, msg_id, title, tags, duration, mime_type FROM med WHERE msg_id = ?", (msg_id,))
    video = cursor.fetchone()
    
    # Check if video is in user's favorites
    is_favorite = False
    if video:
        cursor.execute("SELECT id FROM fav WHERE usr_id = ? AND med_id = ?", 
                      (session.get('user_id'), video[0]))
        is_favorite = cursor.fetchone() is not None
    
    conn.close()

    if not video:
        flash('Video not found', 'error')
        return redirect(url_for('home'))

    return render_template('video.html', video=video, is_favorite=is_favorite)


@app.route('/thumbnail/<int:msg_id>')
@login_required
def get_thumbnail(msg_id):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT thumbnail FROM med WHERE msg_id = ?", (msg_id,))
    result = cursor.fetchone()
    conn.close()

    if not result or not result[0]:
        return send_file('static/default-thumbnail.jpg', mimetype='image/jpeg')

    return Response(result[0], mimetype='image/jpeg')


@app.route('/stream/<int:msg_id>')
@login_required
def stream_video(msg_id):
    # This function will handle streaming the video from Telegram
    # We'll need to implement proper streaming with range requests
    async def get_video_chunk(msg_id, offset=0, limit=1024*1024*10):
        """Get a chunk of video data for streaming"""
        channel = await client.get_entity(CHANNEL_USERNAME)
        message = await client.get_messages(channel, ids=msg_id)
        
        if not message or not message.media:
            return None, None, 0
        
        doc = message.media.document
        total_size = doc.size
        
        # Use client.iter_download instead of download_media for streaming
        buffer = b''
        async for chunk in client.iter_download(message.media, offset=offset, request_size=limit):
            buffer += chunk
            if len(buffer) >= limit:
                break
        
        return buffer, doc.mime_type, total_size

    # Get range header
    range_header = request.headers.get('Range', 'bytes=0-')
    offset = int(range_header.replace('bytes=', '').split('-')[0])
    chunk_size = 1024 * 1024 * 10 # 10MB chunks

    
    try:
        # Get the video chunk
        chunk, mime_type, total_size = run_async(get_video_chunk(msg_id, offset, chunk_size))
        
        if not chunk:
            return "Error streaming video", 500
            
        # Calculate end byte position
        end = min(offset + len(chunk) - 1, total_size - 1)
        
        # Create response
        response = Response(
            chunk,
            206,  # Partial Content
            mimetype=mime_type,
            direct_passthrough=True
        )
        
        # Set response headers
        response.headers.add('Content-Range', f'bytes {offset}-{end}/{total_size}')
        response.headers.add('Accept-Ranges', 'bytes')
        response.headers.add('Content-Length', str(len(chunk)))
        
        return response
    
    except Exception as e:
        print(f"Error streaming video: {e}")
        return "Error streaming video", 500


@app.route('/favourites')
@login_required
def favourites():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    # Join med and fav tables to get user's favorite videos
    cursor.execute("""
        SELECT med.id, med.msg_id, med.title, med.tags, med.duration 
        FROM med 
        INNER JOIN fav ON med.id = fav.med_id 
        WHERE fav.usr_id = ? 
        ORDER BY fav.id DESC
    """, (session.get('user_id'),))
    videos = cursor.fetchall()
    random.shuffle(videos)
    conn.close()

    return render_template('favourites.html', username=session.get('username'), videos=videos)


@app.route('/add_favourite/<int:med_id>', methods=['POST'])
@login_required
def add_favourite(med_id):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Check if already in favorites
    cursor.execute("SELECT id FROM fav WHERE usr_id = ? AND med_id = ?", 
                  (session.get('user_id'), med_id))
    existing = cursor.fetchone()
    
    if existing:
        flash('Video already in favourites', 'info')
    else:
        cursor.execute("INSERT INTO fav (usr_id, med_id) VALUES (?, ?)", 
                      (session.get('user_id'), med_id))
        conn.commit()
        flash('Added to favourites', 'success')
    
    conn.close()
    return redirect(request.referrer or url_for('home'))


@app.route('/remove_favourite/<int:med_id>', methods=['POST'])
@login_required
def remove_favourite(med_id):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM fav WHERE usr_id = ? AND med_id = ?", 
                  (session.get('user_id'), med_id))
    conn.commit()
    conn.close()
    flash('Removed from favourites', 'success')
    return redirect(request.referrer or url_for('favourites'))


@app.route('/upload', methods=['GET'])
@login_required
def upload_page():
    return render_template('upload.html')


@app.route('/generate_thumbnail', methods=['POST'])
@login_required
def generate_thumbnail():
    if 'video' not in request.files:
        return jsonify({'error': 'No video file uploaded'}), 400

    video_file = request.files['video']
    timestamp = request.form.get('timestamp', 'random')
    
    # Create temp file with unique name for video
    temp_video_fd, video_path = tempfile.mkstemp(suffix='.mp4')
    os.close(temp_video_fd)  # Close the file descriptor
    
    # Create temp file with unique name for thumbnail
    temp_thumb_fd, thumbnail_path = tempfile.mkstemp(suffix='.jpg')
    os.close(temp_thumb_fd)  # Close the file descriptor
    
    try:
        # Save the uploaded video to the temp file
        video_file.save(video_path)
        
        # Open video with OpenCV
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            cap.release()
            os.unlink(video_path)
            os.unlink(thumbnail_path)
            return jsonify({'error': 'Could not open video file'}), 400
        
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        
        if frame_count <= 0:
            cap.release()
            os.unlink(video_path)
            os.unlink(thumbnail_path)
            return jsonify({'error': 'Invalid video file'}), 400
        
        # Get timestamp for the frame
        if timestamp == 'random':
            frame_number = int(frame_count * 0.3)  # At 30% of the video
        else:
            try:
                # Convert timestamp (in seconds) to frame number
                frame_number = int(float(timestamp) * fps)
                if frame_number >= frame_count:
                    frame_number = int(frame_count / 2)
            except:
                frame_number = int(frame_count / 2)
        
        # Seek to the specific frame
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        ret, frame = cap.read()
        
        # Release the video capture
        cap.release()
        
        if not ret:
            os.unlink(video_path)
            os.unlink(thumbnail_path)
            return jsonify({'error': 'Could not extract frame from video'}), 400
        
        # Save thumbnail
        cv2.imwrite(thumbnail_path, frame)
        
        # Read thumbnail as base64
        with open(thumbnail_path, 'rb') as f:
            thumbnail_data = f.read()
        
        import base64
        thumbnail_base64 = base64.b64encode(thumbnail_data).decode('utf-8')
        
        # Clean up temporary files
        os.unlink(video_path)
        os.unlink(thumbnail_path)
        
        return jsonify({
            'thumbnail': f'data:image/jpeg;base64,{thumbnail_base64}',
            'width': width,
            'height': height
        })
        
    except Exception as e:
        # Make sure to clean up resources in case of exception
        if 'cap' in locals() and cap is not None:
            cap.release()
        
        # Try to clean up temporary files
        try:
            os.unlink(video_path)
        except:
            pass
            
        try:
            os.unlink(thumbnail_path)
        except:
            pass
            
        return jsonify({'error': f'Error generating thumbnail: {str(e)}'}), 500
    

@app.route('/upload_video', methods=['POST'])
# @login_required
def upload_video():
    if 'video' not in request.files:
        return jsonify({'error': 'No video file uploaded'}), 400
    
    video_file = request.files['video']
    title = request.form.get('title', '')
    tags = request.form.get('tags', '')
    thumbnail_data = None
    
    # Check if a thumbnail was uploaded
    if 'thumbnail' in request.files and request.files['thumbnail'].filename:
        thumbnail_file = request.files['thumbnail']
        thumbnail_data = thumbnail_file.read()
    # Otherwise use the base64 thumbnail data
    elif 'thumbnail_data' in request.form and request.form['thumbnail_data']:
        import base64
        thumbnail_base64 = request.form['thumbnail_data'].split(',')[1]
        thumbnail_data = base64.b64decode(thumbnail_base64)
    else:
        return jsonify({'error': 'No thumbnail provided'}), 400
    
    # Check file size (1GB limit)
    video_file.seek(0, os.SEEK_END)
    file_size = video_file.tell()
    video_file.seek(0)
    
    if file_size > 1024 * 1024 * 1024:  # 1GB
        return jsonify({'error': 'Video file too large (max 1GB)'}), 400
    
    # Check file extension
    filename = secure_filename(video_file.filename)
    if not filename.lower().endswith('.mp4'):
        return jsonify({'error': 'Only MP4 videos are supported'}), 400
    
    # Create temporary files
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mp4') as temp_video, \
         tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as temp_thumb:
        
        video_path = temp_video.name
        thumbnail_path = temp_thumb.name
        
        # Save files
        video_file.save(video_path)
        with open(thumbnail_path, 'wb') as f:
            f.write(thumbnail_data)
    
    try:
        # Get video metadata using OpenCV
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            cap.release()  # Make sure to release before any return or exception
            os.unlink(video_path)
            os.unlink(thumbnail_path)
            return jsonify({'error': 'Could not open video file'}), 400
        
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        duration = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) / cap.get(cv2.CAP_PROP_FPS))
        
        # Important: Release the video capture BEFORE trying to delete the file
        cap.release()
        
        # Create JSON message
        message_json = {
            "media_name": title,
            "tags": tags
        }
        
        # Upload to Telegram using async function
        async def upload_to_telegram():
            await connect_client_if_needed()
            
            # Get channel entity
            channel = await client.get_entity(CHANNEL_USERNAME)
            
            # Upload thumbnail image first
            thumbnail_file = await client.upload_file(thumbnail_path, file_name="thumbnail.jpg")
            thumb_media = InputMediaUploadedPhoto(thumbnail_file)
            
            # Upload video file
            video_file = await client.upload_file(video_path, file_name=filename)
            
            # Video attributes
            attributes = [
                DocumentAttributeVideo(
                    duration=duration,
                    w=width,
                    h=height,
                    supports_streaming=True
                ),
                DocumentAttributeFilename(filename)
            ]
            
            video_media = InputMediaUploadedDocument(
                file=video_file,
                mime_type="video/mp4",
                attributes=attributes,
                thumb=thumbnail_file
            )
            
            # Send as a group message
            await client.send_message(
                channel,
                json.dumps(message_json),
                file=video_media
            )
        
        # Run the async function
        run_async(upload_to_telegram())
        
        # Clean up temporary files
        os.unlink(video_path)
        os.unlink(thumbnail_path)
        
        return jsonify({'success': True, 'message': 'Video uploaded successfully'})
        
    except Exception as e:
        # Make sure cap is released if it exists
        try:
            if 'cap' in locals() and cap is not None:
                cap.release()
        except:
            pass
            
        # Clean up temporary files
        try:
            os.unlink(video_path)
        except:
            pass
            
        try:
            os.unlink(thumbnail_path)
        except:
            pass
            
        return jsonify({'error': str(e)}), 500


@app.route('/shorts')
@login_required
def shorts():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    # Select only videos less than 3 minutes (180 seconds)
    cursor.execute("""
        SELECT id, msg_id, title, tags, duration, mime_type
        FROM med 
        WHERE duration < 180 
        ORDER BY id DESC
    """)
    short_videos = cursor.fetchall()
    
    # Convert to list of dictionaries for easier handling in template
    videos = []
    for video in short_videos:
        # Check if in favorites
        cursor.execute("SELECT id FROM fav WHERE usr_id = ? AND med_id = ?", 
                      (session.get('user_id'), video[0]))
        is_favorite = cursor.fetchone() is not None
        
        videos.append({
            'id': video[0],
            'msg_id': video[1],
            'title': video[2],
            'tags': video[3].split(',') if video[3] else [],
            'duration': video[4],
            'is_favorite': is_favorite
        })
    random.shuffle(videos)
    conn.close()
    return render_template('shorts.html', videos=videos)

           
@app.route('/api/videos/by_tag/<tag>')
@login_required
def get_videos_by_tag(tag):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Use LIKE with wildcards to match partial tags in the tags field
    # This will match "tag" both as a standalone tag and as part of a comma-separated list
    cursor.execute("""
        SELECT id, msg_id, title, tags, duration 
        FROM med 
        WHERE (tags LIKE ? OR tags LIKE ? OR tags LIKE ? OR tags = ?)
        AND thumbnail IS NOT NULL
        ORDER BY id DESC
        LIMIT 10
    """, (f"{tag},%", f"%, {tag},%", f"%, {tag}", tag))

    videos = cursor.fetchall()
    
    # Convert to JSON friendly format
    result = []
    for video in videos:
        # Check if in favorites
        cursor.execute("SELECT id FROM fav WHERE usr_id = ? AND med_id = ?", 
                      (session.get('user_id'), video[0]))
        is_favorite = cursor.fetchone() is not None
        
        result.append({
            'id': video[0],
            'msg_id': video[1],
            'title': video[2],
            'tags': video[3].split(',') if video[3] else [],
            'duration': video[4],
            'is_favorite': is_favorite
        })
    
    conn.close()
    return jsonify(result)


@app.route('/search')
@login_required
def search():
    query = request.args.get('q', '').strip()
    page = request.args.get('page', 1, type=int)
    per_page = 100  # Videos per page
    
    if not query:
        return redirect(url_for('home'))
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Get total count of matching videos
    cursor.execute("""
        SELECT COUNT(*) 
        FROM med 
        WHERE lower(title) LIKE ? OR lower(tags) LIKE ?
    """, (f'%{query.lower()}%', f'%{query.lower()}%'))
    total_videos = cursor.fetchone()[0]
    
    # Calculate total pages
    total_pages = (total_videos + per_page - 1) // per_page  # Ceiling division
    
    # Ensure page is within valid range
    if page < 1:
        page = 1
    elif page > total_pages and total_pages > 0:
        page = total_pages
    
    # Calculate offset for pagination
    offset = (page - 1) * per_page
    
    # Search for videos where title or tags contain the query (case insensitive)
    cursor.execute("""
        SELECT id, msg_id, title, tags, duration 
        FROM med 
        WHERE lower(title) LIKE ? OR lower(tags) LIKE ?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (f'%{query.lower()}%', f'%{query.lower()}%', per_page, offset))
    
    videos = cursor.fetchall()
    conn.close()
    
    return render_template('search_results.html', 
                          videos=videos, 
                          query=query, 
                          page=page, 
                          total_pages=total_pages)

                                 
if __name__ == '__main__':
    # Initialize database
    init_db()

    # Initialize app components (event loop, client, etc.)
    init_app()

    # Start scheduler in a separate thread
    scheduler_thread = threading.Thread(
        target=schedule_fetch,
        kwargs={'interval_seconds': 604800},  # 1 week
        daemon=True
    )
    scheduler_thread.start()

    # Run Flask app
    app.run(debug=True, port=5001, threaded=True, use_reloader=False)  # use_reloader=False to prevent duplicate threads