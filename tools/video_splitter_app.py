import os
import math
import time
import shutil
import threading
import logging
import asyncio
import uuid
import glob
from pathlib import Path
from threading import Thread
from datetime import datetime, timedelta
from flask import Flask, request, render_template, jsonify, send_file, session
from werkzeug.utils import secure_filename
from zipfile import ZipFile
import io
import subprocess
from telethon import TelegramClient, functions, types
from telethon.errors import RPCError
from dotenv import load_dotenv
from flask_session import Session
import secrets
import psutil

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MAX_SIZE_MB = 1700  # 1.7GB split size
PART_SIZE_KB = 1024 * 50  # 50MB chunks for faster uploads
ALLOWED_EXTENSIONS = {'mp4', 'avi', 'mov', 'mkv', 'webm'}

# Telegram credentials
api_id = int(os.getenv("API_ID", 0))
api_hash = os.getenv("API_HASH", "")
if not api_id or not api_hash:
    logger.error("Telegram API credentials not found in environment variables")

# Flask setup
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", secrets.token_hex(32))
app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024 * 1024  # 100 GB limit
app.config['UPLOAD_FOLDER'] = os.path.abspath('uploads')
app.config['BASE_SPLIT_FOLDER'] = os.path.abspath(os.path.expanduser('~/Downloads/video_splitter'))
app.config['ALLOWED_EXTENSIONS'] = ALLOWED_EXTENSIONS
app.config['SESSION_TYPE'] = 'filesystem'
app.config['SESSION_FILE_DIR'] = './flask_session'
app.config['CLEANUP_INTERVAL'] = 300  # Cleanup every 5 minutes

# Create directories if they don't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['BASE_SPLIT_FOLDER'], exist_ok=True)
os.makedirs(app.config['SESSION_FILE_DIR'], exist_ok=True)
logger.info(f"Upload folder: {app.config['UPLOAD_FOLDER']}")
logger.info(f"Split folder: {app.config['BASE_SPLIT_FOLDER']}")

# Initialize session
Session(app)

# Global dictionaries
progress_dict = {}
session_files = {}  # Track files by session
upload_status = {}  # For Telegram uploads


def ensure_session_files(session_id):
    """Ensure session files storage exists for the given session ID"""
    if session_id not in session_files:
        session_files[session_id] = {
            'uploads': [],
            'splits': []
        }
        logger.info(f"Initialized session files storage for session: {session_id}")
    return session_files[session_id]


def start_cleanup_thread():
    """Start background cleanup thread"""
    def cleanup_task():
        while True:
            try:
                cleanup_old_files()
            except Exception as e:
                logger.error(f"Cleanup task error: {e}")
            time.sleep(app.config['CLEANUP_INTERVAL'])

    thread = threading.Thread(target=cleanup_task, daemon=True)
    thread.start()
    logger.info("Started background cleanup thread")


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def cleanup_folder(folder_path):
    """Remove folder and its contents"""
    try:
        shutil.rmtree(folder_path)
        logger.info(f"Cleaned up folder: {folder_path}")
        return True
    except Exception as e:
        logger.error(f"Error cleaning up folder {folder_path}: {e}")
        return False


def cleanup_old_files():
    """Clean up files older than 1 hour"""
    now = time.time()
    cutoff = now - 3600  # 1 hour

    # Clean upload folder
    for filename in os.listdir(app.config['UPLOAD_FOLDER']):
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff:
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up old upload: {file_path}")
            except Exception as e:
                logger.error(f"Error cleaning up old upload {file_path}: {e}")

    # Clean split folders
    for folder in os.listdir(app.config['BASE_SPLIT_FOLDER']):
        folder_path = os.path.join(app.config['BASE_SPLIT_FOLDER'], folder)
        if os.path.isdir(folder_path) and os.path.getmtime(folder_path) < cutoff:
            try:
                cleanup_folder(folder_path)
            except Exception as e:
                logger.error(f"Error cleaning up old split folder {folder_path}: {e}")


def create_zip(folder_path):
    """Create a zip file from folder contents"""
    memory_file = io.BytesIO()
    with ZipFile(memory_file, 'w') as zf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zf.write(file_path, arcname)
    memory_file.seek(0)
    return memory_file


def get_video_duration(filename):
    """Get video duration in seconds using ffprobe"""
    try:
        result = subprocess.run([
            'ffprobe', '-v', 'error', '-show_entries',
            'format=duration', '-of',
            'default=noprint_wrappers=1:nokey=1', filename
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return float(result.stdout)
    except Exception as e:
        logger.error(f"Error getting video duration: {e}")
        return None


def split_video_with_ffmpeg(input_path, output_folder, part_size_mb=MAX_SIZE_MB):
    """Split video properly using ffmpeg"""
    filename = os.path.basename(input_path)
    name, ext = os.path.splitext(filename)

    # Get video duration
    duration = get_video_duration(input_path)
    if duration is None:
        logger.error(f"Could not determine duration for {input_path}")
        return None

    # Calculate split points (in seconds)
    file_size = os.path.getsize(input_path)
    part_size_bytes = part_size_mb * 1024 * 1024  # Convert MB to bytes
    total_parts = math.ceil(file_size / part_size_bytes)
    part_duration = duration / total_parts

    part_files = []

    for i in range(total_parts):
        part_filename = f"{name}_part{i+1}{ext}"
        part_path = os.path.join(output_folder, part_filename)

        start_time = i * part_duration
        end_time = (i + 1) * part_duration if i < total_parts - 1 else None

        cmd = [
            'ffmpeg', '-i', input_path,
            '-ss', str(start_time),
            '-c', 'copy'  # Use stream copy for no re-encoding
        ]

        if end_time is not None:
            cmd.extend(['-to', str(end_time)])

        cmd.append(part_path)

        try:
            logger.info(f"Splitting part {i+1}/{total_parts}: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            part_files.append(part_filename)

            # Update progress
            progress = ((i + 1) / total_parts) * 100
            progress_dict[filename] = progress
            logger.info(f"Created part {part_filename}, progress: {progress:.2f}%")
        except subprocess.CalledProcessError as e:
            error_msg = f"Error splitting video: {e.stderr.decode('utf-8') if e.stderr else str(e)}"
            logger.error(error_msg)
            return None

    return part_files


class ProgressCallback:
    """Callback class for Telegram upload progress"""
    def __init__(self, task_id, part_index, total_parts):
        self.task_id = task_id
        self.part_index = part_index
        self.total_parts = total_parts
        self.start_time = time.time()
        self.last_update = self.start_time
        self.last_bytes = 0
        self.speed = 0

    def __call__(self, sent_bytes, total):
        now = time.time()
        elapsed = now - self.last_update

        # Update speed every 0.5 seconds
        if elapsed > 0.5:
            transferred = sent_bytes - self.last_bytes
            self.speed = transferred / elapsed / 1024  # KB/s
            self.last_bytes = sent_bytes
            self.last_update = now

            # Calculate overall progress
            overall_progress = (self.part_index - 1 + (sent_bytes / total)) / self.total_parts * 100

            upload_status[self.task_id] = {
                "stage": f"Uploading part {self.part_index}/{self.total_parts}",
                "progress": round(overall_progress, 1),
                "speed": round(self.speed, 2),
                "done": False,
                "error": None
            }


# Async upload handler for Telegram
def background_upload(task_id, folder_path, filename):
    try:
        upload_status[task_id] = {
            "stage": "Preparing upload",
            "progress": 0,
            "speed": 0,
            "done": False,
            "error": None
        }

        # Get all files in the folder
        files = sorted(glob.glob(os.path.join(folder_path, '*')))
        if not files:
            raise Exception("No files found in folder")

        total_parts = len(files)

        async def send():
            # Create session directory if not exists
            session_dir = Path("telegram_session")
            session_dir.mkdir(exist_ok=True)
            session_path = str(session_dir / "session")

            client = TelegramClient(session_path, api_id, api_hash)
            await client.start()

            # Fix time synchronization issue
            try:
                await client(functions.help.GetConfigRequest())
            except RPCError as e:
                logger.warning(f"Time sync issue: {e}")
                await client(functions.help.GetNearestDcRequest())
                await client(functions.help.GetConfigRequest())

            if not await client.is_user_authorized():
                raise Exception("Telegram client is not authorized")

            for i, file_path in enumerate(files, 1):
                part_filename = os.path.basename(file_path)
                caption = f"{filename} - Part {i}/{total_parts}"

                # Create progress callback
                progress_cb = ProgressCallback(task_id, i, total_parts)

                # Update status before starting upload
                upload_status[task_id] = {
                    "stage": f"Starting upload of part {i}/{total_parts}",
                    "progress": ((i-1) / total_parts) * 100,
                    "speed": 0,
                    "done": False,
                    "error": None
                }

                await client.send_file(
                    "me",
                    file_path,
                    caption=caption,
                    progress_callback=progress_cb,
                    part_size_kb=PART_SIZE_KB,
                    force_document=True
                )

                upload_status[task_id] = {
                    "stage": f"Completed part {i}/{total_parts}",
                    "progress": (i / total_parts) * 100,
                    "speed": 0,
                    "done": False,
                    "error": None
                }

            await client.disconnect()
            upload_status[task_id] = {
                "stage": "Completed",
                "progress": 100,
                "speed": 0,
                "done": True,
                "error": None
            }

        asyncio.run(send())

    except Exception as e:
        upload_status[task_id] = {
            "stage": "Error",
            "progress": 0,
            "speed": 0,
            "done": False,
            "error": str(e)
        }
        logger.error(f"Upload failed: {str(e)}")
        logger.exception("Telegram upload error")


@app.before_request
def before_request():
    """Initialize session tracking"""
    if 'session_id' not in session:
        session_id = secrets.token_hex(16)
        session['session_id'] = session_id
        logger.info(f"New session started: {session_id}")

    ensure_session_files(session['session_id'])


@app.route('/')
def index():
    cleanup_session_files()
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file part in request'})

        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No selected file'})

        if not allowed_file(file.filename):
            return jsonify({'success': False, 'error': 'Invalid file type'})

        filename = secure_filename(file.filename)
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        file.save(upload_path)

        if not os.path.exists(upload_path):
            return jsonify({'success': False, 'error': 'File save failed'})

        session_id = session['session_id']
        session_data = ensure_session_files(session_id)
        session_data['uploads'].append(upload_path)

        file_size = os.path.getsize(upload_path)
        logger.info(f"Uploaded {filename} ({file_size} bytes) to {upload_path}")

        return jsonify({'success': True, 'filename': filename})

    except Exception as e:
        logger.exception("Error during upload")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/process', methods=['POST'])
def process():
    try:
        filename = request.form['filename']
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        if not os.path.exists(upload_path):
            logger.error(f"File not found: {upload_path}")
            return jsonify({'success': False, 'error': 'Uploaded file not found'})

        name, ext = os.path.splitext(filename)
        output_folder = os.path.join(app.config['BASE_SPLIT_FOLDER'], name)

        os.makedirs(output_folder, exist_ok=True)
        logger.info(f"Created output folder: {output_folder}")

        session_id = session['session_id']
        session_data = ensure_session_files(session_id)
        session_data['splits'].append(output_folder)

        part_files = split_video_with_ffmpeg(upload_path, output_folder, MAX_SIZE_MB)

        if part_files is None:
            return jsonify({'success': False, 'error': 'Failed to split video'})

        try:
            if upload_path in session_data['uploads']:
                session_data['uploads'].remove(upload_path)
            os.remove(upload_path)
            logger.info(f"Removed original file: {upload_path}")
        except Exception as e:
            logger.error(f"Error removing original file: {e}")

        progress_dict[filename] = 100

        return jsonify({
            'success': True,
            'filename': filename,
            'split_files': part_files,
            'output_folder': output_folder,
            'folder_name': name
        })

    except Exception as e:
        logger.exception("Error during processing")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/progress/<filename>')
def progress(filename):
    prog = progress_dict.get(filename, 0)
    return jsonify({'progress': round(prog, 2)})


@app.route('/upload_status/<task_id>')
def get_upload_status(task_id):
    return jsonify(upload_status.get(task_id, {
        "error": "Task ID not found",
        "stage": "Unknown",
        "progress": 0,
        "speed": 0,
        "done": False
    }))


@app.route('/upload_to_telegram', methods=['POST'])
def upload_to_telegram():
    try:
        filename = request.form['filename']
        folder_name = request.form['folder_name']
        output_folder = os.path.join(app.config['BASE_SPLIT_FOLDER'], folder_name)

        if not os.path.exists(output_folder):
            return jsonify({'success': False, 'error': 'Folder not found'})

        task_id = str(uuid.uuid4())
        upload_status[task_id] = {
            "stage": "Queued",
            "progress": 0,
            "speed": 0,
            "done": False,
            "error": None
        }

        Thread(target=background_upload, args=(task_id, output_folder, filename)).start()

        return jsonify({'success': True, 'task_id': task_id})

    except Exception as e:
        logger.exception("Error during Telegram upload initiation")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/download/zip/<folder_name>')
def download_zip(folder_name):
    try:
        folder_path = os.path.join(app.config['BASE_SPLIT_FOLDER'], folder_name)
        if not os.path.exists(folder_path):
            return jsonify({'success': False, 'error': 'Folder not found'})

        zip_file = create_zip(folder_path)
        response = send_file(
            zip_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'{folder_name}.zip'
        )

        @response.call_on_close
        def cleanup():
            cleanup_folder(folder_path)
            session_id = session.get('session_id')
            if session_id:
                session_data = ensure_session_files(session_id)
                if folder_path in session_data['splits']:
                    session_data['splits'].remove(folder_path)

        return response
    except Exception as e:
        logger.exception("Error during zip download")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/download/separate/<folder_name>/<filename>')
def download_separate(folder_name, filename):
    try:
        folder_path = os.path.join(app.config['BASE_SPLIT_FOLDER'], folder_name)
        if not os.path.exists(folder_path):
            return jsonify({'success': False, 'error': 'Folder not found'})

        file_path = os.path.join(folder_path, filename)
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'error': 'File not found'})

        files_in_folder = os.listdir(folder_path)
        is_last_file = len(files_in_folder) == 1 and files_in_folder[0] == filename

        response = send_file(
            file_path,
            as_attachment=True
        )

        if is_last_file:
            @response.call_on_close
            def cleanup():
                cleanup_folder(folder_path)
                session_id = session.get('session_id')
                if session_id:
                    session_data = ensure_session_files(session_id)
                    if folder_path in session_data['splits']:
                        session_data['splits'].remove(folder_path)

        return response
    except Exception as e:
        logger.exception("Error during separate download")
        return jsonify({'success': False, 'error': str(e)})


def cleanup_session_files():
    """Clean up files associated with the current session"""
    session_id = session.get('session_id')
    if not session_id or session_id not in session_files:
        return

    session_data = session_files[session_id]

    for file_path in session_data['uploads'][:]:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned session upload: {file_path}")
            session_data['uploads'].remove(file_path)
        except Exception as e:
            logger.error(f"Error cleaning session upload: {e}")

    for folder_path in session_data['splits'][:]:
        try:
            if os.path.exists(folder_path):
                cleanup_folder(folder_path)
                logger.info(f"Cleaned session split folder: {folder_path}")
            session_data['splits'].remove(folder_path)
        except Exception as e:
            logger.error(f"Error cleaning session split folder: {e}")


@app.route('/cleanup', methods=['POST'])
def cleanup():
    """Explicit cleanup endpoint"""
    cleanup_session_files()
    return jsonify({'success': True, 'message': 'Session files cleaned'})


templates_dir = Path("templates")
templates_dir.mkdir(exist_ok=True)

with open(templates_dir / "index.html", "w") as f:
    f.write("""<!doctype html>
<html>
<head>
    <title>Video Splitter & Telegram Uploader</title>
    <style>
        :root {
            --primary-color: #6c5ce7;
            --secondary-color: #a29bfe;
            --success-color: #00b894;
            --error-color: #d63031;
            --info-color: #0984e3;
            --warning-color: #fdcb6e;
            --text-color: #2d3436;
            --light-bg: #f5f6fa;
            --card-shadow: 0 10px 20px rgba(0,0,0,0.1);
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 0;
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            color: var(--text-color);
            min-height: 100vh;
        }
        .container {
            max-width: 900px;
            margin: 0 auto;
            padding: 30px;
            animation: fadeIn 0.5s ease-in-out;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        h2 {
            text-align: center;
            color: var(--primary-color);
            margin-bottom: 30px;
            font-size: 2.5rem;
            position: relative;
            display: inline-block;
            width: 100%;
        }
        h2::after {
            content: '';
            position: absolute;
            bottom: -10px;
            left: 50%;
            transform: translateX(-50%);
            width: 100px;
            height: 4px;
            background: var(--primary-color);
            border-radius: 2px;
        }
        .card {
            background: white;
            border-radius: 15px;
            padding: 30px;
            box-shadow: var(--card-shadow);
            margin-bottom: 30px;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 15px 30px rgba(0,0,0,0.15);
        }
        .upload-area {
            border: 3px dashed var(--secondary-color);
            border-radius: 10px;
            padding: 30px;
            text-align: center;
            margin-bottom: 20px;
            transition: all 0.3s ease;
            background: rgba(162, 155, 254, 0.05);
        }
        .upload-area:hover {
            border-color: var(--primary-color);
            background: rgba(108, 92, 231, 0.05);
        }
        .file-input-wrapper {
            position: relative;
            overflow: hidden;
            display: inline-block;
            margin-bottom: 20px;
        }
        .btn {
            padding: 12px 25px;
            background: var(--primary-color);
            color: white;
            border: none;
            border-radius: 50px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: 0 4px 6px rgba(108, 92, 231, 0.2);
            text-transform: uppercase;
            letter-spacing: 1px;
            display: inline-block;
        }
        .btn:hover {
            background: #5649c4;
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(108, 92, 231, 0.3);
        }
        .btn:active {
            transform: translateY(0);
        }
        .btn-telegram {
            background: #0088cc;
            box-shadow: 0 4px 6px rgba(0, 136, 204, 0.2);
        }
        .btn-telegram:hover {
            background: #0077b3;
            box-shadow: 0 6px 12px rgba(0, 136, 204, 0.3);
        }
        .btn-download {
            background: var(--success-color);
            box-shadow: 0 4px 6px rgba(0, 184, 148, 0.2);
        }
        .btn-download:hover {
            background: #00a383;
            box-shadow: 0 6px 12px rgba(0, 184, 148, 0.3);
        }
        .btn-delete {
            background: var(--error-color);
            box-shadow: 0 4px 6px rgba(214, 48, 49, 0.2);
        }
        .btn-delete:hover {
            background: #c0392b;
            box-shadow: 0 6px 12px rgba(214, 48, 49, 0.3);
        }
        .progress-container {
            margin: 25px 0;
            animation: fadeIn 0.5s ease-in-out;
        }
        .progress-label {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
            font-weight: 600;
            color: var(--primary-color);
        }
        .progress-bar {
            height: 20px;
            background: #e0e0e0;
            border-radius: 10px;
            margin-bottom: 15px;
            overflow: hidden;
            position: relative;
        }
        .progress {
            height: 100%;
            background: linear-gradient(90deg, var(--primary-color), var(--secondary-color));
            width: 0%;
            color: white;
            text-align: center;
            line-height: 20px;
            font-size: 12px;
            font-weight: bold;
            transition: width 0.5s ease, background-color 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        .progress::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(
                90deg,
                rgba(255, 255, 255, 0) 0%,
                rgba(255, 255, 255, 0.3) 50%,
                rgba(255, 255, 255, 0) 100%
            );
            animation: shimmer 2s infinite;
        }
        @keyframes shimmer {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(100%); }
        }
        .speed-info {
            font-size: 14px;
            color: #666;
            text-align: right;
            margin-top: -10px;
            margin-bottom: 15px;
        }
        .stage-info {
            font-size: 14px;
            margin-bottom: 5px;
            color: var(--info-color);
            font-weight: 600;
        }
        .action-buttons {
            margin-top: 30px;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            justify-content: center;
        }
        .file-list {
            max-height: 250px;
            overflow-y: auto;
            border: 2px solid #eee;
            border-radius: 10px;
            padding: 15px;
            margin: 20px 0;
            background: white;
        }
        .file-list p {
            font-weight: 600;
            color: var(--primary-color);
            margin-top: 0;
        }
        .file-list ul {
            list-style-type: none;
            padding: 0;
            margin: 0;
        }
        .file-list li {
            padding: 8px 15px;
            margin: 5px 0;
            background: rgba(162, 155, 254, 0.1);
            border-left: 4px solid var(--secondary-color);
            border-radius: 4px;
            transition: all 0.3s ease;
        }
        .file-list li:hover {
            background: rgba(162, 155, 254, 0.2);
            transform: translateX(5px);
        }
        .status-message {
            padding: 15px;
            border-radius: 8px;
            margin: 15px 0;
            font-weight: 600;
            text-align: center;
            animation: fadeIn 0.5s ease-in-out;
        }
        .status-success {
            background: rgba(0, 184, 148, 0.1);
            border: 1px solid var(--success-color);
            color: var(--success-color);
        }
        .status-error {
            background: rgba(214, 48, 49, 0.1);
            border: 1px solid var(--error-color);
            color: var(--error-color);
        }
        .status-info {
            background: rgba(9, 132, 227, 0.1);
            border: 1px solid var(--info-color);
            color: var(--info-color);
        }
        .pulse {
            animation: pulse 1.5s infinite;
        }
        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.6; }
            100% { opacity: 1; }
        }
        @media (max-width: 768px) {
            .container { padding: 20px; }
            .action-buttons { flex-direction: column; gap: 10px; }
            .btn { width: 100%; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="card">
            <h2>Video Splitter & Telegram Uploader</h2>
            <div class="upload-area">
                <form id="uploadForm" enctype="multipart/form-data">
                    <div class="file-input-wrapper">
                        <input type="file" name="file" id="fileInput" accept="video/*" required style="display: none;">
                        <label for="fileInput" class="btn">Choose Video File</label>
                    </div>
                    <p id="fileName" style="margin-top: 10px; color: #666; font-style: italic;">No file selected</p>
                    <input type="submit" value="Upload & Process" class="btn" id="uploadBtn">
                </form>
            </div>
            <p style="text-align: center; color: #666;">Max file size: 100GB | Allowed formats: mp4, avi, mov, mkv, webm</p>
        </div>
    </div>
</body>
</html>""")


if __name__ == '__main__':
    for path in [app.config['UPLOAD_FOLDER'], app.config['BASE_SPLIT_FOLDER']]:
        if not os.access(path, os.W_OK):
            logger.warning(f"Directory not writable: {path}")
    start_cleanup_thread()
    app.run(debug=True, host='0.0.0.0', port=5000)
