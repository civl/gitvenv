from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import requests
import os
import sys
import time
import threading
import json
import re
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import datetime
import pboc_initial_database as db

# Load environment variables
basedir = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

class DownloadManager:
    def __init__(self):
        self.is_running = False
        self.queue = []
        self.total = 0
        self.current = 0
        self.success = 0
        self.fail = 0
        self.logs = []
        self.province = ""
        self.download_dir = ""

    def add_log(self, message, level="info"):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        log_entry = {
            "time": timestamp,
            "message": message,
            "level": level,
            "type": "log"
        }
        self.logs.append(log_entry)
        # Keep logs manageable
        if len(self.logs) > 1000:
            self.logs.pop(0)
        return log_entry

    def get_progress(self):
        return {
            "type": "progress",
            "current": self.current,
            "total": self.total,
            "success": self.success,
            "fail": self.fail
        }

manager = DownloadManager()

def get_db_connection():
    try:
        conn = db.get_connection('fic')
        return conn
    except Exception as e:
        print(f"DB Error: {e}")
        return None

def sanitize_filename(name):
    # Remove invalid characters
    name = re.sub(r'[\\/*?:"<>|]', '', name)
    name = name.replace('\n', '').replace('\r', '').strip()
    return name

def process_download(province):
    manager.is_running = True
    manager.province = province
    manager.logs = []
    manager.current = 0
    manager.success = 0
    manager.fail = 0
    
    manager.add_log(f"Starting download for province: {province}")
    
    # 1. Setup download directory
    # User requested: ~/Downloads/pboc_penalty/{province} (implied or I chose it)
    # Actually user said "save as xlsx (naming same as field value)".
    # I'll put it in a dedicated folder in user's downloads or project downloads to be safe.
    # Previous task established project downloads folder is better.
    # But user specifically asked for "User's download folder" in previous turn.
    # Let's use project downloads folder to avoid permission issues, as learned before.
    
    base_download_dir = os.path.join(basedir, "downloads", "pboc_penalty", province)
    if not os.path.exists(base_download_dir):
        os.makedirs(base_download_dir)
    manager.download_dir = base_download_dir
    manager.add_log(f"Saving files to: {base_download_dir}")

    # 2. Fetch records
    conn = get_db_connection()
    if not conn:
        manager.add_log("Failed to connect to database", "error")
        manager.is_running = False
        return

    try:
        cursor = conn.cursor(dictionary=True)
        # Select records for the province
        sql = "SELECT * FROM pboc_penalty WHERE 省份 LIKE %s"
        cursor.execute(sql, (f"%{province}%",))
        records = cursor.fetchall()
        manager.total = len(records)
        manager.add_log(f"Found {manager.total} records.")
        
        cursor.close()
        conn.close()
        
        session = requests.Session()
        
        for i, row in enumerate(records):
            manager.current = i + 1
            file_name_base = row.get('行政处罚文件') or f"record_{row['id']}"
            file_name_base = sanitize_filename(file_name_base)
            detail_url = row.get('下载链接')
            
            if not detail_url:
                manager.add_log(f"Skipping {file_name_base}: No URL", "error")
                manager.fail += 1
                continue

            manager.add_log(f"Processing: {file_name_base}")
            
            try:
                # Visit detail page
                resp = session.get(detail_url, headers=HEADERS, timeout=15)
                resp.raise_for_status()
                resp.encoding = 'utf-8' # Assume utf-8, maybe adjust if garbled
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                
                # Strategy 1: Look for file links
                file_found = False
                
                # Extensions to look for
                exts = ['.doc', '.docx', '.pdf', '.xls', '.xlsx', '.et']
                
                # Find all links
                links = soup.find_all('a', href=True)
                target_link = None
                target_ext = None
                
                for link in links:
                    href = link['href'].strip()
                    lower_href = href.lower()
                    for ext in exts:
                        if lower_href.endswith(ext):
                            target_link = href
                            target_ext = ext
                            break
                    if target_link:
                        break
                
                if target_link:
                    # Download file
                    full_url = urljoin(detail_url, target_link)
                    manager.add_log(f"Found file: {target_link}")
                    
                    file_resp = session.get(full_url, headers=HEADERS, stream=True, timeout=30)
                    file_resp.raise_for_status()
                    
                    final_path = os.path.join(base_download_dir, f"{file_name_base}{target_ext}")
                    
                    with open(final_path, "wb") as f:
                        for chunk in file_resp.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    manager.add_log(f"Saved: {os.path.basename(final_path)}", "success")
                    manager.success += 1
                    file_found = True
                    
                else:
                    # Strategy 2: Look for table
                    manager.add_log("No file link found, looking for table...", "info")
                    tables = soup.find_all('table')
                    
                    # Filter out tiny layout tables if possible, but for now just take the largest or first meaningful one
                    # The example shows a layout table (border=0), but maybe the data is inside?
                    # Actually the example shows the file link INSIDE a table. 
                    # If we are here, Strategy 1 failed, so no link in table.
                    # We need to extract DATA from table.
                    
                    valid_table = None
                    # Simple heuristic: table with most text content
                    max_len = 0
                    
                    for table in tables:
                        txt = table.get_text(strip=True)
                        if len(txt) > max_len:
                            max_len = len(txt)
                            valid_table = table
                    
                    if valid_table and max_len > 20: # Arbitrary threshold
                        try:
                            # Use pandas to parse
                            # We need to wrap html in StringIO for pandas
                            dfs = pd.read_html(str(valid_table))
                            if dfs:
                                df = dfs[0]
                                final_path = os.path.join(base_download_dir, f"{file_name_base}.xlsx")
                                df.to_excel(final_path, index=False)
                                manager.add_log(f"Saved table to xlsx: {os.path.basename(final_path)}", "success")
                                manager.success += 1
                                file_found = True
                        except Exception as e:
                            manager.add_log(f"Table parsing failed: {e}", "error")
                    
                if not file_found:
                    manager.add_log("No document or valid table found.", "error")
                    manager.fail += 1
                    
            except Exception as e:
                manager.add_log(f"Error processing {detail_url}: {e}", "error")
                manager.fail += 1
            
            # Polite delay
            time.sleep(0.5)
            
    except Exception as e:
        manager.add_log(f"Global Error: {e}", "error")
    
    manager.is_running = False
    manager.add_log("Download completed.", "done")

@app.route('/')
def index():
    return render_template('pboc_index.html')

@app.route('/start', methods=['POST'])
def start():
    data = request.json
    province = data.get('province', '上海')
    
    if manager.is_running:
        return jsonify({"status": "error", "message": "Already running"})
    
    thread = threading.Thread(target=process_download, args=(province,))
    thread.daemon = True
    thread.start()
    
    return jsonify({"status": "started"})

@app.route('/stream')
def stream():
    def event_stream():
        # Yield initial progress
        yield f"data: {json.dumps(manager.get_progress())}\n\n"
        
        last_log_idx = 0
        while manager.is_running or last_log_idx < len(manager.logs):
            # Send new logs
            while last_log_idx < len(manager.logs):
                log = manager.logs[last_log_idx]
                yield f"data: {json.dumps(log)}\n\n"
                last_log_idx += 1
            
            # Send progress update
            yield f"data: {json.dumps(manager.get_progress())}\n\n"
            
            time.sleep(0.5)
            
        # Send final logs
        while last_log_idx < len(manager.logs):
            log = manager.logs[last_log_idx]
            yield f"data: {json.dumps(log)}\n\n"
            last_log_idx += 1
            
        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")

if __name__ == '__main__':
    # Auto-open browser
    def open_browser():
        time.sleep(1)
        os.system("open http://127.0.0.1:5001")
        
    threading.Thread(target=open_browser).start()
    app.run(host='0.0.0.0', port=5001, debug=False)
