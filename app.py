import threading
import time
import os
import webbrowser
from flask import Flask, render_template, request, jsonify
from pboc_approval_mysql import run_task, db_host, db_port, db_user, db_password, db_schema, db_charset

app = Flask(__name__)

# Global state
scraper_state = {
    "status": "idle",  # idle, running, completed, error
    "message": "Ready to start",
    "progress": {
        "registered": {"current": 0, "total": 0, "items": 0},
        "unregistered": {"current": 0, "total": 0, "items": 0},
        "important_news": "pending"
    },
    "logs": [],
    "results": None
}

# Lock for thread safety
state_lock = threading.Lock()

def update_state(key, value):
    with state_lock:
        scraper_state[key] = value

def append_log(message):
    with state_lock:
        scraper_state["logs"].append(f"{time.strftime('%H:%M:%S')} - {message}")
        # Keep logs manageable
        if len(scraper_state["logs"]) > 1000:
            scraper_state["logs"] = scraper_state["logs"][-1000:]

def scraper_callback(phase, current, total, items):
    with state_lock:
        if phase == "registered":
            scraper_state["progress"]["registered"] = {"current": current, "total": total, "items": items}
            scraper_state["message"] = f"正在抓取已获许可机构: {current}/{total}页"
        elif phase == "unregistered":
            scraper_state["progress"]["unregistered"] = {"current": current, "total": total, "items": items}
            scraper_state["message"] = f"正在抓取已注销许可机构: {current}/{total}页"
        elif phase == "important_news_start":
            scraper_state["progress"]["important_news"] = "running"
            scraper_state["message"] = "正在抓取重大事项变更..."
        elif phase == "done":
            scraper_state["progress"]["important_news"] = "done"
            scraper_state["message"] = "抓取完成"
        elif phase == "error":
            scraper_state["message"] = f"出错: {items}" # items carries error message here

def background_task(db_config, max_workers):
    try:
        update_state("status", "running")
        update_state("results", None)
        with state_lock:
             scraper_state["progress"] = {
                "registered": {"current": 0, "total": 0, "items": 0},
                "unregistered": {"current": 0, "total": 0, "items": 0},
                "important_news": "pending"
            }
             scraper_state["logs"] = []
        
        append_log("任务开始...")
        
        results = run_task(db_config, max_workers, scraper_callback)
        
        update_state("results", results)
        update_state("status", "completed")
        append_log("任务成功完成。")
        
    except Exception as e:
        update_state("status", "error")
        append_log(f"任务失败: {str(e)}")
        # Re-raise to print to console if needed
        print(f"Error in background task: {e}")

@app.route('/')
def index():
    # Provide default config for the form
    default_config = {
        'host': db_host or 'localhost',
        'port': db_port or 3306,
        'user': db_user or 'root',
        'password': db_password or '',
        'schema': db_schema or 'fic',
        'charset': db_charset or 'utf8mb4'
    }
    return render_template('index.html', config=default_config)

@app.route('/start', methods=['POST'])
def start_scraper():
    if scraper_state["status"] == "running":
        return jsonify({"status": "error", "message": "Scraper is already running"}), 400
    
    data = request.json
    db_config = data.get('db_config')
    max_workers = int(data.get('max_workers', 3))
    
    # Validate DB config minimally
    if not db_config or not db_config.get('host'):
        return jsonify({"status": "error", "message": "Invalid DB configuration"}), 400

    thread = threading.Thread(target=background_task, args=(db_config, max_workers))
    thread.daemon = True
    thread.start()
    
    return jsonify({"status": "started"})

@app.route('/status')
def get_status():
    with state_lock:
        return jsonify(scraper_state)

if __name__ == '__main__':
    if not os.environ.get("WERKZEUG_RUN_MAIN"):
        webbrowser.open("http://127.0.0.1:5200")
    app.run(debug=True, port=5200)
