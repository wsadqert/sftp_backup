import schedule
import datetime
import time
import os
import pytz
import shutil
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from backup import backup

MSK = pytz.timezone("Europe/Moscow")

# Max retention per tier (in hours)
RETENTION_HOURS = {
	"hourly": 12,
	"daily": 3 * 24,
	"weekly": 2 * 7 * 24
}

last_backup_logs = []

def now_msk():
	return datetime.datetime.now(MSK)

def msk_hour():
	return now_msk().hour

def make_path(tier: str) -> str:
	t = now_msk()
	if tier == 'hourly':
		name = t.strftime("%Y-%m-%d_%H.%M.%S")
	else:
		name = t.strftime("%Y-%m-%d")
	return f"./backups/{tier}/{name}"

def log(msg):
	timestamp = now_msk().strftime('%Y-%m-%d %H:%M:%S')
	full_msg = f"[{timestamp}] {msg}"
	print(full_msg)
	last_backup_logs.append(full_msg)
	if len(last_backup_logs) > 50:
		del last_backup_logs[0]

def backup_with_log(tier):
	path = make_path(tier)
	log(f"Starting {tier} backup to {path}")
	backup(path)
	log(f"Finished {tier} backup")
	cleanup_backups()

def backup_hourly():
	if 14 <= msk_hour() <= 21:
		backup_with_log("hourly")
	else:
		log("Hourly backup skipped (outside active hours)")

def backup_daily():
	backup_with_log("daily")

def backup_weekly():
	if now_msk().weekday() == 6:
		backup_with_log("weekly")
	else:
		log("Weekly backup skipped (not Sunday)")

def cleanup_backups():
	log("Starting cleanup of old backups")
	max_total_bytes = 14 * 1024**3
	cutoff = now_msk()

	def get_all_backups():
		entries = []
		for tier in ('hourly', 'daily', 'weekly'):
			tier_path = f'./backups/{tier}'
			if not os.path.exists(tier_path):
				continue
			for name in os.listdir(tier_path):
				full = os.path.join(tier_path, name)
				if os.path.isdir(full):
					entries.append((tier, full, os.path.getmtime(full)))
		return entries

	# Delete old by retention policy
	for tier, path, mtime in get_all_backups():
		age_hours = (cutoff - datetime.datetime.fromtimestamp(mtime, MSK)).seconds / 3600
		if age_hours > RETENTION_HOURS[tier]:
			log(f"Deleting {tier} backup {path} (age {age_hours}h > {RETENTION_HOURS[tier]}h)")
			shutil.rmtree(path)

	# Then enforce disk quota
	def get_dir_size(path):
		total = 0
		for dirpath, _, filenames in os.walk(path):
			for f in filenames:
				total += os.path.getsize(os.path.join(dirpath, f))
		return total

	def total_size():
		return sum(get_dir_size(p) for _, p, _ in get_all_backups())

	while total_size() > max_total_bytes:
		sorted_by_time = sorted(get_all_backups(), key=lambda x: x[2])
		oldest_tier, oldest_path, _ = sorted_by_time[0]
		log(f"Deleting oldest {oldest_tier} backup {oldest_path} to free space")
		shutil.rmtree(oldest_path)

	log("Cleanup complete")

# Scheduling
for hour in range(14, 22):
	schedule.every().day.at(f"{(hour - 3):02d}:00").do(backup_hourly)  # MSK -> UTC

schedule.every().day.at("23:00").do(backup_daily)     # 02:00 MSK
schedule.every().sunday.at("23:00").do(backup_weekly) # Sunday 02:00 MSK

log("Backup scheduler started")

# HTTP status server
class StatusHandler(BaseHTTPRequestHandler):
	def do_GET(self):
		self.send_response(200)
		self.send_header('Content-type', 'text/plain; charset=utf-8')
		self.end_headers()
		output = "Backup Status Log (MSK):\n\n" + "\n".join(last_backup_logs)
		self.wfile.write(output.encode('utf-8'))

def start_status_server():
	srv = HTTPServer(('0.0.0.0', 8080), StatusHandler)
	log("Status server running on port 8080")
	srv.serve_forever()

threading.Thread(target=start_status_server, daemon=True).start()

while True:
	schedule.run_pending()
	time.sleep(10)
