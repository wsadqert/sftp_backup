import schedule
import datetime
import time
import os
import pytz
import shutil
from backup import backup

MSK = pytz.timezone("Europe/Moscow")

# Max retention per tier (in hours)
RETENTION_HOURS = {
	"hourly": 12,
	"daily": 3 * 24,
	"weekly": 2 * 7 * 24
}

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
	print(f"[{now_msk().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

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

while True:
	schedule.run_pending()
	time.sleep(10)
