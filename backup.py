import os
import stat
import threading
import queue
import paramiko
from dotenv import load_dotenv

load_dotenv()

# Number of parallel download threads
THREAD_COUNT = 16

# Shared queue of (remote_path, local_path)
download_queue = queue.Queue()

def create_sftp_client():
	hostname = os.environ["SFTP_HOST"]
	port = int(os.environ["SFTP_PORT"])
	username = os.environ["SFTP_USERNAME"]
	password = os.environ["SFTP_PASSWORD"]

	ssh = paramiko.SSHClient()
	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	ssh.connect(hostname, port=port, username=username, password=password, look_for_keys=False)
	return ssh, ssh.open_sftp()

def download_worker():
	ssh, sftp = create_sftp_client()
	while True:
		try:
			remote_path, local_path = download_queue.get(timeout=5)
		except queue.Empty:
			break
		try:
			local_dir = os.path.dirname(local_path)
			os.makedirs(local_dir, exist_ok=True)
			print(f"[{threading.current_thread().name}] Downloading {remote_path} → {local_path}")
			sftp.get(remote_path, local_path)
		except Exception as e:
			print(f"Error downloading {remote_path}: {e}")
		finally:
			download_queue.task_done()
	sftp.close()
	ssh.close()

def walk_remote_dir(sftp: paramiko.SFTPClient, remote_dir, local_dir):
	os.makedirs(local_dir, exist_ok=True)
	for entry in sftp.listdir_attr(remote_dir):
		remote_path = remote_dir + '/' + entry.filename
		local_path = os.path.join(local_dir, entry.filename)
		if stat.S_ISDIR(entry.st_mode):
			walk_remote_dir(sftp, remote_path, local_path)
		else:
			download_queue.put((remote_path, local_path))

def backup(local_destination: str):
	ssh, sftp = create_sftp_client()
	remote_start_path = "/world"
	# local_destination = f"./backups/{datetime.datetime.now().strftime('%Y-%m-%d_%H.%M.%S')}"

	# Crawl and enqueue files
	walk_remote_dir(sftp, remote_start_path, local_destination)
	sftp.close()
	ssh.close()

	# Start worker threads
	threads = []
	for _ in range(THREAD_COUNT):
		t = threading.Thread(target=download_worker, daemon=True)
		t.start()
		threads.append(t)

	# Wait for all files to be downloaded
	download_queue.join()
