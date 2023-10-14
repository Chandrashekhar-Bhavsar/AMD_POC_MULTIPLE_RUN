# HDFS Configuration
hdfs_host = 'http://localhost:9870'
hdfs_user = 'hdoop'
from hdfs import InsecureClient
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os

hdfs_client = InsecureClient(hdfs_host, user=hdfs_user)
local_path = "/var/www/html/myfiles/"

def move_to_hdfs(local_path, event_src_path):
    try:
        # Specify the destination path in HDFS
        hdfs_dest_path = '/Apche_Server'

        # Form the relative path for the new directory
        relative_path = os.path.relpath(event_src_path, local_path)

        # Form the destination path in HDFS
        hdfs_dest_path = os.path.join(hdfs_dest_path, relative_path)

        # Check if folder exists in HDFS and delete it if it does
        if hdfs_client.content(hdfs_dest_path, strict=False):
            hdfs_client.delete(hdfs_dest_path, recursive=True)

        # Copy the local folder to HDFS
        hdfs_client.upload(hdfs_dest_path, event_src_path, n_threads=5)
        print(f"{event_src_path} copied to HDFS at {hdfs_dest_path}")
    except Exception as e:
        print(f"An error occurred while moving to HDFS: {e}")

class Watcher:
    def __init__(self, directory_to_watch):
        self.observer = Observer()
        self.directory_to_watch = directory_to_watch

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:  # Only consider directories
            print(f'New directory created: {event.src_path}')
            move_to_hdfs(local_path, event.src_path)

if __name__ == "__main__":
    watch = Watcher(local_path)
    watch.run()
