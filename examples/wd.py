import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    #path = '/Users/alex/Projects/pdf2swf_converter/tmp/'

    class MyHandler(FileSystemEventHandler):
        def process(self, event):
            print event.src_path, event.event_type  # print now only for degug

        def on_modified(self, event):
            self.process(event)

        def on_created(self, event):
            self.process(event)

    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()