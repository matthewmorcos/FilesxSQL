import os
import sqlite3
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Database connection setup
def connect_db():
    try:
        conn = sqlite3.connect('documents.db')
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS documents
                          (id INTEGER PRIMARY KEY, filename TEXT, content TEXT)''')
        conn.commit()
        logging.info("Database connected and table created if not exists.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

# Event handler for file system changes
class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, db_conn):
        self.db_conn = db_conn

    def on_modified(self, event):
        if not event.is_directory:
            logging.info(f'Modified file: {event.src_path}')
            self.update_db(event.src_path)

    def on_created(self, event):
        if not event.is_directory:
            logging.info(f'Created file: {event.src_path}')
            self.update_db(event.src_path)

    def on_deleted(self, event):
        if not event.is_directory:
            logging.info(f'Deleted file: {event.src_path}')
            self.delete_from_db(event.src_path)

    def update_db(self, file_path):
        try:
            with open(file_path, 'r') as file:
                content = file.read()
            filename = os.path.basename(file_path)
            cursor = self.db_conn.cursor()
            cursor.execute('''INSERT OR REPLACE INTO documents (filename, content)
                              VALUES (?, ?)''', (filename, content))
            self.db_conn.commit()
            logging.info(f'Updated database with file: {filename}')
        except Exception as e:
            logging.error(f"Failed to update database for file {file_path}: {e}")

    def delete_from_db(self, file_path):
        try:
            filename = os.path.basename(file_path)
            cursor = self.db_conn.cursor()
            cursor.execute('''DELETE FROM documents WHERE filename = ?''', (filename,))
            self.db_conn.commit()
            logging.info(f'Removed file from database: {filename}')
        except Exception as e:
            logging.error(f"Failed to delete from database for file {file_path}: {e}")

# Function to query the database and retrieve document contents
def get_documents():
    try:
        conn = sqlite3.connect('documents.db')
        cursor = conn.cursor()
        cursor.execute('''SELECT filename, content FROM documents''')
        documents = cursor.fetchall()
        conn.close()
        return documents
    except sqlite3.Error as e:
        logging.error(f"Failed to retrieve documents from database: {e}")
        return []

# Main function to set up the observer
def main():
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Documents')
    db_conn = connect_db()
    if db_conn is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    event_handler = FileChangeHandler(db_conn)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    logging.info(f'Starting to monitor: {path}')
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
    # Example usage of get_documents function
    documents = get_documents()
    for filename, content in documents:
        print(f'Filename: {filename}\nContent:\n{content}\n')

