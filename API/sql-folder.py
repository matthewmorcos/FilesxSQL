import os
import mysql.connector
import logging
import threading
import queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Database connection setup
def connect_db():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='@C0ntrolsM4nufactur!ng',
            database='intranetDB'
        )
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS folders (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          foldername VARCHAR(255) UNIQUE)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS documents (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          filename VARCHAR(255),
                          content TEXT,
                          folder_id INT,
                          FOREIGN KEY (folder_id) REFERENCES folders(id))''')
        conn.commit()
        logging.info("Database connected and tables created if not exists.")
        return conn
    except mysql.connector.Error as e:
        logging.error(f"Database connection failed: {e}")
        return None

# Function to process database operations
def db_worker(db_conn, db_queue):
    while True:
        operation, args = db_queue.get()
        if operation == 'update':
            update_db(db_conn, *args)
        elif operation == 'delete':
            delete_from_db(db_conn, *args)
        db_queue.task_done()

# Update database with file content
def update_db(db_conn, file_path):
    try:
        with open(file_path, 'r') as file:
            content = file.read()
        filename = os.path.basename(file_path)
        foldername = os.path.basename(os.path.dirname(file_path))
        cursor = db_conn.cursor()

        # Insert or get the folder_id
        cursor.execute('''INSERT IGNORE INTO folders (foldername) VALUES (%s)''', (foldername,))
        cursor.execute('''SELECT id FROM folders WHERE foldername = %s''', (foldername,))
        folder_id = cursor.fetchone()[0]

        # Insert or replace the document
        cursor.execute('''INSERT INTO documents (filename, content, folder_id)
                          VALUES (%s, %s, %s)
                          ON DUPLICATE KEY UPDATE content = VALUES(content), folder_id = VALUES(folder_id)''', 
                          (filename, content, folder_id))
        db_conn.commit()
        logging.info(f'Updated database with file: {filename} in folder: {foldername}')
    except Exception as e:
        logging.error(f"Failed to update database for file {file_path}: {e}")

# Delete file from database
def delete_from_db(db_conn, file_path):
    try:
        filename = os.path.basename(file_path)
        cursor = db_conn.cursor()
        cursor.execute('''DELETE FROM documents WHERE filename = %s''', (filename,))
        db_conn.commit()
        logging.info(f'Removed file from database: {filename}')
    except Exception as e:
        logging.error(f"Failed to delete from database for file {file_path}: {e}")

# Event handler for file system changes
class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, db_queue):
        self.db_queue = db_queue

    def on_modified(self, event):
        if not event.is_directory:
            logging.info(f'Modified file: {event.src_path}')
            self.db_queue.put(('update', (event.src_path,)))
            self.list_children(event.src_path)

    def on_created(self, event):
        if event.is_directory:
            logging.info(f'Created directory: {event.src_path}')
        else:
            logging.info(f'Created file: {event.src_path}')
            self.db_queue.put(('update', (event.src_path,)))

    def on_deleted(self, event):
        if not event.is_directory:
            logging.info(f'Deleted file: {event.src_path}')
            self.db_queue.put(('delete', (event.src_path,)))

    def list_children(self, file_path):
        directory = os.path.dirname(file_path)
        children = os.listdir(directory)
        logging.info(f'Children of {directory}:')
        for child in children:
            child_path = os.path.join(directory, child)
            logging.info(f'Path: {child_path}, Name: {child}')

# Function to query the database and retrieve document contents
def get_documents():
    try:
        conn = mysql.connector.connect(
            host='your_host',
            user='your_username',
            password='your_password',
            database='your_database'
        )
        cursor = conn.cursor()
        cursor.execute('''SELECT d.filename, d.content, f.foldername 
                          FROM documents d 
                          JOIN folders f ON d.folder_id = f.id''')
        documents = cursor.fetchall()
        conn.close()
        return documents
    except mysql.connector.Error as e:
        logging.error(f"Failed to retrieve documents from database: {e}")
        return []

# Main function to set up the observer
def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(current_dir, '..', 'Documents')
    path = os.path.normpath(path)

    db_conn = connect_db()
    if db_conn is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    db_queue = queue.Queue()
    db_thread = threading.Thread(target=db_worker, args=(db_conn, db_queue), daemon=True)
    db_thread.start()

    event_handler = FileChangeHandler(db_queue)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    logging.info(f'Starting to monitor: {path}')
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    db_queue.join()

if __name__ == "__main__":
    main()
    # Example usage of get_documents function
    documents = get_documents()
    for filename, content in documents:
        print(f'Filename: {filename}\nContent:\n{content}\n')