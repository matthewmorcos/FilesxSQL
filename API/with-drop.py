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
        
        # Drop existing tables
        cursor.execute('''DROP TABLE IF EXISTS documents''')
        cursor.execute('''DROP TABLE IF EXISTS folders''')
        
        # Create tables with the correct schema
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
        logging.info("Database connected and tables created.")
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
        folder_path = os.path.dirname(file_path)
        folder_id = get_or_create_folder(db_conn, folder_path)

        cursor = db_conn.cursor()
        cursor.execute('''INSERT INTO documents (filename, content, folder_id)
                          VALUES (%s, %s, %s)
                          ON DUPLICATE KEY UPDATE content = VALUES(content), folder_id = VALUES(folder_id)''', 
                          (filename, content, folder_id))
        db_conn.commit()
        logging.info(f'Updated database with file: {filename} in folder: {folder_path}')
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

# Get or create folder and return its ID
def get_or_create_folder(db_conn, folder_path):
    base_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Documents'))
    relative_path = os.path.relpath(folder_path, base_dir)
    if relative_path.startswith('..'):
        return None  # Skip folders outside the base directory

    parent_id = None
    for part in relative_path.split(os.sep):
        cursor = db_conn.cursor()
        cursor.execute('''INSERT IGNORE INTO folders (foldername, parent_id) VALUES (%s, %s)''', (part, parent_id))
        cursor.close()

        cursor = db_conn.cursor()
        cursor.execute('''SELECT id FROM folders WHERE foldername = %s AND parent_id <=> %s''', (part, parent_id))
        result = cursor.fetchone()
        cursor.close()

        if result:
            parent_id = result[0]
        else:
            cursor = db_conn.cursor()
            cursor.execute('''SELECT id FROM folders WHERE foldername = %s AND parent_id IS NULL''', (part,))
            parent_id = cursor.fetchone()[0]
            cursor.close()
    db_conn.commit()
    return parent_id

# Scan folder and add existing documents to the database
def scan_and_add_existing_files(db_conn, path):
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            update_db(db_conn, file_path)

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
            host='localhost',
            user='root',
            password='@C0ntrolsM4nufactur!ng',
            database='intranetDB'
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

    # Scan and add existing files to the database
    scan_and_add_existing_files(db_conn, path)

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