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
             host='???',
            user='???',
            password='???',
            database='???'
        )

        
        cursor = conn.cursor()



        # Drop existing tables
        cursor.execute('''DROP TABLE IF EXISTS documents''')
        cursor.execute('''DROP TABLE IF EXISTS folders''')


        cursor.execute('''CREATE TABLE IF NOT EXISTS folders (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          foldername VARCHAR(255),
                          path VARCHAR(255) NOT NULL,
                          parent_folder_id INT,

                          FOREIGN KEY (parent_folder_id) REFERENCES folders(id),
                       UNIQUE (foldername, path))''')
        

        
        
        cursor.execute('''CREATE TABLE IF NOT EXISTS documents (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          filename VARCHAR(255),
                          path TEXT NOT NULL,
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




def update_db(db_conn, file_path):
    try:
        filename = os.path.basename(file_path)
        reactFilename = os.path.splitext(os.path.basename(file_path))[0]
        foldername = os.path.basename(os.path.dirname(file_path))
        folderPath = os.path.dirname(file_path)
        parentFolder = os.path.basename(os.path.dirname(os.path.dirname(file_path)))
        
        reactFilePath = file_path.replace('\\', '\\\\')

        parent_FolderPath = os.path.dirname(folderPath)

        cursor = db_conn.cursor()

        # Get the parent folder ID
        cursor.execute('''SELECT id FROM folders WHERE path = %s''', (parent_FolderPath,))
        parent_folder_id = cursor.fetchone()
        if parent_folder_id:
            parent_folder_id = parent_folder_id[0]
        else:
            parent_folder_id = None

        # Insert or get the folder_id
        cursor.execute('''INSERT INTO folders (foldername, path, parent_folder_id) 
                          VALUES (%s, %s, %s)
                          ON DUPLICATE KEY UPDATE path = VALUES(path), parent_folder_id = VALUES(parent_folder_id)''', 
                          (foldername, folderPath, parent_folder_id))
        cursor.execute('''SELECT id FROM folders WHERE path = %s''', (folderPath,))
        folder_id = cursor.fetchone()[0]

        # Insert or replace the document
        cursor.execute('''INSERT INTO documents (filename, path, folder_id)
                          VALUES (%s, %s, %s)
                          ON DUPLICATE KEY UPDATE path = VALUES(path), folder_id = VALUES(folder_id)''', 
                          (reactFilename, reactFilePath, folder_id))
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
            host='???',
            user='???',
            password='???',
            database='???'
        )
        cursor = conn.cursor()
        cursor.execute('''SELECT d.filename, d.path, f.foldername 
                          FROM documents d 
                          JOIN folders f ON d.folder_id = f.id''')
        documents = cursor.fetchall()
        conn.close()
        return documents
    except mysql.connector.Error as e:
        logging.error(f"Failed to retrieve documents from database: {e}")
        return []
    



#GETTING CHILDREN
def get_direct_children_folders_and_files(db_conn, parent_folder_id, parent_folder_path):
    try:
        cursor = db_conn.cursor()
        # Retrieve direct child folders
        cursor.execute('''SELECT foldername, path FROM folders WHERE parent_folder_id = %s''', (parent_folder_id,))
        folders = cursor.fetchall()

        # Retrieve direct child files
        cursor.execute('''SELECT filename, path FROM documents WHERE folder_id = %s''', (parent_folder_id,))
        files = cursor.fetchall()

        return folders, files
    except mysql.connector.Error as e:
        logging.error(f"Failed to retrieve direct children folders and files: {e}")
        return [], []
    

def get_parent_folder_id(db_conn, parent_folder_path):
    try:
        cursor = db_conn.cursor()
        cursor.execute('''SELECT id FROM folders WHERE path = %s''', (parent_folder_path,))
        parent_folder_id = cursor.fetchone()
        if parent_folder_id:
            return parent_folder_id[0]
        else:
            logging.error(f"No folder found with path: {parent_folder_path}")
            return None
    except mysql.connector.Error as e:
        logging.error(f"Failed to retrieve parent folder ID: {e}")
        return None

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

    # Get user input for the folder path
    user_input_path = input("Enter the folder path to retrieve connected folders and files: ")
    user_input_path = os.path.normpath(user_input_path)

    # Get the parent folder ID
    parent_folder_id = get_parent_folder_id(db_conn, user_input_path)
    if parent_folder_id is not None:
        # Retrieve and print direct children folders and files
        folders, files = get_direct_children_folders_and_files(db_conn, parent_folder_id, user_input_path)
        print("Folders:")
        for foldername, folderpath in folders:
            print(f"Folder: {foldername}, Path: {folderpath}")
        print("Files:")
        for filename, filepath in files:
            print(f"File: {filename}, Path: {filepath}")

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    db_queue.join()



if __name__ == "__main__":
    main()
    documents = get_documents()
    for filename in documents:
        print(f'Filename: {filename}\n')
