import sqlite3
import threading
import logging
from datetime import datetime
from config import conf
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
                cls._instance.init_db()
            return cls._instance
    
    def init_db(self):
        """初始化数据库连接和表结构"""
        try:
            db_path = os.path.join(conf().get("appdata_dir", ""), "group_messages.db")
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.cursor = self.conn.cursor()
            
            # 创建群消息表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    msg_id TEXT UNIQUE,
                    group_id TEXT,
                    group_name TEXT,
                    sender_id TEXT,
                    sender_name TEXT,
                    content TEXT,
                    msg_type TEXT,
                    create_time INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建索引
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_group_id ON group_messages(group_id)
            ''')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_create_time ON group_messages(create_time)
            ''')
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def save_message(self, message_data):
        """保存消息到数据库"""
        try:
            with self._lock:
                self.cursor.execute('''
                    INSERT INTO group_messages 
                    (msg_id, group_id, group_name, sender_id, sender_name, content, msg_type, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    message_data['msg_id'],
                    message_data['group_id'],
                    message_data['group_name'],
                    message_data['sender_id'],
                    message_data['sender_name'],
                    message_data['content'],
                    message_data['msg_type'],
                    message_data['create_time']
                ))
                self.conn.commit()
                
        except sqlite3.IntegrityError:
            logger.warning(f"Duplicate message ID: {message_data['msg_id']}")
        except Exception as e:
            logger.error(f"Error saving message to database: {e}")
            self.conn.rollback()
            raise

def save_group_message(message_data):
    """保存群消息的公共接口"""
    try:
        db = DatabaseManager()
        db.save_message(message_data)
    except Exception as e:
        logger.error(f"Failed to save group message: {e}") 