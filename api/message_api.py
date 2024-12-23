from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import threading
from common.db_utils import DatabaseManager
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="Group Message API", 
             description="API for querying WeChat group messages")

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    # 允许的源列表 - 可以设置多个域名
    allow_origins=[
        "http://103.194.107.232:8000",
        "http://localhost:8000",
        "*"  # 允许所有源访问 (仅用于开发环境，生产环境建议明确指定允许的域名)
    ],
    allow_credentials=True,
    allow_methods=["*"],  # 允许所有HTTP方法
    allow_headers=["*"],  # 允许所有请求头
    expose_headers=["*"],  # 允许暴露的响应头
    max_age=3600,  # 预检请求的缓存时间
)

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI application starting up...")
    
# 定义响应模型
class GroupMessage(BaseModel):
    id: int
    msg_id: str
    group_id: str
    group_name: str
    sender_id: str
    sender_name: str
    content: str
    msg_type: str
    create_time: int
    created_at: str

class GroupInfo(BaseModel):
    group_id: str
    group_name: str
    message_count: int
    last_active: str

@app.get("/messages/", response_model=List[GroupMessage])
async def get_messages(
    group_id: Optional[str] = None,
    sender_id: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    msg_type: Optional[str] = None,
    limit: int = Query(default=50, le=100),
    offset: int = 0
):
    """
    获取群消息列表
    - group_id: 群ID
    - sender_id: 发送者ID
    - start_time: 开始时间戳
    - end_time: 结束时间戳
    - msg_type: 消息类型
    - limit: 返回记录数量限制
    - offset: 分页偏移量
    """
    try:
        db = DatabaseManager()
        
        # 构建查询条件
        conditions = []
        params = []
        
        if group_id:
            conditions.append("group_id = ?")
            params.append(group_id)
        if sender_id:
            conditions.append("sender_id = ?")
            params.append(sender_id)
        if start_time:
            conditions.append("create_time >= ?")
            params.append(start_time)
        if end_time:
            conditions.append("create_time <= ?")
            params.append(end_time)
        if msg_type:
            conditions.append("msg_type = ?")
            params.append(msg_type)
            
        # 组装SQL语句
        sql = "SELECT * FROM group_messages"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY create_time DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        # 执行查询
        with db._lock:
            db.cursor.execute(sql, params)
            rows = db.cursor.fetchall()
            
        # 转换结果
        messages = []
        for row in rows:
            messages.append({
                "id": row[0],
                "msg_id": row[1],
                "group_id": row[2],
                "group_name": row[3],
                "sender_id": row[4],
                "sender_name": row[5],
                "content": row[6],
                "msg_type": row[7],
                "create_time": row[8],
                "created_at": row[9]
            })
            
        return messages
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/groups/", response_model=List[GroupInfo])
async def get_groups():
    """获取所有群组信息"""
    logger.debug("Received request for groups")
    try:
        db = DatabaseManager()
        with db._lock:
            db.cursor.execute("""
                SELECT 
                    group_id,
                    group_name,
                    COUNT(*) as message_count,
                    MAX(create_time) as last_active
                FROM group_messages 
                GROUP BY group_id, group_name
                ORDER BY last_active DESC
            """)
            rows = db.cursor.fetchall()
            
        groups = []
        for row in rows:
            groups.append({
                "group_id": row[0],
                "group_name": row[1],
                "message_count": row[2],
                "last_active": datetime.fromtimestamp(row[3]).strftime('%Y-%m-%d %H:%M:%S')
            })
            
        logger.info(f"Successfully retrieved {len(groups)} groups")
        return groups
    
    except Exception as e:
        logger.error(f"Error getting groups: {e}")
        raise

@app.get("/messages/{msg_id}", response_model=GroupMessage)
async def get_message_by_id(msg_id: str):
    """根据消息ID获取具体消息"""
    try:
        db = DatabaseManager()
        with db._lock:
            db.cursor.execute("SELECT * FROM group_messages WHERE msg_id = ?", (msg_id,))
            row = db.cursor.fetchone()
            
        if not row:
            raise HTTPException(status_code=404, detail="Message not found")
            
        return {
            "id": row[0],
            "msg_id": row[1],
            "group_id": row[2],
            "group_name": row[3],
            "sender_id": row[4],
            "sender_name": row[5],
            "content": row[6],
            "msg_type": row[7],
            "create_time": row[8],
            "created_at": row[9]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def start_api_server(host="0.0.0.0", port=8000):
    """启动API服务器"""
    uvicorn.run(
        app, 
        host=host,
        port=port,
        access_log=True,
        log_level="debug",  # 添加详细日志
        workers=1,
        reload=False
    )

def run_api_server_in_thread(host="0.0.0.0", port=8000):
    """在新线程中运行API服务器"""
    try:
        logger.info(f"Starting API server on {host}:{port}")
        api_thread = threading.Thread(
            target=start_api_server,
            args=(host, port),
            daemon=True
        )
        api_thread.start()
        return api_thread
    except Exception as e:
        logger.error(f"Failed to start API server: {e}")
        raise