import asyncio
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple
from streamlink.stream import StreamIO
from streamlink_cli.output import FileOutput
from loguru import logger


class RecordingState(Enum):
    """录制状态枚举"""
    IDLE = "idle"
    STARTING = "starting"
    RECORDING = "recording"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class RecordingInfo:
    """录制信息数据类"""
    url: str
    stream_fd: Optional[StreamIO] = None
    output: Optional[FileOutput] = None
    state: RecordingState = RecordingState.IDLE
    start_time: Optional[datetime] = None
    title: str = ""
    filename: str = ""
    error_msg: str = ""


class RecordingManager:
    """基于asyncio.Queue的录制状态管理器"""
    
    def __init__(self):
        self._recordings: Dict[str, RecordingInfo] = {}
        self._state_queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        
    async def is_recording(self, url: str) -> bool:
        """检查是否正在录制"""
        async with self._lock:
            info = self._recordings.get(url)
            return info is not None and info.state in (RecordingState.STARTING, RecordingState.RECORDING)
    
    async def start_recording(self, url: str, title: str = "", filename: str = "") -> bool:
        """开始录制，返回是否成功开始"""
        async with self._lock:
            # 检查是否已在录制
            if url in self._recordings:
                current_state = self._recordings[url].state
                if current_state in (RecordingState.STARTING, RecordingState.RECORDING):
                    return False
            
            # 创建录制信息
            info = RecordingInfo(
                url=url,
                state=RecordingState.STARTING,
                start_time=datetime.now(),
                title=title,
                filename=filename
            )
            self._recordings[url] = info
            
            # 发送状态变更通知
            await self._state_queue.put(('start', url, info))
            return True
    
    async def set_recording_streams(self, url: str, stream_fd: StreamIO, output: FileOutput) -> bool:
        """设置录制流对象"""
        async with self._lock:
            if url not in self._recordings:
                return False
            
            info = self._recordings[url]
            info.stream_fd = stream_fd
            info.output = output
            info.state = RecordingState.RECORDING
            
            # 发送状态变更通知
            await self._state_queue.put(('recording', url, info))
            return True
    
    async def stop_recording(self, url: str, error_msg: str = "") -> Optional[RecordingInfo]:
        """停止录制，返回录制信息"""
        async with self._lock:
            if url not in self._recordings:
                return None
            
            info = self._recordings[url]
            info.state = RecordingState.STOPPING if not error_msg else RecordingState.ERROR
            info.error_msg = error_msg
            
            # 发送状态变更通知
            await self._state_queue.put(('stop', url, info))
            
            # 清理资源
            if info.output:
                try:
                    info.output.close()
                except Exception as e:
                    logger.warning(f"关闭输出流失败: {e}")
            
            # 从管理器中移除
            removed_info = self._recordings.pop(url, None)
            return removed_info
    
    async def get_recording_info(self, url: str) -> Optional[RecordingInfo]:
        """获取录制信息"""
        async with self._lock:
            return self._recordings.get(url)
    
    async def get_all_recordings(self) -> Dict[str, RecordingInfo]:
        """获取所有录制信息"""
        async with self._lock:
            return self._recordings.copy()
    
    async def cleanup_error_recordings(self) -> int:
        """清理错误状态的录制，返回清理数量"""
        async with self._lock:
            error_urls = [
                url for url, info in self._recordings.items() 
                if info.state == RecordingState.ERROR
            ]
            
            for url in error_urls:
                info = self._recordings.pop(url, None)
                if info and info.output:
                    try:
                        info.output.close()
                    except Exception:
                        pass
            
            return len(error_urls)
    
    async def get_state_updates(self) -> Tuple[str, str, RecordingInfo]:
        """获取状态更新队列（用于监控）"""
        return await self._state_queue.get()


# 全局录制管理器实例
recording_manager = RecordingManager()