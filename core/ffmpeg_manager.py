"""
FFmpeg异步管理器
解决FFmpeg处理阻塞主线程的问题
"""
import asyncio
import os
import signal
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
from enum import Enum
from loguru import logger
import ffmpeg


class FFmpegTaskStatus(Enum):
    """FFmpeg任务状态"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class FFmpegTask:
    """FFmpeg任务信息"""
    task_id: str
    input_file: str
    output_file: str
    status: FFmpegTaskStatus = FFmpegTaskStatus.PENDING
    process: Optional[subprocess.Popen] = None
    progress_callback: Optional[Callable] = None
    error_message: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None


class AsyncFFmpegManager:
    """异步FFmpeg管理器"""
    
    def __init__(self, max_concurrent_tasks: int = 3):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.tasks: Dict[str, FFmpegTask] = {}
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._task_counter = 0
    
    def _generate_task_id(self) -> str:
        """生成任务ID"""
        self._task_counter += 1
        return f"ffmpeg_task_{self._task_counter}"
    
    async def convert_format(
        self,
        input_file: str,
        output_format: str,
        progress_callback: Optional[Callable] = None,
        **ffmpeg_options
    ) -> str:
        """
        异步格式转换
        
        Args:
            input_file: 输入文件路径
            output_format: 输出格式
            progress_callback: 进度回调函数
            **ffmpeg_options: FFmpeg选项
            
        Returns:
            任务ID
        """
        task_id = self._generate_task_id()
        
        # 生成输出文件名
        input_path = Path(input_file)
        output_file = str(input_path.with_suffix(f'.{output_format}'))
        
        # 创建任务
        task = FFmpegTask(
            task_id=task_id,
            input_file=input_file,
            output_file=output_file,
            progress_callback=progress_callback
        )
        
        self.tasks[task_id] = task
        
        # 启动异步任务
        async_task = asyncio.create_task(
            self._execute_conversion(task, **ffmpeg_options)
        )
        self.running_tasks[task_id] = async_task
        
        logger.info(f"FFmpeg转换任务已创建: {task_id} ({input_file} -> {output_file})")
        return task_id
    
    async def _execute_conversion(self, task: FFmpegTask, **ffmpeg_options) -> bool:
        """执行转换任务"""
        async with self.semaphore:
            try:
                task.status = FFmpegTaskStatus.RUNNING
                task.start_time = asyncio.get_event_loop().time()
                
                logger.info(f"开始FFmpeg转换: {task.task_id}")
                
                # 构建FFmpeg命令
                stream = ffmpeg.input(task.input_file)
                
                # 默认选项
                default_options = {
                    'codec': 'copy',
                    'map_metadata': '-1',
                    'movflags': 'faststart'
                }
                default_options.update(ffmpeg_options)
                
                stream = ffmpeg.output(stream, task.output_file, **default_options)
                stream = ffmpeg.global_args(stream, '-hide_banner', '-y')
                
                # 获取命令行参数
                cmd = ffmpeg.compile(stream)
                
                # 异步执行FFmpeg
                success = await self._run_ffmpeg_process(task, cmd)
                
                if success:
                    task.status = FFmpegTaskStatus.COMPLETED
                    task.end_time = asyncio.get_event_loop().time()
                    
                    # 删除原文件
                    try:
                        os.remove(task.input_file)
                        logger.info(f"FFmpeg转换完成，原文件已删除: {task.input_file}")
                    except OSError as e:
                        logger.warning(f"删除原文件失败: {task.input_file}, 错误: {e}")
                    
                    return True
                else:
                    task.status = FFmpegTaskStatus.FAILED
                    return False
                    
            except Exception as e:
                task.status = FFmpegTaskStatus.FAILED
                task.error_message = str(e)
                logger.error(f"FFmpeg转换异常: {task.task_id}, 错误: {e}")
                return False
            finally:
                task.end_time = asyncio.get_event_loop().time()
                # 清理运行中的任务记录
                self.running_tasks.pop(task.task_id, None)
    
    async def _run_ffmpeg_process(self, task: FFmpegTask, cmd: list) -> bool:
        """运行FFmpeg进程"""
        try:
            # 创建进程
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                preexec_fn=None if os.name == 'nt' else os.setsid
            )
            
            task.process = process
            
            # 监控进程输出
            stdout_task = asyncio.create_task(
                self._monitor_output(task, process.stdout, "stdout")
            )
            stderr_task = asyncio.create_task(
                self._monitor_output(task, process.stderr, "stderr")
            )
            
            # 等待进程完成
            returncode = await process.wait()
            
            # 等待输出监控完成
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            
            if returncode == 0:
                logger.info(f"FFmpeg进程成功完成: {task.task_id}")
                return True
            else:
                logger.error(f"FFmpeg进程失败: {task.task_id}, 返回码: {returncode}")
                return False
                
        except Exception as e:
            logger.error(f"FFmpeg进程执行异常: {task.task_id}, 错误: {e}")
            return False
    
    async def _monitor_output(self, task: FFmpegTask, stream, stream_name: str):
        """监控进程输出"""
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                
                line_str = line.decode('utf-8', errors='ignore').strip()
                if line_str:
                    logger.debug(f"FFmpeg {stream_name} [{task.task_id}]: {line_str}")
                    
                    # 如果有进度回调，尝试解析进度信息
                    if task.progress_callback and 'time=' in line_str:
                        try:
                            await self._parse_progress(task, line_str)
                        except Exception as e:
                            logger.debug(f"解析FFmpeg进度失败: {e}")
                            
        except Exception as e:
            logger.debug(f"监控FFmpeg输出异常: {e}")
    
    async def _parse_progress(self, task: FFmpegTask, line: str):
        """解析FFmpeg进度信息"""
        if task.progress_callback:
            try:
                if 'time=' in line:
                    time_part = line.split('time=')[1].split()[0]
                    await task.progress_callback(task.task_id, time_part)
            except Exception:
                pass
    
    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        if task_id not in self.tasks:
            return False
        
        task = self.tasks[task_id]
        
        # 取消异步任务
        if task_id in self.running_tasks:
            self.running_tasks[task_id].cancel()
        
        # 终止进程
        if task.process and task.process.returncode is None:
            try:
                if os.name == 'nt':
                    task.process.terminate()
                else:
                    os.killpg(os.getpgid(task.process.pid), signal.SIGTERM)
                try:
                    await asyncio.wait_for(task.process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    if os.name == 'nt':
                        task.process.kill()
                    else:
                        os.killpg(os.getpgid(task.process.pid), signal.SIGKILL)
                        
            except Exception as e:
                logger.error(f"终止FFmpeg进程失败: {task_id}, 错误: {e}")
        
        task.status = FFmpegTaskStatus.CANCELLED
        logger.info(f"FFmpeg任务已取消: {task_id}")
        return True
    
    def get_task_status(self, task_id: str) -> Optional[FFmpegTaskStatus]:
        """获取任务状态"""
        task = self.tasks.get(task_id)
        return task.status if task else None
    
    def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务详细信息"""
        task = self.tasks.get(task_id)
        if not task:
            return None
        
        info = {
            'task_id': task.task_id,
            'input_file': task.input_file,
            'output_file': task.output_file,
            'status': task.status.value,
            'error_message': task.error_message,
            'start_time': task.start_time,
            'end_time': task.end_time
        }
        
        if task.start_time and task.end_time:
            info['duration'] = task.end_time - task.start_time
        elif task.start_time:
            info['duration'] = asyncio.get_event_loop().time() - task.start_time
            
        return info
    
    def get_running_tasks(self) -> list:
        """获取正在运行的任务列表"""
        return [
            task_id for task_id, task in self.tasks.items()
            if task.status == FFmpegTaskStatus.RUNNING
        ]
    
    async def wait_for_task(self, task_id: str, timeout: Optional[float] = None) -> bool:
        """等待任务完成"""
        if task_id not in self.running_tasks:
            task = self.tasks.get(task_id)
            return task and task.status == FFmpegTaskStatus.COMPLETED
        
        try:
            await asyncio.wait_for(self.running_tasks[task_id], timeout=timeout)
            task = self.tasks.get(task_id)
            return task and task.status == FFmpegTaskStatus.COMPLETED
        except asyncio.TimeoutError:
            logger.warning(f"等待FFmpeg任务超时: {task_id}")
            return False
        except Exception as e:
            logger.error(f"等待FFmpeg任务异常: {task_id}, 错误: {e}")
            return False
    
    async def cleanup_completed_tasks(self, max_age_hours: float = 24):
        """清理已完成的任务记录"""
        current_time = asyncio.get_event_loop().time()
        max_age_seconds = max_age_hours * 3600
        
        to_remove = []
        for task_id, task in self.tasks.items():
            if (task.status in [FFmpegTaskStatus.COMPLETED, FFmpegTaskStatus.FAILED, FFmpegTaskStatus.CANCELLED] 
                and task.end_time 
                and current_time - task.end_time > max_age_seconds):
                to_remove.append(task_id)
        
        for task_id in to_remove:
            del self.tasks[task_id]
            logger.debug(f"清理过期任务记录: {task_id}")
    
    async def shutdown(self):
        """关闭管理器，取消所有运行中的任务"""
        logger.info("正在关闭FFmpeg管理器...")
        
        # 取消所有运行中的任务
        cancel_tasks = []
        for task_id in list(self.running_tasks.keys()):
            cancel_tasks.append(self.cancel_task(task_id))
        
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
        
        logger.info("FFmpeg管理器已关闭")


ffmpeg_manager = AsyncFFmpegManager()