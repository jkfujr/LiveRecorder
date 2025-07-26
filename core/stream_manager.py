import streamlink
from streamlink.session import Streamlink
from typing import Optional
from loguru import logger

from .ffmpeg_manager import ffmpeg_manager


class StreamManagerMixin:
    flag: str
    ssl: bool
    proxy: str
    headers: dict
    cookies: str
    format: str
    """流管理相关功能的Mixin类"""
    
    def get_streamlink(self) -> Streamlink:
        """获取streamlink会话"""
        session = streamlink.session.Streamlink({
            'stream-segment-timeout': 60,
            'hls-segment-queue-threshold': 10
        })
        ssl = self.ssl
        logger.info(f'是否验证SSL：{ssl}')
        session.set_option('http-ssl-verify', ssl)
        if proxy := self.proxy:
            # 代理为socks5时，streamlink的代理参数需要改为socks5h，防止部分直播源获取失败
            if 'socks' in proxy:
                proxy = proxy.replace('://', 'h://')
            session.set_option('http-proxy', proxy)
        if self.headers:
            session.set_option('http-headers', self.headers)
        if self.cookies:
            session.set_option('http-cookies', self.cookies)
        return session

    async def run_ffmpeg_async(self, filename: str, format: str) -> Optional[str]:
        """异步FFmpeg进行格式转换"""
        logger.info(f'{self.flag}开始异步ffmpeg封装：{filename}')
        
        try:
            # 创建进度回调函数
            async def progress_callback(task_id: str, time_info: str):
                logger.debug(f'{self.flag}FFmpeg进度 [{task_id}]: {time_info}')
            
            task_id = await ffmpeg_manager.convert_format(
                input_file=filename,
                output_format=self.format,
                progress_callback=progress_callback,
                codec='copy',
                map_metadata='-1',
                movflags='faststart'
            )
            
            logger.info(f'{self.flag}FFmpeg任务已启动: {task_id}')
            return task_id
            
        except Exception as e:
            logger.error(f'{self.flag}启动FFmpeg异步任务失败：{filename}\n错误信息：{e}')
            return None

    async def wait_ffmpeg_completion(self, task_id: str, timeout: Optional[float] = 300) -> bool:
        """等待FFmpeg任务完成"""
        if not task_id:
            return False
            
        try:
            success = await ffmpeg_manager.wait_for_task(task_id, timeout=timeout)
            
            if success:
                logger.info(f'{self.flag}FFmpeg任务完成: {task_id}')
            else:
                task_info = ffmpeg_manager.get_task_info(task_id)
                if task_info:
                    logger.error(f'{self.flag}FFmpeg任务失败: {task_id}, 状态: {task_info["status"]}')
                    if task_info.get('error_message'):
                        logger.error(f'{self.flag}错误信息: {task_info["error_message"]}')
                else:
                    logger.error(f'{self.flag}FFmpeg任务不存在: {task_id}')
            
            return success
            
        except Exception as e:
            logger.error(f'{self.flag}等待FFmpeg任务异常: {task_id}, 错误: {e}')
            return False