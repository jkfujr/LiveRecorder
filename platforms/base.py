import asyncio, re, httpx
from typing import Any, Dict, Optional, Union
from pathlib import Path
from streamlink.stream import StreamIO, HTTPStream
from streamlink_cli.main import open_stream
from streamlink_cli.output import FileOutput
from streamlink_cli.streamrunner import StreamRunner
from loguru import logger

from utils.template import TemplateEngine, time_zone, format_date
from utils.network import NetworkMixin
from utils.file_handler import FileHandlerMixin
from core.stream_manager import StreamManagerMixin
from core.state_manager import recording_manager


class LiveRecorder(NetworkMixin, FileHandlerMixin, StreamManagerMixin):
    """直播录制器基类"""
    
    id: str
    platform: str
    name: str
    flag: str
    interval: int
    crypto_js_url: str
    headers: Dict[str, str]
    cookies: Optional[str]
    format: str
    proxy: Optional[str]
    output: str
    ssl: bool
    client: httpx.AsyncClient
    env: TemplateEngine
    mState: Union[int, str] = 0

    """直播录制器基类"""
    
    def __init__(self, config: Dict[str, Any], user: Dict[str, Any]):
        self.id = user['id']
        platform = user['platform']
        self.platform = platform
        self.name = user.get('name', self.id)
        self.flag = f'[{platform}][{self.name}]'
        
        self.interval = user.get('interval', config.get('interval', 10))
        self.crypto_js_url = user.get('crypto_js_url', '')
        self.headers = user.get('headers', {'User-Agent': 'Chrome'})
        self.cookies = user.get('cookies')
        self.format = user.get('format', 'flv')
        self.proxy = user.get('proxy', config.get('proxy'))
        self.output = user.get('output', config.get('output', 'output'))
        self.ssl = True
        if not self.crypto_js_url:
            self.crypto_js_url = 'https://cdnjs.cloudflare.com/ajax/libs/crypto-js/4.1.1/crypto-js.min.js'
        self.get_cookies()
        self.client = self.get_client()

        self.env = TemplateEngine()
        self.env.add_filter('time_zone', time_zone)
        self.env.add_filter('format_date', format_date)

    async def start(self):
        """开始监控直播状态"""
        self.ssl = True
        self.mState = 0
        while True:
            try:
                logger.debug(f'{self.flag}正在检测直播状态')
                logger.debug(f'预配置刷新间隔：{self.interval}s')
                try:
                    await self.run()   
                except Exception as run_error:
                    logger.error(f"{self.flag}直播检测内部错误\n{repr(run_error)}")
                state = self.mState
                timeI = self.interval
                if state == '1':
                    timeI = 2
                logger.debug(f'->直播状态：{state}  实际刷新间隔：{timeI}s')
                await asyncio.sleep(timeI)
            except ConnectionError as error:
                if '直播检测请求协议错误' not in str(error):
                    logger.error(error)
                await self.client.aclose()
                self.client = self.get_client()
            except Exception as error:
                logger.exception(f'{self.flag}直播检测错误\n{repr(error)}')

    async def run(self):
        """子类需要实现的直播检测方法"""
        pass

    async def run_record(self, stream: Union[StreamIO, HTTPStream], url: str, title: str, format: str) -> None:
        """执行录制"""
        # 获取输出文件名
        filename = self.get_filename(title, format)
        if stream:
            logger.info(f'{self.flag}开始录制：{filename}')
            # 调用streamlink录制直播
            result = await self.stream_writer(stream, url, filename, title)
            # 录制成功、format配置存在且不等于直播平台默认格式时运行ffmpeg封装
            if result and self.format and self.format != format:
                # 使用异步FFmpeg处理，不阻塞主线程
                task_id = await self.run_ffmpeg_async(filename, format)
                if task_id:
                    logger.info(f'{self.flag}FFmpeg转换任务已启动，任务ID: {task_id}')
                else:
                    logger.error(f'{self.flag}FFmpeg异步任务启动失败')
                    # 如果FFmpeg处理失败，仍然保留原文件
                    logger.warning(f'{self.flag}保留原始文件: {filename}')
            # 停止录制并清理状态
            await recording_manager.stop_recording(url)
            logger.info(f'{self.flag}停止录制：{filename}')
        else:
            logger.error(f'{self.flag}无可用直播源：{filename}')
            # 标记为错误状态
            await recording_manager.stop_recording(url, "无可用直播源")

    async def stream_writer(self, stream: Union[StreamIO, HTTPStream], url: str, filename: str, title: str = "") -> bool:
        """流写入器"""
        logger.info(f'{self.flag}获取到直播流链接：{filename}\n{stream.url}')
        output = FileOutput(Path(filename))
        try:
            stream_fd, prebuffer = open_stream(stream)
            output.open()
            
            # 设置录制流对象到管理器
            await recording_manager.set_recording_streams(url, stream_fd, output)
            logger.info(f'{self.flag}正在录制：{filename}')
            # 移除 show_progress 参数，新版本的 Streamlink 会自动处理进度显示，不需要手动指定 show_progress 参数
            await asyncio.to_thread(StreamRunner(stream_fd, output).run, prebuffer)
            return True
        except Exception as error:
            error_msg = str(error)
            if 'timeout' in error_msg:
                logger.warning(f'{self.flag}直播录制超时，请检查主播是否正常开播或网络连接是否正常：{filename}\n{error}')
            elif re.search(f'SSL: CERTIFICATE_VERIFY_FAILED', error_msg):
                logger.warning(f'{self.flag}SSL错误，将取消SSL验证：{filename}\n{error}')
                self.ssl = False
            elif re.search(f'(Unable to open URL|No data returned from stream)', error_msg):
                logger.warning(f'{self.flag}直播流打开错误，请检查主播是否正常开播：{filename}\n{error}')
            else:
                logger.exception(f'{self.flag}直播录制错误：{filename}\n{error}')
            # 标记为错误状态
            await recording_manager.stop_recording(url, error_msg)
            return False
        finally:
            output.close()