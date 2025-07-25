import asyncio
import json
import os
import re
import sys
import time
import uuid
import pytz
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Dict, Tuple, Union, Optional, NamedTuple
from urllib.parse import parse_qs
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

import anyio
import ffmpeg
import httpx
import jsengine
import streamlink
from httpx_socks import AsyncProxyTransport
from jsonpath_ng.ext import parse
from loguru import logger
from streamlink.options import Options
from streamlink.stream import StreamIO, HTTPStream, HLSStream
from streamlink_cli.main import open_stream
from streamlink_cli.output import FileOutput
from streamlink_cli.streamrunner import StreamRunner


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
    
    async def get_state_updates(self):
        """获取状态更新队列（用于监控）"""
        return await self._state_queue.get()


# 全局录制管理器实例
recording_manager = RecordingManager()


class TemplateEngine:
    """轻量级模板引擎，替代liquid依赖"""
    
    def __init__(self):
        self.filters = {}
    
    def add_filter(self, name, func):
        """注册过滤器函数"""
        self.filters[name] = func
    
    def from_string(self, template):
        """创建模板对象"""
        return Template(template, self)

class Template:
    """模板类"""
    
    def __init__(self, template, engine):
        self.template = template
        self.engine = engine
    
    def render(self, context):
        """渲染模板"""
        # 使用正则表达式找到所有模板变量 {{ ... }}
        pattern = r'\{\{\s*([^}]+)\s*\}\}'
        
        def replace_var(match):
            expr = match.group(1).strip()
            return self._evaluate_expression(expr, context)
        
        return re.sub(pattern, replace_var, self.template)
    
    def _evaluate_expression(self, expr, context):
        """评估表达式，支持变量和管道过滤器"""
        # 解析管道过滤器
        parts = [p.strip() for p in expr.split('|')]
        var_name = parts[0]
        
        # 获取变量值
        if var_name not in context:
            raise KeyError(f"Template variable '{var_name}' not found in context")
        
        value = context[var_name]
        
        # 应用过滤器链
        for filter_expr in parts[1:]:
            value = self._apply_filter(filter_expr, value)
        
        return str(value)
    
    def _apply_filter(self, filter_expr, value):
        """应用单个过滤器"""
        # 解析过滤器名称和参数
        if ':' in filter_expr:
            filter_name, args_str = filter_expr.split(':', 1)
            filter_name = filter_name.strip()
            # 简单的参数解析（处理引号和逗号分隔）
            args = []
            for arg in args_str.split(','):
                arg = arg.strip()
                # 移除引号
                if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
                    arg = arg[1:-1]
                args.append(arg)
        else:
            filter_name = filter_expr.strip()
            args = []
        
        if filter_name not in self.engine.filters:
            raise ValueError(f"Unknown filter: {filter_name}")
        
        return self.engine.filters[filter_name](value, *args)


class LiveRecoder:
    def __init__(self, config: dict, user: dict):
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
        self.env.add_filter('time_zone', self.time_zone)
        self.env.add_filter('format_date', self.format_date)

    async def start(self):
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
        pass

    async def request(self, method, url, **kwargs):
        try:
            response = await self.client.request(method, url, **kwargs)
            return response
        except httpx.ProtocolError as error:
            raise ConnectionError(f'{self.flag}直播检测请求协议错误\n{error}')
        except httpx.HTTPStatusError as error:
            raise ConnectionError(
                f'{self.flag}直播检测请求状态码错误\n{error}\n{response.text}')
        except anyio.EndOfStream as error:
            raise ConnectionError(f'{self.flag}直播检测代理错误\n{error}')
        except httpx.HTTPError as error:
           logger.error(f'网络异常 重试...')
           raise ConnectionError(f'{self.flag}直播检测请求错误\n{repr(error)}')
		
           

    def get_client(self):
        client_kwargs = {
            'timeout': httpx.Timeout(30.0),
            'headers': self.headers,
            'cookies': self.cookies,
            'verify': self.ssl,
            'follow_redirects': True
        }
        if self.proxy:
            if 'socks' in self.proxy:
                client_kwargs['transport'] = AsyncProxyTransport.from_url(self.proxy)
            else:
                # 使用 proxy 而不是 proxies, 较新版本的 httpx proxies 参数已被移除
                client_kwargs['proxy'] = self.proxy
        return httpx.AsyncClient(**client_kwargs)

    def get_cookies(self):
        if self.cookies:
            cookies = SimpleCookie()
            cookies.load(self.cookies)
            self.cookies = {k: v.value for k, v in cookies.items()}

    # 时区
    def time_zone(self, value, tz_name):
        tz = pytz.timezone(tz_name)
        if isinstance(value, (int, float)):
            value = datetime.fromtimestamp(value, tz)
        elif isinstance(value, datetime):
            value = value.astimezone(tz)
        else:
            raise TypeError(f"Unsupported type for time_zone filter: {type(value)}")
        return value

    # 时间格式
    def format_date(self, value, date_format):
        if isinstance(value, datetime):
            return value.strftime(date_format)[:-3]
        raise TypeError(f"Unsupported type for format_date filter: {type(value)}")

    def get_filename(self, title, format):
        title = title or "title"

        # 文件名特殊字符转换为全角字符
        char_dict = {
            '"': '＂',
            '*': '＊',
            ':': '：',
            '<': '＜',
            '>': '＞',
            '?': '？',
            '/': '／',
            '\\': '＼',
            '|': '｜'
        }
        for half, full in char_dict.items():
            title = title.replace(half, full)

        # 调用模板处理
        directory, filename = self.render_filename_template(title, format)
        
        # 限制文件名长度
        max_length = 240
        name_part, ext_part = os.path.splitext(filename)
        if len(filename.encode('utf-8')) > max_length:
            encoded_name = name_part.encode('utf-8')
            encoded_ext = ext_part.encode('utf-8')
            max_name_bytes = max_length - len(encoded_ext)
            truncated_name_bytes = encoded_name[:max_name_bytes]
            while True:
                try:
                    truncated_name = truncated_name_bytes.decode('utf-8')
                    break
                except UnicodeDecodeError:
                    truncated_name_bytes = truncated_name_bytes[:-1]
            
            truncated_filename = truncated_name + ext_part
            full_title_path = os.path.join(directory, truncated_name + '.txt')
            
            try:
                os.makedirs(directory, exist_ok=True)
                with open(full_title_path, 'w', encoding='utf-8') as f:
                    f.write(f"完整标题: {title}\n")
                    f.write(f"录制URL: {self.flag}\n")
                    f.write(f"录制时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                logger.info(f"{self.flag}标题过长，已截断并保存完整标题到: {full_title_path}")
            except Exception as e:
                logger.error(f"{self.flag}保存完整标题失败: {repr(e)}")
            
            filename = truncated_filename

        try:
            if not os.path.exists(directory):
                os.makedirs(directory)
        except OSError as error:
            raise OSError(f"路径创建失败: {directory}\n{error}")

        return os.path.join(directory, filename)

    def render_filename_template(self, title, format):
        # 模板参数
        context = {
            "platform": self.platform,  # 使用纯平台名
            "id": self.id,
            "name": self.name,
            "title": title,
            "format": format,
            "now": datetime.now()
        }

        # 如果没有配置文件名模板则使用默认模板
        if not self.output or '{{' not in self.output:
            return self.default_filename_template(title, format)

        try:
            template = self.env.from_string(self.output)
            rendered_output = template.render(context)

            if "{{" in rendered_output or "}}" in rendered_output:
                raise ValueError(f"路径中存在未解析的模板变量: {rendered_output}")

            directory, filename = os.path.split(rendered_output)

            if not directory:
                directory = "output"

            return directory, filename

        except (KeyError, ValueError) as e:
            logger.warning(f"{self.flag}模板渲染失败，使用默认文件名模板。错误信息: {e}")
            return self.default_filename_template(title, format)

    def default_filename_template(self, title, format):
        live_time = time.strftime('%Y.%m.%d %H.%M.%S')
        filename = f'[{live_time}]{self.flag}{title[:50]}.{format}'
        directory = self.output or 'output'
        return directory, filename

    def get_streamlink(self):
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

    async def run_record(self, stream: Union[StreamIO, HTTPStream], url, title, format):
        # 获取输出文件名
        filename = self.get_filename(title, format)
        if stream:
            logger.info(f'{self.flag}开始录制：{filename}')
            # 调用streamlink录制直播
            result = await self.stream_writer(stream, url, filename, title)
            # 录制成功、format配置存在且不等于直播平台默认格式时运行ffmpeg封装
            if result and self.format and self.format != format:
                await asyncio.to_thread(self.run_ffmpeg, filename, format)
            # 停止录制并清理状态
            await recording_manager.stop_recording(url)
            logger.info(f'{self.flag}停止录制：{filename}')
        else:
            logger.error(f'{self.flag}无可用直播源：{filename}')
            # 标记为错误状态
            await recording_manager.stop_recording(url, "无可用直播源")

    async def stream_writer(self, stream, url, filename, title=""):
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

    def run_ffmpeg(self, filename, format):
        logger.info(f'{self.flag}开始ffmpeg封装：{filename}')
        directory, file_basename = os.path.split(filename)
        new_basename = file_basename.replace(f'.{format}', f'.{self.format}')
        new_filename = os.path.join(directory, new_basename)

        try:
            ffmpeg.input(filename).output(
                new_filename,
                codec='copy',
                map_metadata='-1',
                movflags='faststart'
            ).global_args('-hide_banner').run()

            # 确保封装成功后再删除原文件
            os.remove(filename)
            logger.info(f'{self.flag}封装完成，原始文件已删除：{filename}')

        except Exception as e:
            logger.error(f'{self.flag}FFmpeg 处理失败：{filename}\n错误信息：{e}')


class Bilibili(LiveRecoder):
    async def run(self):
        url = f'https://live.bilibili.com/{self.id}'
        
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url='https://api.live.bilibili.com/room/v1/Room/get_info',
                params={'room_id': self.id}
            )).json()
            if response['data']['live_status'] == 1:
                title = response['data']['title']
                if await recording_manager.start_recording(url, title):
                    stream = self.get_streamlink().streams(url).get('best')  # HTTPStream[flv]
                    await self.run_record(stream, url, title, 'flv')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Douyu(LiveRecoder):
    async def run(self):
        url = f'https://www.douyu.com/{self.id}'
        
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url=f'https://open.douyucdn.cn/api/RoomApi/room/{self.id}',
            )).json()
            state = response['data']['room_status']
            self.mState = state
            logger.info(
                f'直播状态[1已开播，2未开播]：{state} 上一次开播时间：{response["data"]["start_time"]}')
            if state == '1':
                liveUrl = await self.get_live()
                if liveUrl != '':
                    title = response['data']['room_name']
                    if await recording_manager.start_recording(url, title):
                        stream = HTTPStream(
                            self.get_streamlink(),
                            liveUrl
                        )  # HTTPStream[flv]
                        await self.run_record(stream, url, title, 'flv')
                    else:
                        logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')
            else:
                self.ssl = True

    async def get_js(self):
        response = (await self.request(
            method='POST',
            url=f'https://www.douyu.com/swf_api/homeH5Enc?rids={self.id}'
        )).json()
        js_enc = response['data'][f'room{self.id}']
        getUrl = self.crypto_js_url
        crypto_js = (await self.request(
            method='GET',
            url= getUrl
        )).text
        return jsengine.JSEngine(js_enc + crypto_js)

    async def get_live(self):
        did = uuid.uuid4().hex
        tt = str(int(time.time()))
        params = {
            'cdn': 'tct-h5',
            'did': did,
            'tt': tt,
            'rate': 0
        }
        js = await self.get_js()
        query = js.call('ub98484234', self.id, did, tt)
        params.update({k: v[0] for k, v in parse_qs(query).items()})
        response = (await self.request(
            method='POST',
            url=f'https://www.douyu.com/lapi/live/getH5Play/{self.id}',
            params=params
        )).json()
        if response['data'] == '' and response['msg'] != '':
            logger.info(f'直播状态：{response["error"]} {response["msg"]}')
            return ''
        return f"{response['data']['rtmp_url']}/{response['data']['rtmp_live']}"


class Huya(LiveRecoder):
    async def run(self):
        url = f'https://www.huya.com/{self.id}'
        
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url=url
            )).text
            if '"isOn":true' in response:
                title = re.search('"introduction":"(.*?)"', response).group(1)
                
                if await recording_manager.start_recording(url, title):
                    stream = self.get_streamlink().streams(url).get('best')  # HTTPStream[flv]
                    await self.run_record(stream, url, title, 'flv')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Douyin(LiveRecoder):
    async def run(self):
        url = f'https://live.douyin.com/{self.id}'
        
        if not await recording_manager.is_recording(url):
            if not self.client.cookies:
                await self.client.get(url='https://live.douyin.com/')  # 获取ttwid
            response = (await self.request(
                method='GET',
                url='https://live.douyin.com/webcast/room/web/enter/',
                params={
                    'aid': 6383,
                    'device_platform': 'web',
                    'browser_language': 'zh-CN',
                    'browser_platform': 'Win32',
                    'browser_name': 'Chrome',
                    'browser_version': '100.0.0.0',
                    'web_rid': self.id
                },
            )).json()
            if data := response['data']['data']:
                data = data[0]
                if data['status'] == 2:
                    title = data['title']
                    live_url = ''
                    stream_data = json.loads(data['stream_url']['live_core_sdk_data']['pull_data']['stream_data'])
                    for quality_code in ('origin', 'uhd', 'hd', 'sd', 'md', 'ld'):
                        if quality_data := stream_data['data'].get(quality_code):
                            live_url = quality_data['main']['flv']
                            break
                    
                    if await recording_manager.start_recording(url, title):
                        stream = HTTPStream(
                            self.get_streamlink(),
                            live_url
                        )  # HTTPStream[flv]
                        await self.run_record(stream, url, title, 'flv')
                    else:
                        logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Youtube(LiveRecoder):
    def __init__(self, config: dict, user: dict):
        super().__init__(config, user)
        
    async def run(self):
        response = (await self.request(
            method='POST',
            url='https://www.youtube.com/youtubei/v1/browse',
            params={
                'key': 'AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8',
                'prettyPrint': False
            },
            json={
                'context': {
                    'client': {
                        'hl': 'zh-CN',
                        'clientName': 'MWEB',
                        'clientVersion': '2.20230101.00.00',
                        'timeZone': 'Asia/Shanghai'
                    }
                },
                'browseId': self.id,
                'params': 'EgdzdHJlYW1z8gYECgJ6AA%3D%3D'
            }
        )).json()
        
        # 记录当前检测到的所有直播
        current_lives = set()
        
        jsonpath = parse('$..videoWithContextRenderer').find(response)
        for match in jsonpath:
            video = match.value
            if '"style": "LIVE"' in json.dumps(video):
                url = f"https://www.youtube.com/watch?v={video['videoId']}"
                current_lives.add(url)
                title = video['headline']['runs'][0]['text']
                
                # 如果直播未在录制中，则开始录制
                if not await recording_manager.is_recording(url):
                    logger.info(f"{self.flag}检测到新直播: {title}")
                    
                    if await recording_manager.start_recording(url, title):
                        stream = self.get_streamlink().streams(url).get('best')  # HLSStream[mpegts]
                        # 创建录制任务
                        asyncio.create_task(self.record_stream(stream, url, title))
                    else:
                        logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')
    
    async def record_stream(self, stream, url, title):
        """异步包装录制流的方法"""
        try:
            await self.run_record(stream, url, title, 'ts')
        except Exception as e:
            logger.error(f"{self.flag}录制流异常: {url}, {repr(e)}")
            # 确保停止录制状态
            await recording_manager.stop_recording(url, str(e))
            raise


class Twitch(LiveRecoder):
    async def run(self):
        url = f'https://www.twitch.tv/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='POST',
                url='https://gql.twitch.tv/gql',
                headers={'Client-Id': 'kimne78kx3ncx6brgo4mv6wki5h1ko'},
                json=[{
                    'operationName': 'StreamMetadata',
                    'variables': {'channelLogin': self.id},
                    'extensions': {
                        'persistedQuery': {
                            'version': 1,
                            'sha256Hash': 'a647c2a13599e5991e175155f798ca7f1ecddde73f7f341f39009c14dbf59962'
                        }
                    }
                }]
            )).json()
            if response[0]['data']['user']['stream']:
                title = response[0]['data']['user']['lastBroadcast']['title']
                if await recording_manager.start_recording(url, title):
                    options = Options()
                    options.set('disable-ads', True)
                    stream = self.get_streamlink().streams(url, options).get('best')  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Niconico(LiveRecoder):
    async def run(self):
        url = f'https://live.nicovideo.jp/watch/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url=url
            )).text
            if '"content_status":"ON_AIR"' in response:
                title = json.loads(
                    re.search(r'<script type="application/ld\+json">(.*?)</script>', response).group(1)
                )['name']
                if await recording_manager.start_recording(url, title):
                    stream = self.get_streamlink().streams(url).get('best')  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Twitcasting(LiveRecoder):
    async def run(self):
        url = f'https://twitcasting.tv/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url='https://twitcasting.tv/streamserver.php',
                params={
                    'target': self.id,
                    'mode': 'client'
                }
            )).json()
            if response:
                response = (await self.request(
                    method='GET',
                    url=url
                )).text
                title = re.search('<meta name="twitter:title" content="(.*?)">', response).group(1)
                
                if await recording_manager.start_recording(url, title):
                    stream = self.get_streamlink().streams(url).get('best')  # Stream[mp4]
                    await self.run_record(stream, url, title, 'mp4')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Afreeca(LiveRecoder):
    async def run(self):
        url = f'https://play.afreecatv.com/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='POST',
                url='https://live.afreecatv.com/afreeca/player_live_api.php',
                data={'bid': self.id}
            )).json()
            if response['CHANNEL']['RESULT'] != 0:
                title = response['CHANNEL']['TITLE']
                if await recording_manager.start_recording(url, title):
                    stream = self.get_streamlink().streams(url).get('best')  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Pandalive(LiveRecoder):
    async def run(self):
        url = f'https://www.pandalive.co.kr/live/play/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='POST',
                url='https://api.pandalive.co.kr/v1/live/play',
                headers={
                    'x-device-info': '{"t":"webMobile","v":"1.0","ui":0}'
                },
                data={
                    'action': 'watch',
                    'userId': self.id
                }
            )).json()
            if response['result']:
                title = response['media']['title']
                if await recording_manager.start_recording(url, title):
                    stream = self.get_streamlink().streams(url).get('best')  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Bigolive(LiveRecoder):
    async def run(self):
        url = f'https://www.bigo.tv/cn/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='POST',
                url='https://ta.bigo.tv/official_website/studio/getInternalStudioInfo',
                params={'siteId': self.id}
            )).json()
            if response['data']['alive']:
                title = response['data']['roomTopic']
                if await recording_manager.start_recording(url, title):
                    stream = HLSStream(
                        session=self.get_streamlink(),
                        url=response['data']['hls_src']
                    )  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Pixivsketch(LiveRecoder):
    async def run(self):
        url = f'https://sketch.pixiv.net/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url=url
            )).text
            next_data = json.loads(re.search(r'<script id="__NEXT_DATA__".*?>(.*?)</script>', response)[1])
            initial_state = json.loads(next_data['props']['pageProps']['initialState'])
            if lives := initial_state['live']['lives']:
                live = list(lives.values())[0]
                title = live['name']
                if await recording_manager.start_recording(url, title):
                    streams = HLSStream.parse_variant_playlist(
                        session=self.get_streamlink(),
                        url=live['owner']['hls_movie']
                    )
                    stream = list(streams.values())[0]  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


class Chaturbate(LiveRecoder):
    async def run(self):
        url = f'https://chaturbate.com/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='POST',
                url='https://chaturbate.com/get_edge_hls_url_ajax/',
                headers={
                    'X-Requested-With': 'XMLHttpRequest'
                },
                data={
                    'room_slug': self.id
                }
            )).json()
            if response['room_status'] == 'public':
                title = self.id
                if await recording_manager.start_recording(url, title):
                    streams = HLSStream.parse_variant_playlist(
                        session=self.get_streamlink(),
                        url=response['url']
                    )
                    stream = list(streams.values())[2]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')


async def run():
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    try:
        tasks = []
        for item in config['user']:
            platform_class = globals()[item['platform']]
            coro = platform_class(config, item).start()
            tasks.append(asyncio.create_task(coro))
        await asyncio.wait(tasks)
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        logger.warning('用户中断录制，正在关闭直播流')
        # 使用录制管理器获取所有录制信息并关闭流
        all_recordings = await recording_manager.get_all_recordings()
        for url, recording_info in all_recordings.items():
            if recording_info.stream_fd:
                recording_info.stream_fd.close()
            if recording_info.output:
                recording_info.output.close()
            # 停止录制状态
            await recording_manager.stop_recording(url, "用户中断")


if __name__ == '__main__':
    logger.add(
        sink='logs/log_{time:YYYY-MM-DD}.log',
        rotation='00:00',
        retention='3 days',
        level='DEBUG',
        encoding='utf-8',
        format='[{time:YYYY-MM-DD HH:mm:ss}][{level}][{name}][{function}:{line}]{message}'
    )
    
    logger.configure(handlers=[{"sink": sys.stdout, "level": "INFO"}])
    
    asyncio.run(run())
