"""
抖音直播平台实现
"""
import json
from streamlink.stream import HTTPStream
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Douyin(LiveRecorder):
    """抖音直播平台录制器"""
    
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