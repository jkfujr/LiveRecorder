"""
YouTube直播平台实现
"""
import asyncio, json
from jsonpath_ng.ext import parse
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Youtube(LiveRecorder):
    """YouTube直播平台录制器"""
    
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