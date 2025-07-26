"""
Niconico直播平台实现
"""
import json, re
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Niconico(LiveRecorder):
    """Niconico直播平台录制器"""
    
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