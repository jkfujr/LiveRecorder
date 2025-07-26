"""
虎牙直播平台实现
"""
import re
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Huya(LiveRecorder):
    """虎牙直播平台录制器"""
    
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