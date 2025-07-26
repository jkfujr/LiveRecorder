"""
Twitcasting直播平台实现
"""
import re
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Twitcasting(LiveRecorder):
    """Twitcasting直播平台录制器"""
    
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