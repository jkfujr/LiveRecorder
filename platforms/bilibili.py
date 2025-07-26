"""
Bilibili平台录制器实现
"""
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Bilibili(LiveRecorder):
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