"""
PandaTV直播平台实现
"""
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Pandalive(LiveRecorder):
    """PandaTV直播平台录制器"""
    
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