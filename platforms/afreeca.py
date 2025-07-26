"""
AfreecaTV直播平台实现
"""
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Afreeca(LiveRecorder):
    """AfreecaTV直播平台录制器"""
    
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