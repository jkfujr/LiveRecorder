"""
BigoLive直播平台实现
"""
from streamlink.stream.hls import HLSStream
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Bigolive(LiveRecorder):
    """BigoLive直播平台录制器"""
    
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