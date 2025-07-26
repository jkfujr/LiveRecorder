"""
Chaturbate直播平台实现
"""
from streamlink.stream.hls import HLSStream
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Chaturbate(LiveRecorder):
    """Chaturbate直播平台录制器"""
    
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