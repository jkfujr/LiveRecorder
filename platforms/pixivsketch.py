"""
Pixiv Sketch直播平台实现
"""
import json, re
from streamlink.stream.hls import HLSStream
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Pixivsketch(LiveRecorder):
    """Pixiv Sketch直播平台录制器"""
    
    async def run(self):
        url = f'https://sketch.pixiv.net/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='GET',
                url=url
            )).text
            next_data = json.loads(re.search(r'<script id="__NEXT_DATA__".*?>(.*?)</script>', response)[1])
            initial_state = json.loads(next_data['props']['pageProps']['initialState'])
            if lives := initial_state['live']['lives']:
                live = list(lives.values())[0]
                title = live['name']
                if await recording_manager.start_recording(url, title):
                    streams = HLSStream.parse_variant_playlist(
                        session=self.get_streamlink(),
                        url=live['owner']['hls_movie']
                    )
                    stream = list(streams.values())[0]  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')