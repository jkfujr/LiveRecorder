"""
Twitch直播平台实现
"""
from streamlink.options import Options
from loguru import logger

from platforms.base import LiveRecorder
from core.state_manager import recording_manager


class Twitch(LiveRecorder):
    """Twitch直播平台录制器"""
    
    async def run(self):
        url = f'https://www.twitch.tv/{self.id}'
        if not await recording_manager.is_recording(url):
            response = (await self.request(
                method='POST',
                url='https://gql.twitch.tv/gql',
                headers={'Client-Id': 'kimne78kx3ncx6brgo4mv6wki5h1ko'},
                json=[{
                    'operationName': 'StreamMetadata',
                    'variables': {'channelLogin': self.id},
                    'extensions': {
                        'persistedQuery': {
                            'version': 1,
                            'sha256Hash': 'a647c2a13599e5991e175155f798ca7f1ecddde73f7f341f39009c14dbf59962'
                        }
                    }
                }]
            )).json()
            if response[0]['data']['user']['stream']:
                title = response[0]['data']['user']['lastBroadcast']['title']
                if await recording_manager.start_recording(url, title):
                    options = Options()
                    options.set('disable-ads', True)
                    stream = self.get_streamlink().streams(url, options).get('best')  # HLSStream[mpegts]
                    await self.run_record(stream, url, title, 'ts')
                else:
                    logger.warning(f'{self.flag}录制任务启动失败，可能已在录制中')