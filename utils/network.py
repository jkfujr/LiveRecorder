import httpx, anyio
from typing import Any, Dict, Optional
from http.cookies import SimpleCookie
from httpx_socks import AsyncProxyTransport
from loguru import logger


class NetworkMixin:
    flag: str
    client: httpx.AsyncClient
    headers: Dict[str, str]
    cookies: Optional[str]
    ssl: bool
    proxy: Optional[str]
    """网络相关功能的Mixin类"""
    
    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """HTTP请求封装"""
        try:
            response = await self.client.request(method, url, **kwargs)
            return response
        except httpx.ProtocolError as error:
            raise ConnectionError(f'{self.flag}直播检测请求协议错误\n{error}')
        except httpx.HTTPStatusError as error:
            raise ConnectionError(
                f'{self.flag}直播检测请求状态码错误\n{error}\n{response.text}')
        except anyio.EndOfStream as error:
            raise ConnectionError(f'{self.flag}直播检测代理错误\n{error}')
        except httpx.HTTPError as error:
           logger.error(f'网络异常 重试...')
           raise ConnectionError(f'{self.flag}直播检测请求错误\n{repr(error)}')

    def get_client(self) -> httpx.AsyncClient:
        """创建HTTP客户端"""
        client_kwargs = {
            'timeout': httpx.Timeout(30.0),
            'headers': self.headers,
            'cookies': self.cookies,
            'verify': self.ssl,
            'follow_redirects': True
        }
        if self.proxy:
            if 'socks' in self.proxy:
                client_kwargs['transport'] = AsyncProxyTransport.from_url(self.proxy)
            else:
                # 使用 proxy 而不是 proxies, 较新版本的 httpx proxies 参数已被移除
                client_kwargs['proxy'] = self.proxy
        return httpx.AsyncClient(**client_kwargs)

    def get_cookies(self) -> None:
        """处理Cookie字符串"""
        if self.cookies:
            cookies = SimpleCookie()
            cookies.load(self.cookies)
            self.cookies = {k: v.value for k, v in cookies.items()}