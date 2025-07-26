# -*- coding: utf-8 -*-

import punq, httpx

from .config import config_manager


def create_container() -> punq.Container:
    """创建并配置依赖注入容器"""
    container = punq.Container()

    # 注册配置管理器
    container.register("ConfigManager", instance=config_manager)

    def create_http_client() -> httpx.AsyncClient:
        proxy_config = config_manager.get("proxy", {})
        proxy_url = proxy_config.get("url")
        if proxy_url:
            return httpx.AsyncClient(proxies=proxy_url)
        return httpx.AsyncClient()

    container.register(httpx.AsyncClient, factory=create_http_client)

    return container

# 全局容器实例
container = create_container()