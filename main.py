#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, sys
from typing import Any, Dict, List, Type
from loguru import logger
from pathlib import Path

# 根目录
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 核心模块
from core.state_manager import recording_manager
from core.config import config_manager

# 工具模块
from utils.template import TemplateEngine, time_zone, format_date

# 平台模块
from platforms.base import LiveRecorder
from platforms import (
    Bilibili, Douyu, Huya, Douyin, Youtube, Twitch,
    Niconico, Twitcasting, Afreeca, Pandalive, Bigolive,
    Pixivsketch, Chaturbate
)


# 平台类映射
PLATFORM_CLASSES: Dict[str, Type[LiveRecorder]] = {
    'bilibili': Bilibili,
    'douyu': Douyu,
    'huya': Huya,
    'douyin': Douyin,
    'youtube': Youtube,
    'twitch': Twitch,
    'niconico': Niconico,
    'twitcasting': Twitcasting,
    'afreeca': Afreeca,
    'pandalive': Pandalive,
    'bigolive': Bigolive,
    'pixivsketch': Pixivsketch,
    'chaturbate': Chaturbate
}

# 支持的平台列表
SUPPORTED_PLATFORMS = list(PLATFORM_CLASSES.keys())

# 便捷函数
def get_platform_class(platform_name: str):
    """根据平台名称获取对应的平台类"""
    platform_name = platform_name.lower()
    if platform_name not in PLATFORM_CLASSES:
        raise ValueError(f"不支持的平台: {platform_name}. 支持的平台: {', '.join(SUPPORTED_PLATFORMS)}")
    return PLATFORM_CLASSES[platform_name]

def create_recorder(platform_name: str, config: Dict[str, Any], user: Dict[str, Any]) -> LiveRecorder:
    """创建录制实例"""
    platform_class = get_platform_class(platform_name)
    return platform_class(config, user)

def check_version():
    """检查版本信息"""
    return {
        'version': __version__,
        'author': __author__,
        'description': __description__,
        'supported_platforms': SUPPORTED_PLATFORMS
    }

def configure_template_engine():
    """配置模板"""
    engine = TemplateEngine()
    engine.add_filter('time_zone', time_zone)
    engine.add_filter('format_date', format_date)
    return engine

template_engine = configure_template_engine()


async def run():
    """主函数，读取配置并启动所有录制任务"""
    config: Dict[str, Any] = config_manager.all_config
    try:
        tasks: List[asyncio.Task] = []
        users: List[Dict[str, Any]] = config_manager.get('user', [])
        for item in users:
            platform_name = item['platform'].lower()
            if platform_name in PLATFORM_CLASSES:
                platform_class = PLATFORM_CLASSES[platform_name]
                recorder = create_recorder(platform_name, config, item)
                coro = recorder.start()
                tasks.append(asyncio.create_task(coro))
            else:
                logger.error(f"未知的平台类型: {platform_name}. 支持的平台: {', '.join(SUPPORTED_PLATFORMS)}")
                
        if tasks:
            await asyncio.wait(tasks)
        else:
            logger.warning("没有有效的录制任务")
            
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        logger.warning('用户中断录制，正在关闭直播流')
        # 使用录制管理器获取所有录制信息并关闭流
        all_recordings = await recording_manager.get_all_recordings()
        for url, recording_info in all_recordings.items():
            if recording_info.stream_fd:
                recording_info.stream_fd.close()
            if recording_info.output:
                recording_info.output.close()
            # 停止录制状态
            await recording_manager.stop_recording(url, "用户中断")


def main():
    """程序主入口点"""
    # 配置日志
    logger.add(
        sink='logs/log_{time:YYYY-MM-DD}.log',
        rotation='00:00',
        retention='3 days',
        level='DEBUG',
        encoding='utf-8',
        format='[{time:YYYY-MM-DD HH:mm:ss}][{level}][{name}][{function}:{line}]{message}'
    )
    
    logger.configure(handlers=[{"sink": sys.stdout, "level": "INFO"}])
    
    # 运行主程序
    asyncio.run(run())


if __name__ == '__main__':
    main()