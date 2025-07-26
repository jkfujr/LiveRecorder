# -*- coding: utf-8 -*-

import json
from pathlib import Path
from typing import Any, Dict

class ConfigManager:
    def __init__(self, config_path: str = 'config.json'):
        self.config_path = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置项"""
        return self._config.get(key, default)

    @property
    def all_config(self) -> Dict[str, Any]:
        """获取所有配置"""
        return self._config

# 全局配置实例
config_manager = ConfigManager()