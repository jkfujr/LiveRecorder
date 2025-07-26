import os, time
from datetime import datetime
from pathlib import Path
from typing import Tuple

from .template import TemplateEngine
from loguru import logger


class FileHandlerMixin:
    flag: str
    platform: str
    id: str
    name: str
    output: str
    env: TemplateEngine
    """文件处理相关功能的Mixin类"""
    
    def get_filename(self, title: str, format: str) -> str:
        """获取录制文件名"""
        title = title or "title"

        # 文件名特殊字符转换为全角字符
        char_dict = {
            '"': '＂',
            '*': '＊',
            ':': '：',
            '<': '＜',
            '>': '＞',
            '?': '？',
            '/': '／',
            '\\': '＼',
            '|': '｜'
        }
        for half, full in char_dict.items():
            title = title.replace(half, full)

        # 调用模板处理
        directory, filename = self.render_filename_template(title, format)
        
        # 限制文件名长度
        max_length = 240
        name_part, ext_part = os.path.splitext(filename)
        if len(filename.encode('utf-8')) > max_length:
            encoded_name = name_part.encode('utf-8')
            encoded_ext = ext_part.encode('utf-8')
            max_name_bytes = max_length - len(encoded_ext)
            truncated_name_bytes = encoded_name[:max_name_bytes]
            while True:
                try:
                    truncated_name = truncated_name_bytes.decode('utf-8')
                    break
                except UnicodeDecodeError:
                    truncated_name_bytes = truncated_name_bytes[:-1]
            
            truncated_filename = truncated_name + ext_part
            # 使用pathlib.Path处理路径
            directory_path = Path(directory)
            full_title_path = directory_path / (truncated_name + '.txt')
            
            try:
                directory_path.mkdir(parents=True, exist_ok=True)
                with open(full_title_path, 'w', encoding='utf-8') as f:
                    f.write(f"完整标题: {title}\n")
                    f.write(f"录制URL: {self.flag}\n")
                    f.write(f"录制时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                logger.info(f"{self.flag}标题过长，已截断并保存完整标题到: {full_title_path}")
            except Exception as e:
                logger.error(f"{self.flag}保存完整标题失败: {repr(e)}")
            
            filename = truncated_filename

        try:
            directory_path = Path(directory)
            directory_path.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise OSError(f"路径创建失败: {directory}\n{error}")

        # 使用pathlib.Path处理路径并转换为字符串，确保跨平台兼容性
        result_path = Path(directory) / filename
        return str(result_path)

    def render_filename_template(self, title: str, format: str) -> Tuple[str, str]:
        """渲染文件名模板"""
        # 模板参数
        context = {
            "platform": self.platform,
            "id": self.id,
            "name": self.name,
            "title": title,
            "format": format,
            "now": datetime.now()
        }

        # 如果没有配置文件名模板则使用默认模板
        if not self.output or '{{' not in self.output:
            return self.default_filename_template(title, format)

        try:
            template = self.env.from_string(self.output)
            rendered_output = template.render(context)

            if "{{" in rendered_output or "}}" in rendered_output:
                raise ValueError(f"路径中存在未解析的模板变量: {rendered_output}")

            # 使用pathlib.Path处理路径分割，确保跨平台兼容性
            path_obj = Path(rendered_output)
            directory = str(path_obj.parent) if path_obj.parent != Path('.') else ""
            filename = path_obj.name

            if not directory:
                directory = "output"

            return directory, filename

        except (KeyError, ValueError) as e:
            logger.warning(f"{self.flag}模板渲染失败，使用默认文件名模板。错误信息: {e}")
            return self.default_filename_template(title, format)

    def default_filename_template(self, title: str, format: str) -> Tuple[str, str]:
        """默认文件名模板"""
        live_time = time.strftime('%Y.%m.%d %H.%M.%S')
        filename = f'[{live_time}]{self.flag}{title[:50]}.{format}'
        directory = self.output or 'output'
        return directory, filename