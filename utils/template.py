import re, pytz
from typing import Any, Callable, Dict, Optional, Union
from datetime import datetime


class TemplateEngine:
    """轻量模板"""
    
    def __init__(self) -> None:
        self.filters: Dict[str, Callable[..., Any]] = {}
    """轻量模板"""
    
    def __init__(self):
        self.filters = {}
    
    def add_filter(self, name: str, func: Callable[..., Any]) -> None:
        """注册过滤器函数"""
        self.filters[name] = func
    
    def from_string(self, template: str) -> 'Template':
        """创建模板对象"""
        return Template(template, self)


class Template:
    """模板类"""
    
    def __init__(self, template: str, engine: 'TemplateEngine') -> None:
        self.template = template
        self.engine = engine
    """模板类"""
    
    def __init__(self, template, engine):
        self.template = template
        self.engine = engine
    
    def render(self, context: Optional[Dict[str, Any]] = None, **kwargs: Any) -> str:
        """渲染模板"""
        # 如果没有传递 context，使用 kwargs 作为 context
        if context is None:
            context = kwargs
        elif kwargs:
            # 如果同时传递了 context 和 kwargs，合并它们
            context = {**context, **kwargs}
        
        # 使用正则表达式找到所有模板变量 {{ ... }} 或 { ... }
        # 优先匹配双大括号，然后匹配单大括号
        pattern = r'\{\{\s*([^}]+)\s*\}\}|\{\s*([^}]+)\s*\}'
        
        def replace_var(match):
            # 双大括号匹配在 group(1)，单大括号匹配在 group(2)
            expr = (match.group(1) or match.group(2)).strip()
            return self._evaluate_expression(expr, context)
        
        return re.sub(pattern, replace_var, self.template)
    
    def _evaluate_expression(self, expr: str, context: Dict[str, Any]) -> str:
        """评估表达式，支持变量和管道过滤器"""
        # 解析管道过滤器
        parts = [p.strip() for p in expr.split('|')]
        var_name = parts[0]
        
        # 获取变量值
        if var_name not in context:
            raise KeyError(f"Template variable '{var_name}' not found in context")
        
        value = context[var_name]
        
        # 应用过滤器链
        for filter_expr in parts[1:]:
            value = self._apply_filter(filter_expr, value)
        
        return str(value)
    
    def _apply_filter(self, filter_expr: str, value: Any) -> Any:
        """应用单个过滤器"""
        # 解析过滤器名称和参数
        if ':' in filter_expr:
            filter_name, args_str = filter_expr.split(':', 1)
            filter_name = filter_name.strip()
            # 简单的参数解析（处理引号和逗号分隔）
            args = []
            for arg in args_str.split(','):
                arg = arg.strip()
                # 移除引号
                if (arg.startswith("'") and arg.endswith("'")) or (arg.startswith('"') and arg.endswith('"')):
                    arg = arg[1:-1]
                args.append(arg)
        else:
            filter_name = filter_expr.strip()
            args = []
        
        if filter_name not in self.engine.filters:
            raise ValueError(f"Unknown filter: {filter_name}")
        
        return self.engine.filters[filter_name](value, *args)


# 模板过滤器函数
def time_zone(value: Union[int, float, datetime], tz_name: str) -> datetime:
    """时区过滤器"""
    tz = pytz.timezone(tz_name)
    if isinstance(value, (int, float)):
        value = datetime.fromtimestamp(value, tz)
    elif isinstance(value, datetime):
        value = value.astimezone(tz)
    else:
        raise TypeError(f"Unsupported type for time_zone filter: {type(value)}")
    return value


def format_date(value: datetime, date_format: str) -> str:
    """时间格式过滤器"""
    if isinstance(value, datetime):
        return value.strftime(date_format)[:-3]
    raise TypeError(f"Unsupported type for format_date filter: {type(value)}")