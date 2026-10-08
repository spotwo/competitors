from __future__ import annotations

import importlib
import inspect
import re
from typing import Any

# Only repository-owned projector modules may be loaded from reviewed registry data.
BUILTIN_MODULES = frozenset({"position_projection", "work_state_projection"})
GENERATED_MODULE = re.compile(r"^event_pipeline_handlers\.[a-z][a-z0-9_]*$")
CLASS_NAME = re.compile(r"^[A-Z][A-Za-z0-9]*$")


def load_pipeline_handler_type(consumer: Any) -> type:
    """Resolve a fail-closed, code-reviewed business projector, not arbitrary imports."""
    module_name = getattr(consumer, "handler_module", None)
    class_name = getattr(consumer, "handler_class", None)
    if not module_name or not class_name:
        raise ValueError("business consumer handler module and class are required")
    if module_name not in BUILTIN_MODULES and not GENERATED_MODULE.fullmatch(module_name):
        raise ValueError("business consumer handler module is not allowlisted")
    if not CLASS_NAME.fullmatch(class_name):
        raise ValueError("business consumer handler class name is invalid")

    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValueError("business consumer handler module cannot be imported") from exc
    handler_type = getattr(module, class_name, None)
    if not inspect.isclass(handler_type) or not callable(
        getattr(handler_type, "__call__", None)
    ):
        raise ValueError("business consumer handler class must implement __call__")
    return handler_type
