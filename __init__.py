"""Forge platform adapter plugin for Hermes."""

try:
    from .adapter import register
except ImportError:
    from adapter import register

__all__ = ["register"]
