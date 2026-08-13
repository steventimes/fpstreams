"""Compatibility facade for asynchronous flows."""

from .streams.async_flow import AsyncFlow, AsyncStream, aflow

__all__ = ["AsyncFlow", "AsyncStream", "aflow"]
