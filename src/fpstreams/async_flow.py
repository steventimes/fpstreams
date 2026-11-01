"""Re-export asynchronous flow types and the `aflow` constructor for legacy imports."""

from fpstreams.streams.async_flow import AsyncFlow, AsyncStream, aflow

__all__ = ["AsyncFlow", "AsyncStream", "aflow"]
