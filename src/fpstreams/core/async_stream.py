import asyncio
import functools
from typing import (
    TypeVar, Generic, AsyncIterator, Callable, Awaitable, 
    Any, AsyncIterable, Iterable, List, cast
)
from . import async_ops
from .common import T, R
U = TypeVar("U")  # For factories


class AsyncStream(Generic[T]):
    """
    AsyncStream Implementation.
    Wrapper around an AsyncIterator for processing I/O bound tasks.
    """

    def __init__(self, async_iterator: AsyncIterator[T]):
        self._iterator = async_iterator
        self._concurrency_limit: int | None = None
        self._is_unordered = False

    def __aiter__(self) -> AsyncIterator[T]:
        return self._iterator

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Ensures underlying resources (like file handles) are closed."""
        if hasattr(self._iterator, "aclose"):
            await self._iterator.aclose() # type: ignore

    # --- factories ---
    
    @staticmethod
    def of(*elements: U) -> "AsyncStream[U]":
        """
        Creates a stream from a sequence of values.
        Usage: AsyncStream.of(1, 2, 3)
        """
        return AsyncStream.from_iterable(elements)

    @staticmethod
    def from_iterable(iterable: Iterable[U]) -> "AsyncStream[U]":
        """Creates a stream from a synchronous iterable (list, range, etc)."""
        return AsyncStream(async_ops.from_iterable_async(iterable))

    @staticmethod
    def from_aiterable(aiterable: AsyncIterable[U]) -> "AsyncStream[U]":
        """Wraps an existing async iterable or generator."""
        if isinstance(aiterable, AsyncIterator):
            return AsyncStream(aiterable)
        return AsyncStream(aiterable.__aiter__())

    # --- I/O Sources ---

    @staticmethod
    def from_file(path: str, encoding: str = "utf-8") -> "AsyncStream[str]":
        """
        Reads a file line-by-line asynchronously.
        Requires `aiofiles` to be installed.
        """
        try:
            import aiofiles # type: ignore
        except ImportError:
            raise ImportError("AsyncStream.from_file requires 'aiofiles'. Install with `pip install aiofiles`.")

        async def file_gen() -> AsyncIterator[str]:
            async with aiofiles.open(path, mode='r', encoding=encoding) as f:
                async for line in f:
                    yield line.rstrip('\n')
        
        return AsyncStream(file_gen())

    @staticmethod
    def interval(seconds: float) -> "AsyncStream[int]":
        """Emits an increasing integer counter every N seconds. Infinite stream."""
        async def timer_gen() -> AsyncIterator[int]:
            count = 0
            while True:
                await asyncio.sleep(seconds)
                yield count
                count += 1
        return AsyncStream(timer_gen())

    @staticmethod
    def from_paginated(
        first_page_fetcher: Callable[[], Awaitable[Any]], 
        next_page_fetcher: Callable[[Any], Awaitable[Any]],
        data_extractor: Callable[[Any], Iterable[U]],
        has_next: Callable[[Any], bool]
    ) -> "AsyncStream[U]":
        """
        Stream from a paginated API.
        
        Args:
            first_page_fetcher: Async func to get the first response.
            next_page_fetcher: Async func taking previous response to get next.
            data_extractor: Func extracting a list of items from the response.
            has_next: Func checking if more pages exist.
        """
        async def page_gen() -> AsyncIterator[U]:
            current_response = await first_page_fetcher()
            for item in data_extractor(current_response):
                yield item
            
            while has_next(current_response):
                current_response = await next_page_fetcher(current_response)
                for item in data_extractor(current_response):
                    yield item
                    
        return AsyncStream(page_gen())

    # --- Transformations ---

    def map(self, mapper: Callable[[T], R]) -> "AsyncStream[R]":
        """Applies a synchronous function to each element."""
        async def gen() -> AsyncIterator[R]:
            async for item in self._iterator:
                yield mapper(item)
        return AsyncStream(gen())

    def map_async(self, func: Callable[[T], Awaitable[R]]) -> "AsyncStream[Awaitable[R]]":
        """
        Maps elements to Coroutines/Awaitables. \n
        Returns a stream of PENDING tasks. Use .gather() to execute them.
        """
        return AsyncStream(async_ops.map_async_gen(self._iterator, func))

    def filter(self, predicate: Callable[[T], bool]) -> "AsyncStream[T]":
        return AsyncStream(async_ops.filter_gen(self._iterator, predicate))

    def limit(self, n: int) -> "AsyncStream[T]":
        return AsyncStream(async_ops.limit_gen(self._iterator, n))
    
    def debounce(self, wait_ms: float) -> "AsyncStream[T]":
        """
        Drops items that arrive less than 'wait_ms' after the previous item.
        Useful for filtering high-frequency sensor data or event streams.
        """
        return AsyncStream(async_ops.debounce_gen(self._iterator, wait_ms))

    # --- Concurrency Control ---

    def concurrent(self, limit: int) -> "AsyncStream[T]":
        """Sets the max number of concurrent tasks for the next .gather() call."""
        self._concurrency_limit = limit
        return self

    def unordered(self) -> "AsyncStream[T]":
        """
        Allows .gather() to yield results out-of-order (faster).
        """
        self._is_unordered = True
        return self

    def gather(self) -> "AsyncStream[Any]":
        """
        Executes pending coroutines generated by .map_async().\n
        Applies concurrency limits and ordering rules.
        """
        iterator = cast(AsyncIterator[Awaitable[Any]], self._iterator)
        
        if self._is_unordered:
            return AsyncStream(async_ops.unordered_gather_gen(iterator))
        
        return AsyncStream(async_ops.gather_gen(iterator, limit=self._concurrency_limit))

    def timeout(self, seconds: float) -> "AsyncStream[T]":
        """
        Wraps pending coroutines in asyncio.wait_for with a timeout.
        """
        async def gen() -> AsyncIterator[T]:
            async for coro in self._iterator:
                if not asyncio.iscoroutine(coro) and not isinstance(coro, asyncio.Future):
                    yield coro # type: ignore
                    continue
                yield await asyncio.wait_for(coro, timeout=seconds) # type: ignore
        return AsyncStream(gen())

    # --- Terminal ---

    async def to_list(self) -> List[T]:
        """Collects all elements into a list."""
        return [x async for x in self._iterator]

    async def for_each(self, func: Callable[[T], Any]) -> None:
        """Executes func for each element."""
        async for item in self._iterator:
            res = func(item)
            if asyncio.iscoroutine(res):
                await res

    async def to_file_async(self, path: str, encoding: str = "utf-8") -> None:
        """Writes elements to a file asynchronously."""
        try:
            import aiofiles # type: ignore
        except ImportError:
            raise ImportError("to_file_async requires 'aiofiles'.")
            
        async with aiofiles.open(path, mode='w', encoding=encoding) as f:
            async for item in self._iterator:
                await f.write(str(item) + "\n")

    async def collect(self, collector: Callable[[Iterable[T]], R]) -> R:
        """
        Materializes the stream and applies a standard (sync) Collector.\n
        Example: await stream.collect(Collectors.to_list())
        """
        data = [x async for x in self._iterator]
        return collector(data)