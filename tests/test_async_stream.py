import pytest
import asyncio
import time
from fpstreams import Stream
from fpstreams.core.async_stream import AsyncStream
from fpstreams.functional import retry

pytestmark = pytest.mark.asyncio

# --- Fixtures & Helpers ---

async def async_double(x: int) -> int:
    """Simulates a network call."""
    await asyncio.sleep(0.01)
    return x * 2

async def async_slow_task(x: int) -> int:
    """Simulates a slow task."""
    await asyncio.sleep(0.2)
    return x

# --- Core Logic Tests ---

@pytest.mark.asyncio
async def test_creation_and_collection():
    result = await AsyncStream.from_iterable([1, 2, 3]).to_list()
    assert result == [1, 2, 3]

@pytest.mark.asyncio
async def test_sync_transformation():
    """Test standard sync map/filter inside async stream."""
    result = await (
        AsyncStream.from_iterable(range(10))
        .filter(lambda x: x % 2 == 0)
        .map(lambda x: x * 2)
        .to_list()
    )
    assert result == [0, 4, 8, 12, 16]

@pytest.mark.asyncio
async def test_bridge_from_sync_stream():
    """Test .to_async() conversion."""
    result = await Stream.of(1, 2, 3).to_async().map(lambda x: x + 1).to_list()
    assert result == [2, 3, 4]

# --- Concurrency Tests (The Engine) ---

@pytest.mark.asyncio
async def test_map_async_gather_ordered():
    """Ensure gather preserves order by default."""
    data = [1, 2, 3, 4, 5]
    result = await (
        AsyncStream.from_iterable(data)
        .map_async(async_double)
        .gather()
        .to_list()
    )
    assert result == [2, 4, 6, 8, 10]

@pytest.mark.asyncio
async def test_map_async_unordered():
    """Ensure unordered() returns all results (order may vary)."""
    data = [1, 2, 3, 4, 5]
    
    async def variable_sleep(x):
        await asyncio.sleep((5 - x) * 0.01) # 5 sleeps longest, 1 sleeps shortest
        return x

    result = await (
        AsyncStream.from_iterable(data)
        .map_async(variable_sleep)
        .unordered()
        .gather()
        .to_list()
    )
    
    # Order might be reversed due to sleep times, but set must match
    assert set(result) == {1, 2, 3, 4, 5}
    # Sanity check: verify it's NOT just the input order (1 finishes last)
    assert result != [1, 2, 3, 4, 5] 

@pytest.mark.asyncio
async def test_concurrency_limit():
    """
    Verify that concurrency limit prevents running everything at once.
    We run 10 tasks that take 0.1s each.
    With limit=1, it should take ~1.0s.
    With limit=10, it should take ~0.1s.
    """
    start = time.time()
    await (
        AsyncStream.from_iterable(range(5))
        .concurrent(limit=1) # Force serial execution
        .map_async(async_slow_task)
        .gather()
        .to_list()
    )
    duration = time.time() - start
    
    # 5 tasks * 0.2s = 1.0s minimum if serial. 
    # If parallel, it would be 0.2s.
    assert duration >= 0.9 

# --- Resilience Tests ---

@pytest.mark.asyncio
async def test_timeout_mechanism():
    """Ensure timeout raises error for slow tasks."""
    async def never_ending(x):
        await asyncio.sleep(10)
        return x

    with pytest.raises(asyncio.TimeoutError):
        await (
            AsyncStream.of(1)
            .map_async(never_ending)
            .timeout(0.1) # Should fail fast
            .gather()
            .to_list()
        )

@pytest.mark.asyncio
async def test_retry_decorator():
    """Test the functional.retry decorator integration."""
    attempts = 0
    
    @retry(attempts=3, backoff=0.01, jitter=False)
    async def flaky_task(x):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("Fail")
        return x

    result = await (
        AsyncStream.of(100)
        .map_async(flaky_task) # Should retry internally
        .gather()
        .to_list()
    )
    
    assert result == [100]
    assert attempts == 3

@pytest.mark.asyncio
async def test_debounce():
    """Test dropping items that are too fast."""
    async def fast_producer():
        yield 1
        yield 2 # Should be dropped (too fast)
        await asyncio.sleep(0.15)
        yield 3 # Should be kept (0.15 > 0.1)

    result = await (
        AsyncStream.from_aiterable(fast_producer())
        .debounce(wait_ms=100)
        .to_list()
    )

    assert result == [1, 3]

# --- I/O Tests ---

@pytest.mark.asyncio
async def test_file_io(tmp_path):
    file_path = tmp_path / "test.txt"
    lines = ["hello", "world", "async", "streams"]
    with open(file_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    read_lines = await (
        AsyncStream.from_file(str(file_path))
        .map(lambda s: s.upper())
        .to_list()
    )
    
    assert read_lines == ["HELLO", "WORLD", "ASYNC", "STREAMS"]

    out_path = tmp_path / "out.txt"
    await (
        AsyncStream.from_iterable(read_lines)
        .to_file_async(str(out_path))
    )
    
    # Verify Write
    with open(out_path, "r") as f:
        content = f.read().splitlines()
    assert content == read_lines

@pytest.mark.asyncio
async def test_interval_stream():
    """Test infinite interval stream with limit."""
    start = time.time()
    result = await (
        AsyncStream.interval(0.1)
        .limit(3)
        .to_list()
    )
    duration = time.time() - start
    
    assert result == [0, 1, 2]
    # Should take at least 0.1 * 3 = 0.3s
    assert duration >= 0.28