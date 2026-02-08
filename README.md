# fpstreams

[![Build Status](https://github.com/steventimes/fpstreams/actions/workflows/test.yml/badge.svg)](https://github.com/steventimes/fpstreams/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/fpstreams.svg)](https://badge.fury.io/py/fpstreams)

**A robust, type-safe functional programming library for Python.**

`fpstreams` brings the power of **Java Streams**, **Rust Results**, and **JavaScript Array methods** to Python. It provides a fluent interface for data processing, null safety, and error handling without the boilerplate, all while remaining fully typed for IDE autocompletion.

## Features

* **Fluent Streams:** Lazy evaluation chains (`map`, `filter`, `reduce`, `zip`).
* **Structure Operations:** Powerful chunking with `.batch()`, `.window()`, and `.zip_longest()`.
* **Parallel Processing:** Memory-safe multi-core distribution with `.parallel()` and auto-batching.
* **Advanced Statistics:** One-pass summary stats (`.summarizing()`) and SQL-like grouping (`.grouping_by(..., downstream=...)`).
* **Clean Code Syntax:** Syntactic sugar like `.pick()` and `.filter_none()` to replace lambdas.
* **Data Science Ready:** Convert streams directly to Pandas DataFrames, NumPy arrays, or CSV/JSON files.
* **Null Safety:** `Option` to eliminate `None` checks.
* **Error Handling:** `Result` (Success/Failure) to replace ugly `try/except` blocks.

## Installation

```bash
pip install fpstreams
```

## Quick Start

### 1. Stream Factories

Create streams directly from values, functions, or algorithmic sequences.

```python
from fpstreams import Stream

Stream.of(1, 2, 3, 4, 5)

# seed 1, Function: x * 2 -> 1, 2, 4, 8, 16...
Stream.iterate(1, lambda x: x * 2).limit(10)

# Infinite polling (e.g., API)
Stream.generate(lambda: random.random()).limit(5)
```

### 2. Basic Processing

Replace messy loops with clean, readable pipelines.

```python
from fpstreams import Stream, Collectors

data = ["apple", "banana", "cherry", "apricot", "blueberry"]

# Filter, transform, and group in one
result = (
    Stream(data)
    .filter(lambda s: s.startswith("a") or s.startswith("b"))
    .map(str.upper)
    .collect(Collectors.grouping_by(lambda s: s[0]))
)
# Output: {'A': ['APPLE', 'APRICOT'], 'B': ['BANANA', 'BLUEBERRY']}
```

### 3. Structure & Windowing

Process data in chunks or sliding windows—essential for time-series analysis or bulk API processing.

```python
data = range(10)

# Batching: Process 3 items at a time
# Result: [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
Stream(data).batch(3).to_list()

# Sliding Window: View of size 3, sliding by 1
# Result: [[0, 1, 2], [1, 2, 3], [2, 3, 4]...]
Stream(data).window(size=3, step=1).to_list()
```

### 4. Clean Code Shortcuts

Stop writing repetitive lambdas for dictionaries.

```python
users = [
    {"id": 1, "name": "Alice", "role": "admin"},
    {"id": 2, "name": "Bob", "role": None},
    {"id": 3, "name": None, "role": "user"},
]

names = (
    Stream(users)
    .pick("name")      # Extract "name" key
    .filter_none()     # Remove None values
    .to_list()
)
# Output: ["Alice", "Bob"]
```

### 5. Parallel Processing

`fpstreams` can automatically distribute heavy workloads across all CPU cores using the `.parallel()` method. It uses an optimized Map-Reduce architecture to minimize memory usage.

```python
import math
from fpstreams import Stream

def heavy_task_batch(numbers):
    # Process a whole list of numbers at once (Vectorization or bulk API)
    return [math.factorial(n) for n in numbers]

# Memory Efficient: "batch(100)" sends chunks to workers
# instead of pickling 10,000 individual tasks.
results = (
    Stream(range(10000))
    .parallel()
    .batch(100) 
    .map(heavy_task_batch)
    .to_list()
)
```

### 4. Data Science & I/O

Seamlessly integrate with the scientific stack.

```python
# 1. One-pass Statistics (Count, Sum, Min, Max, Avg)
stats = Stream(users).collect(Collectors.summarizing(lambda u: u['age']))
print(f"Average Age: {stats.average}, Max: {stats.max}")

# 2. Advanced Grouping (SQL-style)
# Group by Dept, then Avg Salary
avg_salaries = Stream(employees).collect(
    Collectors.grouping_by(
        lambda e: e['dept'],
        downstream=Collectors.averaging(lambda e: e['salary'])
    )
)

# 3. Export
Stream(users).to_df()
Stream(users).to_csv("output.csv")
```

## Infinite Streams & Lazy Evaluation

Process massive datasets efficiently. Operations are only executed when needed.

```python
# Infinite stream of even numbers using .iterate()
evens = (
    Stream.iterate(0, lambda n: n + 1)
    .filter(lambda x: x % 2 == 0)
    .limit(10)
    .to_list()
)
```

## Benchmark

Comparison between standard streams and `fpstreams.parallel()` on a 4-core machine:

| Task | Sequential(s) | Parallel(s) | Speedup |
| :--- | :--- | :--- | :--- |
| **Heavy Calculation** (Factorials) | 24.8358 | 9.5575 | **2.60x** |
| **I/O Simulation** (Sleep) | 2.1053 | 0.8101 | **2.60x** |
| **Light Calculation** (Multiplication) | 0.0135 | 0.3109 | 0.04x |

*Note: Parallel streams have overhead. Use them for CPU-intensive tasks or slow I/O, not simple arithmetic.*

## Project Structure

* **`Stream`**: The core wrapper for sequential data processing.
* **`ParallelStream`**: A multi-core wrapper for heavy parallel processing.
* **`Option`**: Null-safe container.
* **`Result`**: Error-handling container.
* **`Collectors`**: Accumulation utilities (grouping, joining, summary stats).

## Functional Coverage & Roadmap

`fpstreams` already delivers composable pipelines, collectors, and Option/Result containers, but there are a few areas worth extending (additional combinators, richer statistics, and more ergonomic Option/Result helpers). A longer-term path is to introduce an optional Rust extension to accelerate numeric-heavy collectors and parallel operations while keeping the Python-first API intact. See the roadmap for details: [docs/roadmap.md](docs/roadmap.md).

## Licence

This project is licensed under the MIT License - see the LICENSE file for details.
