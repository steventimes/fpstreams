# fpstreams

[![Build Status](https://github.com/steventimes/fpstreams/actions/workflows/test.yml/badge.svg)](https://github.com/steventimes/fpstreams/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://badge.fury.io/py/fpstreams.svg)](https://badge.fury.io/py/fpstreams)

**A robust, type-safe functional programming library for Python.**

`fpstreams` brings the power of **Java Streams**, **Rust Results**, and **JavaScript Array methods** to Python. It provides a fluent interface for data processing, null safety, and error handling without the boilerplate, all while remaining fully typed for IDE autocompletion.

## Features

* **Fluent Streams:** Lazy evaluation chains (`map`, `filter`, `reduce`, `zip`).
* **Parallel Processing:** Automatic multi-core distribution with `.parallel()`.
* **Clean Code Syntax:** Syntactic sugar like `.pick()` and `.filter_none()` to replace lambdas.
* **Data Science Ready:** Convert streams directly to Pandas DataFrames, NumPy arrays, or CSV/JSON files.
* **Null Safety:** `Option` to eliminate `None` checks.
* **Error Handling:** `Result` (Success/Failure) to replace ugly `try/except` blocks.

## Installation

```bash
pip install fpstreams
```

## Quick Start

### 1. Basic

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

### 2. Clean Code Shortcuts

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

### 3. Parallel Processing

`fpstreams` can automatically distribute heavy workloads across all CPU cores using the `.parallel()` method. It uses an optimized Map-Reduce architecture to minimize memory usage.

```python
import math
from fpstreams import Stream

def heavy_task(x):
    return math.factorial(5000)

# Automatically uses all available CPU cores
results = (
    Stream(range(1000))
    .parallel()
    .map(heavy_task)
    .to_list()
)
```

### 4. Data Science & I/O

Seamlessly integrate with the scientific stack.

```python
# Quick statistics
stats = Stream([1, 2, 3, 4, 5, 100]).describe()

# Output: {'count': 6, 'sum': 115, 'mean': 19.16, 'min': 1, 'max': 100, ...}

# Convert to Pandas
df = Stream(users).to_df()

# Stream directly to file
Stream(users).to_csv("output.csv")
Stream(users).to_json("output.json")
```

## Infinite Streams & Lazy Evaluation

Process massive datasets efficiently. Operations are only executed when needed.

```python
def infinite_counter():
    n = 0
    while True:
        yield n
        n += 1

# Take only the first 10 even numbers
evens = (
    Stream(infinite_counter())
    .filter(lambda x: x % 2 == 0)
    .limit(10)
    .to_list()
)
```

## Benchmark

Comparison between standard streams and `fpstreams.parallel()` on a 4-core machine:

| Task | Sequential(s) | Parallel(s) | Speedup |
| :--- | :--- | :--- | :--- |
| **Heavy Calculation** (Factorials) | 24.7603 | 10.8182 | **2.29x** |
| **I/O Simulation** (Sleep) | 2.0986 | 0.8405 | **2.50x** |
| **Light Calculation** (Multiplication) | 0.0151 | 0.3796 | 0.04x |

*Note: Parallel streams have overhead. Use them for CPU-intensive tasks or slow I/O, not simple arithmetic.*

## Project Structure

* **`Stream`**: The core wrapper for sequential data processing.
* **`ParallelStream`**: A multi-core wrapper for heavy parallel processing.
* **`Option`**: Null-safe container.
* **`Result`**: Error-handling container.
* **`Collectors`**: Accumulation utilities (grouping, joining, summary stats).

## Licence

This project is licensed under the MIT License - see the LICENSE file for details.
