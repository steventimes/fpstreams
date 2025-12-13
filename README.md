# pystream

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A robust, type-safe functional programming library for Python.**

`pystream` brings the power of **Java Streams**, **Rust Results**, and **JavaScript Array methods** to Python. It provides a fluent interface for data processing, null safety, and error handling without the boilerplate, all while remaining fully typed for IDE autocompletion.

## Features

* **Fluent Streams:** Lazy evaluation chains (`map`, `filter`, `reduce`, `zip`)
* **Null Safety:** `Option` monad to eliminate `None` checks.
* **Error Handling:** `Result` monad (Success/Failure) to replace ugly `try/except` blocks.
* **Powerful Collectors:** Grouping, partitioning, and joining made simple.
* **Functional Tools:** Utilities like `pipe` and `curry` for cleaner composition.

## Installation

```bash
pip install pystream
```

## Usage Example

### Streams (using name from Java)

```python
from pystream import Stream

data = [
    {"name": "Alice", "role": "admin", "age": 30},
    {"name": "Bob", "role": "dev", "age": 25},
    {"name": "Charlie", "role": "admin", "age": 45}
]

# Get names of admins whose age is over 25, sorted alphabetically
names = (
    Stream(data)
    .filter(lambda u: u["role"] == "admin")
    .filter(lambda u: u["age"] > 25)
    .map(lambda u: u["name"].upper())
    .sorted()
    .to_list()
)
# Output: ['ALICE', 'CHARLIE']
```

### Null Safety with ```Option```

```python
from pystream import Stream

# Find the first user named "Steven" (who doesn't exist)
email = (
    Stream(data)
    .filter(lambda u: u["name"] == "Steven")
    .find_first()               # Returns Option[User]
    .map(lambda u: u["email"])  # Skipped because Option is empty
    .or_else("default@example.com")
)
```

### Error handling with ```Result```

```python
from pystream import Result

def risky_parsing(value):
    return int(value) # Might crash if value is not a number

# Safe execution
result = (
    Result.of(lambda: risky_parsing("invalid"))
    .map(lambda x: x * 2)
    .on_failure(lambda e: print(f"Parsing failed: {e}")) # Logs error
    .get_or_else(0) # Returns 0 instead of crashing
)
```

### Collectors

grouping data using collectors

```python
from pystream import Stream, Collectors

fruits = ["apple", "avocado", "banana", "blueberry", "cherry"]

# Group fruits by their first letter
grouped = (
    Stream(fruits)
    .collect(Collectors.grouping_by(lambda s: s[0]))
)
# Output: {'a': ['apple', 'avocado'], 'b': ['banana', 'blueberry'], 'c': ['cherry']}
```

### Infinite Streams & Lazy Evaluation

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

## Project Structure

1.```Stream```: The core wrapper for iterables.\
2.```Option```: A container for optional values.\
3.```Result```: A container for operations that may fail.\
4.```Collectors```: Helper functions for aggregation.\
5.```functional```: Utilities like ```pipe``` and ```curry```
