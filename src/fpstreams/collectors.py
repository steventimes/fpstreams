from typing import TypeVar, Callable, Dict, List, Iterable, Any, Tuple

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")

class Collectors:
    @staticmethod
    def to_list() -> Callable[[Iterable[T]], List[T]]:
        """Collects elements into a List."""
        return list

    @staticmethod
    def to_set() -> Callable[[Iterable[T]], set[T]]:
        """Collects elements into a Set."""
        return set

    @staticmethod
    def counting() -> Callable[[Iterable[T]], int]:  # type: ignore
        """Counts the number of elements."""
        def accumulator(iterable: Iterable[T]) -> int:
            return sum(1 for _ in iterable)
        return accumulator

    @staticmethod
    def joining(delimiter: str = "") -> Callable[[Iterable[str]], str]:
        """Joins string elements."""
        def accumulator(iterable: Iterable[str]) -> str:
            return delimiter.join(iterable)
        return accumulator

    @staticmethod
    def grouping_by(key_mapper: Callable[[T], K]) -> Callable[[Iterable[T]], Dict[K, List[T]]]:
        """
        Groups elements by a classification function.
        Example: Stream(["apple", "ant"]).collect(grouping_by(lambda s: s[0]))
        Result: {'a': ['apple', 'ant']}
        """
        def accumulator(iterable: Iterable[T]) -> Dict[K, List[T]]:
            result: Dict[K, List[T]] = {}
            for item in iterable:
                key = key_mapper(item)
                result.setdefault(key, []).append(item)
            return result
        return accumulator
    
    @staticmethod
    def to_dict(key_mapper: Callable[[T], K], value_mapper: Callable[[T], V]) -> Callable[[Iterable[T]], Dict[K, V]]:
        """
        Collects elements into a Dictionary.
        Example: Stream(users).collect(to_dict(lambda u: u.id, lambda u: u.name))
        """
        def accumulator(iterable: Iterable[T]) -> Dict[K, V]:
            return {key_mapper(item): value_mapper(item) for item in iterable}
        return accumulator
    
    @staticmethod
    def partitioning_by(predicate: Callable[[T], bool]) -> Callable[[Iterable[T]], Dict[bool, List[T]]]:
        """
        Partitions elements into two lists based on a predicate.
        Returns a dict: {True: [matches], False: [non-matches]}
        """
        def accumulator(iterable: Iterable[T]) -> Dict[bool, List[T]]:
            result: Dict[bool, List[T]] = {True: [], False: []}
            for item in iterable:
                key = predicate(item)
                result[key].append(item)
            return result
        return accumulator
    
    @staticmethod
    def to_columns() -> Callable[[Iterable[dict]], dict]:
        """
        Transposes a list of dicts into a dict of lists.
        Handles missing keys by inserting None to ensure alignment.
        """
        def accumulator(iterable: Iterable[dict]) -> dict:
            data = list(iterable)
            if not data:
                return {}
            
            all_keys = set()
            for item in data:
                all_keys.update(item.keys())

            result = {k: [] for k in all_keys}

            for item in data:
                for k in all_keys:
                    result[k].append(item.get(k, None))
                    
            return result
        return accumulator