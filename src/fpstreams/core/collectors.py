from typing import TypeVar, Callable, Dict, List, Any, Iterable, Set, Tuple
from dataclasses import dataclass

T = TypeVar("T")
K = TypeVar("K")
R = TypeVar("R")
U = TypeVar("U")
V = TypeVar("V")

@dataclass
class SummaryStatistics:
    """
    Holds the state for statistical summary (count, sum, min, max, average).
    """
    count: int = 0
    sum: float = 0.0
    min: float = float('inf')
    max: float = float('-inf')
    average: float = 0.0

    def accept(self, value: float):
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        self.average = self.sum / self.count

    def __str__(self):
        return (f"SummaryStatistics(count={self.count}, sum={self.sum}, "
                f"min={self.min}, average={self.average:.2f}, max={self.max})")

class Collectors:
    """
    Common implementations of Collector operations.
    """

    @staticmethod
    def to_list() -> Callable[[Iterable[T]], List[T]]:
        return list

    @staticmethod
    def to_set() -> Callable[[Iterable[T]], Set[T]]:
        return set

    @staticmethod
    def joining(delimiter: str = "") -> Callable[[Iterable[Any]], str]:
        def collector(iterable: Iterable[Any]) -> str:
            return delimiter.join(map(str, iterable))
        return collector


    @staticmethod
    def grouping_by(
        classifier: Callable[[T], K], 
        downstream: Callable[[Iterable[T]], R] | None = None
    ) -> Callable[[Iterable[T]], Dict[K, Any]]:
        """
        Groups elements by a classification function.
        
        Args:
            classifier: Function to determine the key.
            downstream: Optional collector to apply to the values of each group.
                        If None, defaults to to_list().
        
        Example:
            grouping_by(lambda x: len(x), downstream=Collectors.counting())
        """
        def collector(iterable: Iterable[T]) -> Dict[K, Any]:
            groups: Dict[K, List[T]] = {}
            for item in iterable:
                key = classifier(item)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
                
            if downstream is None:
                return groups
            return {k: downstream(v) for k, v in groups.items()}
        
        return collector

    @staticmethod
    def summarizing(mapper: Callable[[T], float]) -> Callable[[Iterable[T]], SummaryStatistics]:
        """
        Returns a SummaryStatistics object (count, sum, min, average, max).
        """
        def collector(iterable: Iterable[T]) -> SummaryStatistics:
            stats = SummaryStatistics()
            for item in iterable:
                val = mapper(item)
                stats.accept(val)
            # If stream was empty, fix min/max for cleaner display
            if stats.count == 0:
                stats.min = 0.0
                stats.max = 0.0
            return stats
        return collector

    @staticmethod
    def counting() -> Callable[[Iterable[T]], int]: # type: ignore
        """Counts the elements."""
        return lambda iterable: sum(1 for _ in iterable)

    @staticmethod
    def summing(mapper: Callable[[T], float]) -> Callable[[Iterable[T]], float]:
        """Sums the elements after mapping them."""
        def collector(iterable: Iterable[T]) -> float:
            return sum(mapper(x) for x in iterable)
        return collector

    @staticmethod
    def averaging(mapper: Callable[[T], float]) -> Callable[[Iterable[T]], float]:
        """Calculates the average."""
        def collector(iterable: Iterable[T]) -> float:
            count = 0
            total = 0.0
            for item in iterable:
                count += 1
                total += mapper(item)
            return total / count if count > 0 else 0.0
        return collector

    @staticmethod
    def mapping(
        mapper: Callable[[T], U], 
        downstream: Callable[[Iterable[U]], R]
    ) -> Callable[[Iterable[T]], R]:
        """
        Adapts a collector to accept elements of a different type.
        Useful inside grouping_by.
        
        Example:
            grouping_by(user.department, mapping(user.salary, averaging()))
        """
        def collector(iterable: Iterable[T]) -> R:
            return downstream(map(mapper, iterable))
        return collector
    
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