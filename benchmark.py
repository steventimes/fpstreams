import time
import math
import statistics
from typing import List, Callable
from fpstreams import Stream

def task_light(x):
    """Simple multiplication. Sequential should win due to overhead."""
    return x * 10

def task_heavy(x):
    """Factorial calculation. Parallel should win."""
    return math.factorial(5000) 

def task_io_sim(x):
    """Simulates network/disk lag. Parallel should dominate."""
    time.sleep(0.01)
    return x

def measure(name: str, func: Callable, data: List[int], iterations: int = 3) -> float:
    times = []
    for i in range(iterations):
        start = time.time()
        func(data)
        end = time.time()
        times.append(end - start)
    return statistics.mean(times)

def run_suite():
    small_data = list(range(100_000))  # light task
    heavy_data = list(range(20_000))    # heavy task
    io_data = list(range(200))         # IO task

    print(f"{'BENCHMARK REPORT':^60}")
    print("=" * 60)
    print(f"{'Task':<20} | {'Sequential (s)':<15} | {'Parallel (s)':<15} | {'Speedup':<10}")
    print("-" * 60)

    scenarios = [
        ("Light Calculation", task_light, small_data),
        ("Heavy Calculation", task_heavy, heavy_data),
        ("Simulated I/O", task_io_sim, io_data),
    ]

    for title, task, dataset in scenarios:
        def run_seq(d): 
            Stream(d).map(task).to_list()
            
        def run_par(d): 
            Stream(d).parallel(processes=4).map(task).to_list()

        t_seq = measure(title, run_seq, dataset)
        t_par = measure(title, run_par, dataset)

        speedup = t_seq / t_par

        print(f"{title:<20} | {t_seq:<15.4f} | {t_par:<15.4f} | {speedup:<9.2f}x")

    print("-" * 60)

if __name__ == "__main__":
    run_suite()