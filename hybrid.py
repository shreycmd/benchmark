from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import os
import time
import mmap

FILES = [
    "test/file_1.txt",
    "test/file_2.txt",
    "test/file_3.txt",
    "test/file_4.txt"
]

WORDS = [
    "banana",
    "cloud",
    "apple",
    "Rohan"
]


# ------------------------- THREAD HELPERS -------------------------

# Helper for Approach 1 (mmap find loop - 2 threads)
def search_region_in_mmap_loop(mm, word_bytes, start_byte, end_byte, overlap):
    pos = start_byte
    count = 0
    limit = end_byte + overlap
    while True:
        pos = mm.find(word_bytes, pos, limit)
        if pos == -1 or pos >= end_byte:
            break
        count += 1
        pos += 1
    return count


# Helper for Approach 2 (mmap slicing copy - 4 threads)
def search_region_in_mmap_slice(mm, word_bytes, start_byte, end_byte, overlap):
    chunk = mm[start_byte : end_byte + overlap]
    return chunk.count(word_bytes)


# Helper for Approach 3 (Shared bytes count - 4 threads)
def search_region_in_data(data, word_bytes, start_byte, end_byte, overlap):
    return data.count(word_bytes, start_byte, end_byte + overlap)


# ------------------------- WORKERS -------------------------

# Approach 1: Zero-Copy Mmap Loop (4 Threads, 4 Partitions)
def worker_mmap_loop(filename, word):
    start = time.perf_counter()
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            word_bytes = word.encode("utf-8")
            overlap = len(word_bytes) - 1
            file_size = len(mm)
            part = file_size // 4
            ranges = [
                (0, part), (part, part * 2), (part * 2, part * 3), (part * 3, file_size)
            ]
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(search_region_in_mmap_loop, mm, word_bytes, s, e, overlap)
                    for s, e in ranges
                ]
                count = sum(f.result() for f in futures)
    end = time.perf_counter()
    return count, end - start


# Approach 2: Slicing Mmap (4 Threads, 4 Partitions)
def worker_mmap_slice(filename, word):
    start = time.perf_counter()
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            word_bytes = word.encode("utf-8")
            overlap = len(word_bytes) - 1
            file_size = len(mm)
            part = file_size // 4
            ranges = [
                (0, part), (part, part * 2), (part * 2, part * 3), (part * 3, file_size)
            ]
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(search_region_in_mmap_slice, mm, word_bytes, s, e, overlap)
                    for s, e in ranges
                ]
                count = sum(f.result() for f in futures)
    end = time.perf_counter()
    return count, end - start


# Approach 3: Read Once + Shared Bytes Count (4 Threads, 4 Partitions)
def worker_read_once(filename, word):
    start = time.perf_counter()
    with open(filename, "rb") as f:
        data = f.read()
    word_bytes = word.encode("utf-8")
    overlap = len(word_bytes) - 1
    file_size = len(data)
    part = file_size // 4
    ranges = [
        (0, part), (part, part * 2), (part * 2, part * 3), (part * 3, file_size)
    ]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(search_region_in_data, data, word_bytes, s, e, overlap)
            for s, e in ranges
        ]
        count = sum(f.result() for f in futures)
    end = time.perf_counter()
    return count, end - start


# ------------------------- BENCHMARK RUNNER -------------------------

def run_benchmark(name, worker_func):
    print(f"\n========== {name} ==========")
    start_total = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Pre-warm processes
        list(executor.map(abs, [1, 2, 3, 4]))
        
        start_work = time.perf_counter()
        results = list(executor.map(worker_func, FILES, WORDS))
        end_work = time.perf_counter()
        
    end_total = time.perf_counter()
    counts = [r[0] for r in results]
    times = [r[1] for r in results]
    print("Counts        :", counts)
    print("Worker Times  :", [f"{t:.4f}s" for t in times])
    print("Execution Time:", end_total - start_total, "sec")
    print(f"  * Pure Processing Time: {end_work - start_work:.4f} sec")


if __name__ == "__main__":
    # Warm up page caches first
    print("Warming up files...")
    for f in FILES:
        with open(f, "rb") as file:
            _ = file.read(1024)

    run_benchmark("APPROACH 1: MMAP LOOP (2 Threads, 2 Partitions)", worker_mmap_loop)
    run_benchmark("APPROACH 2: MMAP SLICING (4 Threads, 4 Partitions)", worker_mmap_slice)
    run_benchmark("APPROACH 3: READ ONCE + BOUNDS (4 Threads, 4 Partitions)", worker_read_once)
