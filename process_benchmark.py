from concurrent.futures import ProcessPoolExecutor
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


# ------------------------- WORKERS -------------------------

# Approach 1: Chunked Reads (Current - 100MB chunks, No mmap)
def search_chunked(filename, target_word):
    start = time.perf_counter()
    word_bytes = target_word.encode("utf-8")
    overlap = len(word_bytes) - 1
    count = 0
    chunk_size = 100 * 1024 * 1024 # 100MB chunk

    with open(filename, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            count += chunk.count(word_bytes)
            if len(chunk) == chunk_size:
                f.seek(-overlap, 1)
            chunk = f.read(chunk_size)
    end = time.perf_counter()
    return count, end - start


# Approach 2: Read Entire File (Earlier - f.read() once, No mmap)
def search_entire_read(filename, target_word):
    start = time.perf_counter()
    word_bytes = target_word.encode("utf-8")
    try:
        with open(filename, "rb") as f:
            data = f.read() # Allocates 1GB contiguous buffer
        count = data.count(word_bytes)
    except Exception as e:
        # Catch Windows OSError [Errno 22] or MemoryError to prevent crash
        print(f"Error in process {os.getpid()} reading {filename}: {e}")
        count = -1
    end = time.perf_counter()
    return count, end - start


# Approach 3: Memory-Mapped Loop (mmap.find() zero-copy)
def search_mmap_loop(filename, target_word):
    start = time.perf_counter()
    word_bytes = target_word.encode("utf-8")
    count = 0
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            pos = 0
            limit = len(mm)
            while True:
                pos = mm.find(word_bytes, pos, limit)
                if pos == -1:
                    break
                count += 1
                pos += 1
    end = time.perf_counter()
    return count, end - start


# ------------------------- BENCHMARK RUNNERS -------------------------

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
    

    run_benchmark("APPROACH 1: CHUNKED READS (100MB chunks)", search_chunked)
    run_benchmark("APPROACH 2: READ ENTIRE FILE (f.read once)", search_entire_read)
    run_benchmark("APPROACH 3: MMAP ZERO-COPY LOOP", search_mmap_loop)
