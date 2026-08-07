import asyncio
import time
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

files = [
    "test/file_1.txt",
    "test/file_2.txt",
    "test/file_3.txt",
    "test/file_4.txt"
]

words = [
    "banana",
    "cloud",
    "apple",
    "Rohan"
]

# ----------------- NON-PIPELINED HYBRID WORKER -----------------

def search_region_in_data(data, word_bytes, start_byte, end_byte, overlap):
    return data.count(word_bytes, start_byte, end_byte + overlap)


def hybrid_worker(filename, word):
    """Standard Hybrid worker: Reads whole file, processes with threads."""
    start = time.perf_counter()
    with open(filename, "rb") as f:
        data = f.read()
        
    word_bytes = word.encode("utf-8")
    overlap = len(word_bytes) - 1
    file_size = len(data)
    part = file_size // 4
    
    ranges = [
        (0, part),
        (part, part * 2),
        (part * 2, part * 3),
        (part * 3, file_size)
    ]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(search_region_in_data, data, word_bytes, s, e, overlap)
            for s, e in ranges
        ]
        count = sum(f.result() for f in futures)
        
    end = time.perf_counter()
    return {
        "file": filename,
        "word": word,
        "count": count,
        "worker_time": end - start,
        "pid": os.getpid()
    }


# ----------------- PIPELINED WORKER -----------------

def reader_thread_func(filename, word_bytes, overlap, chunk_size, chunk_queue):
    """Background Reader Thread: Sequentially reads chunks and feeds them to the queue."""
    with open(filename, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            chunk_queue.put(chunk)
            if len(chunk) == chunk_size:
                f.seek(-overlap, 1)
            chunk = f.read(chunk_size)
    # Put sentinel to signal end of file
    chunk_queue.put(None)


def pipelined_worker(filename, word):
    """
    Pipelined Worker:
    Uses a background reader thread for I/O and main thread for computation.
    Disk reads and counting calculations overlap concurrently.
    """
    start = time.perf_counter()
    
    word_bytes = word.encode("utf-8")
    overlap = len(word_bytes) - 1
    chunk_size = 100 * 1024 * 1024 # 100MB chunk
    
    # Bound the queue size to 3 to keep memory consumption low
    chunk_queue = queue.Queue(maxsize=3)
    
    # Spawn background I/O reader thread
    reader_thread = threading.Thread(
        target=reader_thread_func,
        args=(filename, word_bytes, overlap, chunk_size, chunk_queue)
    )
    reader_thread.start()
    
    # Main thread processes chunks concurrently as they are read
    count = 0
    while True:
        chunk = chunk_queue.get()
        if chunk is None: # Sentinel reached
            break
        count += chunk.count(word_bytes)
        chunk_queue.task_done()
        
    reader_thread.join()
    end = time.perf_counter()
    
    return {
        "file": filename,
        "word": word,
        "count": count,
        "worker_time": end - start,
        "pid": os.getpid()
    }


# ----------------- BENCHMARKS -----------------

def run_sync_hybrid():
    print("\n========== SYNC HYBRID BENCHMARK ==========")
    start_total = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        results = list(executor.map(hybrid_worker, files, words))
        end_work = time.perf_counter()
        
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


async def run_async_hybrid():
    print("\n========== ASYNC HYBRID BENCHMARK ==========")
    start_total = time.perf_counter()
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=4) as process_executor:
        list(process_executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        tasks = [
            loop.run_in_executor(process_executor, hybrid_worker, filename, word)
            for filename, word in zip(files, words)
        ]
        results = await asyncio.gather(*tasks)
        end_work = time.perf_counter()
        
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


async def run_pipelined_async_hybrid():
    print("\n========== PIPELINED ASYNC HYBRID BENCHMARK ==========")
    start_total = time.perf_counter()
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=4) as process_executor:
        list(process_executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        tasks = [
            loop.run_in_executor(process_executor, pipelined_worker, filename, word)
            for filename, word in zip(files, words)
        ]
        results = await asyncio.gather(*tasks)
        end_work = time.perf_counter()
        
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


# ----------------- COMMON OUTPUT -----------------

def print_results(results, total_time):
    total_count = 0
    for result in results:
        total_count += result["count"]
        print(f"File        : {result['file']}\nWord        : {result['word']}\nOccurrences : {result['count']}\nWorker Time : {result['worker_time']:.4f} sec\nProcess ID  : {result['pid']}\n")
    print(f"Total Count          : {total_count}")
    print(f"Total Execution Time : {total_time:.4f} sec")


if __name__ == "__main__":
    run_sync_hybrid()
    asyncio.run(run_async_hybrid())
    asyncio.run(run_pipelined_async_hybrid())
