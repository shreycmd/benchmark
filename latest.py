from decimal import HAVE_THREADS
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
import os
import mmap
import queue
import threading

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


# ---------------- PROCESS WORKER (Chunked reads, No mmap) ----------------
def search_in_file(filename, target_word):
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

    return {
        "file": filename,
        "word": target_word,
        "count": count,
        "worker_time": end - start,
        "pid": os.getpid()
    }


# ---------------- THREAD HELPERS ----------------
def search_region_in_data(data, word_bytes, start_byte, end_byte, overlap):
    return data.count(word_bytes, start_byte, end_byte + overlap)


def search_region_in_mmap(mm, word_bytes, start_byte, end_byte, overlap):
    # Slice the mmap (250MB copy) and use native C count speed
    chunk = mm[start_byte : end_byte + overlap]
    return chunk.count(word_bytes)


# ---------------- THREADING BENCHMARK ----------------
def parallel_search(filename, word):
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
            executor.submit(
                search_region_in_data,
                data,
                word_bytes,
                start_byte,
                end_byte,
                overlap
            )
            for start_byte, end_byte in ranges
        ]
        total_count = sum(f.result() for f in futures)
        
    end = time.perf_counter()
    
    return {
        "file": filename,
        "word": word,
        "count": total_count,
        "worker_time": end - start,
        "pid": os.getpid()
    }


def benchmark_threads():
    print("\n========== THREAD BENCHMARK ==========")
    overall_start = time.perf_counter()
    results = []
    for filename, word in zip(files, words):
        results.append(parallel_search(filename, word))
    overall_end = time.perf_counter()
    print_results(results, overall_end - overall_start)


# ---------------- MULTIPROCESSING BENCHMARK ----------------
def benchmark_processes():
    print("\n========== PROCESS BENCHMARK ==========")
    start_total = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        results = list(executor.map(search_in_file, files, words))
        end_work = time.perf_counter()
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


# ---------------- HYBRID WORKER ----------------
def hybrid_worker(filename, word):
    start = time.perf_counter()
    with open(filename, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            word_bytes = word.encode("utf-8")
            overlap = len(word_bytes) - 1
            file_size = len(mm)
            part = file_size // 4
            
            ranges = [
                (0, part),
                (part, part * 2),
                (part * 2, part * 3),
                (part * 3, file_size)
            ]

            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [
                    executor.submit(
                        search_region_in_mmap,
                        mm,
                        word_bytes,
                        start_byte,
                        end_byte,
                        overlap
                    )
                    for start_byte, end_byte in ranges
                ]
                count = sum(f.result() for f in futures)
    end = time.perf_counter()
    return {
        "file": filename, "word": word, "count": count,
        "worker_time": end - start, "pid": os.getpid()
    }


# ---------------- HYBRID BENCHMARK ----------------
def benchmark_hybrid():
    print("\n========== HYBRID BENCHMARK ==========")
    start_total = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        results = list(executor.map(hybrid_worker, files, words))
        end_work = time.perf_counter()
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


# ---------------- PIPELINED HYBRID HELPERS & WORKER ----------------
def reader_thread_func(filename, word_bytes, overlap, chunk_size, chunk_queue):
    with open(filename, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            chunk_queue.put(chunk)
            if len(chunk) == chunk_size:
                f.seek(-overlap, 1)
            chunk = f.read(chunk_size)
    chunk_queue.put(None)


def pipelined_worker(filename, word):
    start = time.perf_counter()
    word_bytes = word.encode("utf-8")
    overlap = len(word_bytes) - 1
    chunk_size = 100 * 1024 * 1024 # 100MB chunk
    chunk_queue = queue.Queue(maxsize=3)
    
    reader_thread = threading.Thread(
        target=reader_thread_func,
        args=(filename, word_bytes, overlap, chunk_size, chunk_queue)
    )
    reader_thread.start()
    
    count = 0
    while True:
        chunk = chunk_queue.get()
        if chunk is None:
            break
        count += chunk.count(word_bytes)
        chunk_queue.task_done()
        
    reader_thread.join()
    end = time.perf_counter()
    return {
        "file": filename, "word": word, "count": count,
        "worker_time": end - start, "pid": os.getpid()
    }


# ---------------- PIPELINED HYBRID BENCHMARK ----------------
def benchmark_pipelined_hybrid():
    print("\n========== PIPELINED HYBRID BENCHMARK ==========")
    start_total = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        results = list(executor.map(pipelined_worker, files, words))
        end_work = time.perf_counter()
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


# ---------------- COMMON OUTPUT ----------------
def print_results(results, total_time):
    total_count = 0
    for result in results:
        total_count += result["count"]
        print(f"File        : {result['file']}\nWord        : {result['word']}\nOccurrences : {result['count']}\nWorker Time : {result['worker_time']:.4f} sec\nProcess ID  : {result['pid']}\n")
    print(f"Total Count          : {total_count}")
    print(f"Total Execution Time : {total_time:.4f} sec")


if __name__ == "__main__":
    import cProfile
    import pstats

    profiler = cProfile.Profile()
    profiler.enable()

    benchmark_processes()
    benchmark_threads()
    benchmark_hybrid()
    benchmark_pipelined_hybrid()

    profiler.disable()
    print("\n========== PROFILING REPORT (Top 30 Cumulative Time) ==========")
    stats = pstats.Stats(profiler).sort_stats('cumulative')
    stats.print_stats(30)