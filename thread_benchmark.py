from concurrent.futures import ThreadPoolExecutor
import os
import time

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


# ------------------------- HELPERS -------------------------

# Helper for Approach 1 (C-level search on shared bytes in RAM)
def search_region_in_data(data, word_bytes, start_byte, end_byte, overlap):
    return data.count(word_bytes, start_byte, end_byte + overlap)


# Helper for Approach 2 (Independent file open, seek, and read on disk)
def search_region_on_disk(filename, word_bytes, start_byte, end_byte):
    overlap = len(word_bytes) - 1
    with open(filename, "rb") as f:
        f.seek(start_byte)
        data = f.read(end_byte - start_byte + overlap)
        count = data.count(word_bytes)
    return count


# ------------------------- APPROACH 1 -------------------------
# Read once sequentially on main thread, then search in RAM
# --------------------------------------------------------------

def parallel_search_read_once(filename, word):
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
        return sum(f.result() for f in futures)


def benchmark_read_once():
    print("\n========== APPROACH 1: SEQUENTIAL READ ONCE ==========")
    start = time.perf_counter()
    results = []
    for filename, word in zip(FILES, WORDS):
        results.append(parallel_search_read_once(filename, word))
    end = time.perf_counter()
    print("Results       :", results)
    print("Execution Time:", end - start, "sec")


# ------------------------- APPROACH 2 -------------------------
# Concurrent file opens, seeks, and reads directly inside threads
# --------------------------------------------------------------

def parallel_search_concurrent_seek(filename, word):
    word_bytes = word.encode("utf-8")
    file_size = os.path.getsize(filename)
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
                search_region_on_disk,
                filename,
                word_bytes,
                start_byte,
                end_byte
            )
            for start_byte, end_byte in ranges
        ]
        return sum(f.result() for f in futures)


def benchmark_concurrent_seek():
    print("\n========== APPROACH 2: CONCURRENT OPEN-SEEK-READ ==========")
    start = time.perf_counter()
    results = []
    for filename, word in zip(FILES, WORDS):
        results.append(parallel_search_concurrent_seek(filename, word))
    end = time.perf_counter()
    print("Results       :", results)
    print("Execution Time:", end - start, "sec")


if __name__ == "__main__":
    # Warm up page caches first
    print("Warming up files...")
    for f in FILES:
        with open(f, "rb") as file:
            _ = file.read(1024)

    benchmark_read_once()
    benchmark_concurrent_seek()