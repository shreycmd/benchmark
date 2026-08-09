from concurrent.futures import ProcessPoolExecutor
import time
import os
import asyncio

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




def background_file_reader(filename, chunk_size, overlap, queue_obj, loop, num_workers):
    """
    Synchronous file reader loop executing in a dedicated background thread.
    Uses call_soon_threadsafe to push chunks into the asyncio.Queue.
    """
    with open(filename, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            # Safely append chunks to the asyncio queue running on the event loop thread
            loop.call_soon_threadsafe(queue_obj.put_nowait, chunk)
            if len(chunk) == chunk_size:
                f.seek(-overlap, 1)
            chunk = f.read(chunk_size)
            
    # Send sentinel shutdown markers to all async worker tasks
    for _ in range(num_workers):
        loop.call_soon_threadsafe(queue_obj.put_nowait, None)


async def async_counting_worker(queue_obj, word_bytes, results):
    """
    Async consumer task running on the event loop.
    Pulls chunks from the queue and offloads the CPU-bound count to a thread.
    """
    while True:
        chunk = await queue_obj.get()
        if chunk is None:
            queue_obj.task_done()
            break
        # Offload count calculation to thread pool to release GIL
        count = await asyncio.to_thread(chunk.count, word_bytes)
        results.append(count)
        queue_obj.task_done()


async def async_pipelined_search(filename, word):
    """
    Asynchronous orchestrator running inside the process worker.
    Spawns the background file reader and runs 4 concurrent counting workers.
    """
    start = time.perf_counter()
    word_bytes = word.encode("utf-8")
    overlap = len(word_bytes) - 1
    chunk_size = 50 * 1024 * 1024 
    
    queue_obj = asyncio.Queue(maxsize=30)
    loop = asyncio.get_running_loop()
    
    num_workers = 4
    results = []
    
    # Run the entire file reader inside a single background thread
    reader_task = asyncio.create_task(
        asyncio.to_thread(
            background_file_reader, filename, chunk_size, overlap, queue_obj, loop, num_workers
        )
    )
    
    # Spawn 4 async consumer tasks on the event loop
    worker_tasks = [
        asyncio.create_task(async_counting_worker(queue_obj, word_bytes, results))
        for _ in range(num_workers)
    ]
    
    # Await concurrent completion of reader and workers
    await asyncio.gather(reader_task, *worker_tasks)
    
    end = time.perf_counter()
    return sum(results), end - start


def async_pipelined_process_worker(filename, word):
    """Target function executed by ProcessPoolExecutor (runs a local event loop)."""
    total_count, duration = asyncio.run(async_pipelined_search(filename, word))
    return {
        "file": filename,
        "word": word,
        "count": total_count,
        "worker_time": duration,
        "pid": os.getpid()
    }


def benchmark_async_pipeline():
    
    start_total = time.perf_counter()
    with ProcessPoolExecutor(max_workers=4) as executor:
        list(executor.map(abs, [1, 2, 3, 4]))
        start_work = time.perf_counter()
        results = list(executor.map(async_pipelined_process_worker, files, words))
        end_work = time.perf_counter()
    end_total = time.perf_counter()
    print_results(results, end_total - start_total)
    print(f"  * Pure Processing Time (excluding startup/teardown): {end_work - start_work:.4f} sec")


def print_results(results, total_time):
    total_count = 0
    for result in results:
        total_count += result["count"]
        print(f"File        : {result['file']}\nWord        : {result['word']}\nOccurrences : {result['count']}\nWorker Time : {result['worker_time']:.4f} sec\nProcess ID  : {result['pid']}\n")
    print(f"Total Count          : {total_count}")
    print(f"Total Execution Time : {total_time:.4f} sec")


if __name__ == "__main__":
    # Warm up page caches if not then we have to wait for ssd to spin up
    for f in files:
        with open(f, "rb") as file:
            _ = file.read(1024)

    benchmark_async_pipeline()
