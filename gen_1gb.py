import os
import random
import time

NUM_FILES = 4
FILE_SIZE_GB = 1
TARGET_SIZE = FILE_SIZE_GB * 1024 * 1024 * 1024 # 1,073,741,824 bytes
OUTPUT_DIR = "test"

# Make output dir if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

WORDS = [
    "India", "Japan", "Canada", "Brazil", "Germany",
    "Australia", "France", "Italy", "China", "Mexico",
    "Maharashtra", "Gujarat", "Rajasthan", "Punjab", "Kerala",
    "Karnataka", "TamilNadu", "Assam", "Odisha", "Bihar",
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Jaipur", "Lucknow", "Ahmedabad",
    "Ganga", "Yamuna", "Narmada", "Godavari", "Krishna",
    "Amazon", "Nile", "Danube", "Mississippi", "Thames",
    "Everest", "K2", "Kangchenjunga", "Makalu", "Kilimanjaro",
    "Fuji", "Alps", "Andes", "Rockies", "Himalayas"
]

TARGET_WORDS = ["banana", "cloud", "apple", "Rohan"]

for i in range(NUM_FILES):
    filename = os.path.join(OUTPUT_DIR, f"file_{i+1}.txt")
    target_word = TARGET_WORDS[i]
    print(f"Generating {filename} ({FILE_SIZE_GB} GB) with target word '{target_word}'...")
    
    start_time = time.perf_counter()
    
    # Frequencies for injection:
    # banana: 0 occurrences
    # cloud: 1 in 50 words
    # apple: 1 in 30 words
    # Rohan: 1 in 40 words
    freq = {
        "banana": 999999999, # practically 0
        "cloud": 50,
        "apple": 30,
        "Rohan": 40
    }[target_word]
    
    # Build a 10MB template block of text
    block_size = 10 * 1024 * 1024 # 10MB
    current_size = 0
    words_temp = []
    
    while current_size < block_size:
        line_words = []
        for _ in range(20):
            if random.randint(1, freq) == 1:
                line_words.append(target_word)
            else:
                line_words.append(random.choice(WORDS))
        line_text = " ".join(line_words) + "\n"
        words_temp.append(line_text)
        current_size += len(line_text)
        
    block_text = "".join(words_temp).encode("utf-8")
    block_len = len(block_text)
    
    # Write the block repeatedly to reach 1GB
    written = 0
    with open(filename, "wb") as f:
        while written < TARGET_SIZE:
            f.write(block_text)
            written += block_len
            
    actual_size = os.path.getsize(filename) / (1024 * 1024 * 1024)
    duration = time.perf_counter() - start_time
    print(f"Finished {filename} in {duration:.2f} seconds. Size: {actual_size:.2f} GB\n")

print("All 4 files generated successfully in 'test/' directory.")
