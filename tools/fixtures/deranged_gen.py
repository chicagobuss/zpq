import csv
import os
import random
import string
import datetime

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_deranged_csv(filename, num_rows=10000):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        # Header
        writer.writerow([
            "sparse_int32",      # 99% nulls
            "bloat_string",      # Very large strings
            "alignment_fixed",   # 7-byte fixed strings (tests alignment)
            "extreme_timestamp", # INT96 candidate
            "mixed_ints",        # Large range to force bit-width changes
            "repetitive_str"     # High cardinality but repetitive (tests dictionary)
        ])
        
        for i in range(num_rows):
            # 1. Sparse INT32
            sparse_int32 = i if random.random() < 0.01 else ""
            
            # 2. Bloat String (1 in 100 is very large)
            if random.random() < 0.01:
                bloat_string = random_string(70000) # > 64KB
            else:
                bloat_string = random_string(10)
                
            # 3. Alignment Fixed (7 bytes)
            alignment_fixed = random_string(7)
            
            # 4. Extreme Timestamp
            dt = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=random.randint(0, 50000))
            extreme_timestamp = dt.isoformat()
            
            # 5. Mixed Ints
            mixed_ints = random.randint(0, 2**31 - 1)
            
            # 6. Repetitive String
            repetitive_str = f"category_{i % 10}"
            
            writer.writerow([
                sparse_int32,
                bloat_string,
                alignment_fixed,
                extreme_timestamp,
                mixed_ints,
                repetitive_str
            ])

if __name__ == "__main__":
    generate_deranged_csv("data/deranged.csv")
    print("Generated data/deranged.csv")

