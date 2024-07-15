from mpi4py import MPI
import networkx as nx
import subprocess
import numpy as np
import test_input
import time
import hashlib

dependency_graph = nx.DiGraph()
fully_independent_transactions = []
Track = []
processor_count = 1

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
trans_data = ""

def generate_hash(data):
    hash_object = hashlib.sha256()
    hash_object.update(str(data).encode('utf-8'))  
    return hash_object.hexdigest()

def Generatehashes(data):
    with open('hashed_data.txt', 'w') as file: 
        for item in data:
            hash_value = generate_hash(item)
            file.write(f"{item},{hash_value}\n")
    
def process_data(data):
    num_parts = processor_count 
    sub_arrays = np.array_split(data, num_parts)
    for i, sub_array in enumerate(sub_arrays, start=1):
        file_name = f"{i}.txt"
        with open(file_name, "w") as file:
            trans_data = ",".join(map(str, sub_array))
            file.write(trans_data)
            
def call_consensus(data):
    block_size = len(data)
    block_start_time = time.time()
    subprocess.run(["mpiexec", "-host", "au-prd-cnode001,au-prd-cnode002,au-prd-cnode003,au-prd-cnode004,au-prd-cnode005", "-n", str(processor_count), "python", "2PQC_sharding.py", trans_data]) #HPC nodes
    block_end_time = time.time()
    total_block_execution_time = block_end_time - block_start_time
    block_result = f"Total Block execution time: {total_block_execution_time} seconds"
    print(block_size, block_result)
    
def independent_transactions():
    for transaction, info in test_input.transactions.items():
        if not info["Dependency"]:
            fully_independent_transactions.append(transaction)

    Generatehashes(fully_independent_transactions)
    process_data(fully_independent_transactions)
    call_consensus(fully_independent_transactions)

def Dependent_transactions():
    dependent_transactions = []
    for transaction, info in test_input.transactions.items():
        if info["Dependency"]:
            dependent_transactions.append(transaction)

    while dependent_transactions:
        current_group = []
        for transaction in dependent_transactions:
            if set(test_input.transactions[transaction]["Dependency"]).issubset(fully_independent_transactions):
                current_group.append(transaction)

        Generatehashes(current_group)
        process_data(current_group)
        Track.append(current_group)
        fully_independent_transactions.extend(current_group)
        call_consensus(current_group)
        dependent_transactions = [t for t in dependent_transactions if t not in current_group]

if __name__ == '__main__':
    total_start_time = time.time()
    independent_transactions()
    Dependent_transactions()
    total_end_time = time.time()

    total_execution_time = total_end_time - total_start_time
    num_transactions_processed = len(fully_independent_transactions)
    throughput = num_transactions_processed / total_execution_time
    print(f"Throughput: {throughput} transactions per second")
    print(f"Total execution time: {total_execution_time} seconds")
