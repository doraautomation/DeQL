from mpi4py import MPI
import os
import datetime
from smart_contract import SimpleBank
import test
import threading
import time
import hashlib
import test_input
import subprocess

filename = "userinfo.txt"
output_path = "./output"
simple_bank = SimpleBank(filename)
_debug_ = True

def pre_prepare_phase(comm, rank, size, received_txn):
    comm.barrier()
    message_count = 0
    for dest in range(size):
        if dest != rank:
            message = f"Received Request"
            comm.send(message, dest=dest)

    for source in range(size):
        if source != rank:
            try:
                message = comm.recv(source=source, status=MPI.Status())
                message_count += 1
            except MPI.Exception as e:
                pass

    comm.barrier()

    if message_count >= (size - 1):
        return True
    else:
        return False

def prepare_phase(received_txn, comm, rank, size):
    comm.barrier()
    count = 0
    if process_txn(received_txn):
        for dest in range(size):
            if dest != rank:
                message = f"prepare"
                comm.send(message, dest=dest)

        for source in range(size):
            if source != rank:
                try:
                    message = comm.recv(source=source, status=MPI.Status())
                    count += 1
                except MPI.Exception as e:
                    pass
        comm.barrier()

        if count >= (size - 1):
            return True
        else:
            return False

def commit_phase(received_txn, comm, rank, size):
    comm.barrier()
    ready_commit = 0
    c_message = 0

    for r in range(0, size):
        if rank == r:
            log_entry = f"log{rank} at {datetime.datetime.now()}: {', '.join(map(str, received_txn))}\n"
            with open(os.path.join(output_path, f"log{rank}.txt"), "a+") as fp:
                fp.write(log_entry)
            ready_commit = 1

    if ready_commit == 1:
        for dest in range(size):
            if dest != rank:
                message = f"Ready Commit"
                comm.send(message, dest=dest)

        for source in range(size):
            if source != rank:
                try:
                    message = comm.recv(source=source, status=MPI.Status())
                    c_message += 1
                except MPI.Exception as e:
                    pass

        comm.barrier()

        if c_message >= (size - 1):
            return True
        else:
            return False

def pbft_consensus(comm, rank, size, received_txn):
    if rank == 0:
        if process_element(received_txn):
            comm.bcast("Pre-prepare", root=0)
        else:
            print("Leader decided not to proceed with Pre-prepare phase.")
            return
    else:
        comm.bcast(None, root=0)

def submit_txn(txn, comm, rank):
    txn = comm.bcast(txn, root=0)
    return txn

def process_chunk(thread_id, chunk):
    local_result = 0
    try:
        for element in chunk:
            username, password, code_file = test_input.get_transaction_info(element)
            db_name = f"db-{element}"
            # Perform your logic with the transaction data
            if simple_bank.execute_transaction(username, password,  db_name):
                local_result += 1
                #print(f"Thread-{thread_id}: Authentication successful for transaction {element}.")
            else:
                pass
                #print(f"Thread-{thread_id}: Authentication failed for transaction {element}.")
    except Exception as e:
        print(f"Thread-{thread_id}: Exception occurred - {e}")
    return local_result

def process_txn(current):
    num_threads = 5
    threads = []
    results = []

    # Ensure there are transactions to process
    if not current:
        return 0

    # Split transactions into chunks
    chunk_size = max(1, len(current) // num_threads)
    chunks = [current[i:i + chunk_size] for i in range(0, len(current), chunk_size)]

    for i in range(min(num_threads, len(chunks))):  # Ensure i is within the valid range
        thread = threading.Thread(target=lambda i=i: results.append(process_chunk(i, chunks[i])), name=f"Thread-{i}")
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Retrieve results from the threads
    total_successful_authentications = sum(results)
    if(total_successful_authentications== len(current)):
        return True
    else:
       return False
def generate_hash(data):
    return hashlib.sha256(str(data).encode('utf-8')).hexdigest()

def get_original_hash(file_path, element):
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line_element, line_hash = line.strip().split(',')
                if line_element == str(element):
                    return line_hash
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")
    return None

def process_element(received_txn):
   file_path = "hashed_data.txt"
   count = 0
   for element in received_txn:
     node_hash = generate_hash(element)
     original_hash = get_original_hash(file_path, element)
     if node_hash == original_hash:
        count += 1 
        username, password, code_file = test_input.get_transaction_info(element)
        CODEQL_PATH = r"./codeql"
        code_file = f"File{element}"
        code_path = f"{code_file}"
        db_name = f"db-{element}"
        db_path = f"./{db_name}"  # Adjust this path as necessary
        command = [
            CODEQL_PATH,
            "database",
            "create",
            db_path,  # This is the path where the database will be created
            "--language=python",
            "--source-root",  # This flag is needed before specifying the source code directory
            code_path
        ]

        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            stdout, stderr = process.communicate()

            if process.returncode == 0:
                print(f"CodeQL database {db_name} created successfully.")
                if simple_bank.execute_transaction(username, password,  db_name):
                 local_result += 1
            else:
                print(f"Error creating CodeQL database {db_name}:")
                print(stderr)
        except Exception as e:
            print("Error:", e)
            print(f"Failed to create CodeQL database {db_name}.")
            
   if count == len(received_txn):
     return True
         
    
if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    size = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()
    name = MPI.Get_processor_name()

    output_path = "./output"
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    txn = []
    file_path = "output.txt"
    if rank == 0:
        with open(file_path, "r") as file:
            txn = [int(x) for x in file.read().split(',') if x.strip()]

    received_txn = submit_txn(txn, comm, rank)
    pbft_consensus(comm, rank, size, received_txn)
    
    if pre_prepare_phase(comm, rank, size, received_txn):
        print(f"Rank {rank} done with pre_prepare_phase")
        
    comm.barrier()

    if prepare_phase(received_txn, comm, rank, size):
        print(f"Rank {rank} done with prepare_phase")
        
    comm.barrier()

    if commit_phase(received_txn, comm, rank, size):
        print(f"Rank {rank} done with commit_phase")
    comm.barrier()
