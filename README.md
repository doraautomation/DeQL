# DeQL
Reinventing CI/CD for Collaborative Science: A Blockchain-Integrated Decentralized Middleware for Scalable and Fault-Tolerant Workflows

# Features:

- Parallel Commit Manager
- Task Level Sharding
- Code Analysis Smart Contract
- Blockchain-style validation using a two-phase qourum commit(2PQC) simulated with python code
- The system uses MPI to distribute tasks across processes, and simulate internal node consensus (like in a sub-cluster or committee).

## Development Setup
DeQL should be run using python.
First install **[python]( https://www.python.org/downloads/)** 

DeQL is integrated with MPI
Then install **[mpi4py](https://github.com/mpi4py/mpi4py/)**

To clone the code to your target directory
```bash
git clone https://github.com/doraautomation/DeQL
cd DeQL
```
Install all required package.
```bash
pip install -r requirements.txt
```
Run the Project Locally

After installing the dependencies, you can run the project.

Run on HPC with SLURM

If you're working in an HPC environment, you can use the provided SLURM script to run your job.

Submit the Job
```bash
sbatch run_job.slurm
```
