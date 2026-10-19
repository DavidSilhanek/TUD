import numpy as np
import time
import argparse

argument_parser = argparse.ArgumentParser(description="Random Task Simulation")
argument_parser.add_argument("--size", type=int, default=512, help="Size of the matrices for multiplication")
argument_parser.add_argument("--wait", type=float, default=0.5, help="Wait time between task runs in seconds")
argument_parser.add_argument("--n", type=int, default=100, help="Number of times to run the task")

def main(args) -> None:
    size = args.size
    n = args.n

    # while True:
    for _ in range(n):  # Run the task n times for demonstration
        matrix_a = np.random.rand(size, size)
        matrix_b = np.random.rand(size, size)
        result = matrix_a @ matrix_b
        print(f"Matrix multiplication complete; checksum: {result.sum():.4f}")
        time.sleep(args.wait)  # Wait for the specified time before the next run

if __name__ == "__main__":
    args = argument_parser.parse_args()
    main(args)