from tabnanny import verbose

from typing import Callable
import threading
import copy
import psutil
import platform
import time
from datetime import datetime
import pendulum
import subprocess
import argparse
# allowed time intervals
# allowed_times = {
#     "work_days": (pendulum.time(14, 0), pendulum.time(6, 0)),  # 9 PM to 6 AM - night time
#     "weekends": (pendulum.time(0, 0), pendulum.time(23, 59, 59)),  # All day Saturday and Sunday
# }

parser = argparse.ArgumentParser(description="Scheduler Simulation")
parser.add_argument("--wait", type=int, default=5, help="Wait time between scheduler s runs in seconds")
parser.add_argument("--allowed_workload", type=int, default=1, help="Maximum allowed computational cost for tasks")
parser.add_argument("--log_interval", type=int, default=5, help="Interval for logging machine utilization in seconds")
parser.add_argument("--log_file", type=str, default="utilization_log.csv", help="Path to the log file for machine utilization metrics")

def is_time_allowed(
    current_datetime_time: pendulum.DateTime,
    assumed_task_duration: pendulum.Duration = pendulum.duration(hours=1),
    allowed_times: dict = None,
) -> bool:
    """Check whether the task can fit within the allowed time interval."""

    if allowed_times is None:
        raise ValueError("allowed_times must be provided.")

    # print(f"Current day: {current_datetime_time.day_of_week}.")
    # print(f"Current time: {current_datetime_time.time()}.")
    # print(f"Assumed task duration: {assumed_task_duration}.")

    # Select the appropriate time interval.
    if current_datetime_time.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY):
        start_time, end_time = allowed_times["weekends"]
    else:
        start_time, end_time = allowed_times["work_days"]

    # Combine the current date with the allowed interval boundaries.
    start_datetime = current_datetime_time.start_of("day").set(
        hour=start_time.hour,
        minute=start_time.minute,
        second=start_time.second,
    )

    end_datetime = current_datetime_time.start_of("day").set(
        hour=end_time.hour,
        minute=end_time.minute,
        second=end_time.second,
    )

    # Handle intervals that cross midnight.
    if end_datetime <= start_datetime:
        end_datetime = end_datetime.add(days=1)

    # Ensure the task can finish before the allowed interval ends.
    latest_start_time = end_datetime - assumed_task_duration

    return start_datetime <= current_datetime_time <= latest_start_time


# class Scheduler:
#     def __init__(self, tasks: list, wait_time: int = 5, allowed_workload: int = 1):
#         """Initialize the scheduler."""
#         self.tasks = tasks
#         self.wait_time = wait_time
#         self.lock = threading.Lock()
#         self.allowed_workload = allowed_workload  # Maximum allowed computational cost for tasks
#         self.current_workload = 0

#     def add_task(self, task):
#         """Add a task while the scheduler is running."""
#         with self.lock:
#             self.tasks.append(task)

#         print(f"Task {task.name} added to scheduler.")

#     def run(self):
#         """Run the scheduler continuously."""
#         while True:
#             current_time = pendulum.now()

#             # Create a snapshot of the task list safely.
#             with self.lock:
#                 tasks_snapshot = list(self.tasks)

#             for task in tasks_snapshot:
#                 if is_time_allowed(current_time, task.assumed_task_duration, task.allowed_times):
#                     utilization = get_machine_utilization(verbose=False)

#                     if task.allowed_to_run(utilization):
#                         if self.current_workload + task.workload > self.allowed_workload:
#                             print(
#                                 f"Task {task.name} skipped due to exceeding allowed workload. "
#                             )
                            
#                             continue # Skip this task if it exceeds the allowed workload
#                         else:
#                             self.current_workload += task.workload
#                             task.run()
#                             self.current_workload -= task.workload
#                     else:
#                         print(
#                             f"Task {task.name} not allowed to run "
#                             "due to high utilization."
#                         )
#                 else:
#                     print(
#                         f"Task {task.name} not allowed to run at this time: "
#                         f"{current_time.time()}"
#                     )

#             time.sleep(self.wait_time)



class Scheduler:
    def __init__(self, tasks: list, wait_time: int = 5, allowed_workload: int = 1):
        """Initialize the scheduler."""
        self.tasks = tasks
        self.wait_time = wait_time
        self.lock = threading.Lock()
        self.allowed_workload = allowed_workload
        self.current_workload = 0

        # Track currently running tasks and their threads.
        self.running_tasks = {}

    def add_task(self, task):
        """Add a task while the scheduler is running."""
        with self.lock:
            self.tasks.append(task)

        print(f"Task {task.name} added to scheduler.")

    def _run_task(self, task):
        """Execute a task in a background thread and track its completion."""
        try:
            print(f"Task {task.name} started.")
            task.run()
        except Exception as exc:
            print(f"Task {task.name} failed: {exc}")
        finally:
            with self.lock:
                print(f"Task {task.name} completed. Cleaning up.")
                print(f"Current workload before cleanup: {self.current_workload}")
                self.current_workload -= task.workload
                print(f"Current workload after cleanup: {self.current_workload}")
                self.running_tasks.pop(task.name, None)
                self.tasks.remove(task)  # Remove the task from the scheduler after completion

            print(f"Task {task.name} finished.")

    def is_task_running(self, task_name: str) -> bool:
        """Return whether a task is currently running."""
        with self.lock:
            return task_name in self.running_tasks

    def run(self):
        """Run the scheduler continuously."""
        while True:
            current_time = pendulum.now()
            print(f"\nScheduler running at {current_time}. Current workload: {self.current_workload}/{self.allowed_workload}")
            print(f"Remaining tasks: {[task.name for task in self.tasks]}")
            print(f"Running tasks: {list(self.running_tasks.keys())}")
            

            # Take a snapshot of the task list safely.
            with self.lock:
                tasks_snapshot = list(self.tasks)

            for task in tasks_snapshot:
                # Skip tasks that are already running.
                with self.lock:
                    if task.name in self.running_tasks:
                        continue

                if not is_time_allowed(
                    current_time,
                    task.assumed_task_duration,
                    task.allowed_times,
                ):
                    print(
                        f"Task {task.name} not allowed to run at this time: "
                        f"{current_time.time()}"
                    )
                    continue

                utilization = get_machine_utilization(verbose=False)

                if not task.allowed_to_run(utilization):
                    print(
                        f"Task {task.name} not allowed to run "
                        "due to high utilization."
                    )
                    continue

                # Reserve workload and register the thread atomically.
                with self.lock:
                    if task.name in self.running_tasks:
                        continue

                    if self.current_workload + task.workload > self.allowed_workload:
                        print(
                            f"Task {task.name} skipped due to exceeding "
                            "allowed workload."
                        )
                        continue

                    self.current_workload += task.workload

                    thread = threading.Thread(
                        target=self._run_task,
                        args=(task,),
                        daemon=True,
                        name=f"Task-{task.name}",
                    )

                    self.running_tasks[task.name] = thread
                    thread.start()

            time.sleep(self.wait_time)

class Task:
    def __init__(self, name, command: list[str] | Callable, assumed_task_duration=pendulum.duration(hours=1), allowed_times: dict = {},
                    utilization_limits: dict = {
                        "cpu_percent": 70,  # CPU utilization threshold
                        "memory_percent": 80,  # Memory utilization threshold
                }, workload: int = 1):
        self.name = name
        self.command = command
        self.assumed_task_duration = assumed_task_duration
        self.utilization_limits = utilization_limits
        self.allowed_times = allowed_times
        self.workload = workload  # 

    def allowed_to_run(self, utilization: dict) -> bool:
        """Check if the task is allowed to run based on the current time."""

        # check if the current time is within allowed intervals
        time = pendulum.now()
        if not is_time_allowed(time, self.assumed_task_duration, self.allowed_times ):
            return False
        
        # check if the current utilization is below the defined limits
        for metric, limit in self.utilization_limits.items():
            if utilization.get(metric, 0) > limit:
                return False
        return True

    def run(self):
        """Run the scheduled task."""
        try:
            print(f"Running task: {self.name} at {datetime.now()}")
            if callable(self.command):
                self.command()
            else:
               subprocess.run(self.command, shell=True)
            print(f"Task {self.name} completed at {datetime.now()}")
        except Exception as e:
            print(f"Error running task {self.name}: {e}")


def get_machine_utilization(verbose: bool = True) -> dict:
    """Collect machine utilization metrics."""

    # CPU utilization
    cpu_percent = psutil.cpu_percent(interval=3)
    # cpu_percent = psutil.cpu_percent(interval=3, percpu=True)
    cpu_count = psutil.cpu_count(logical=True)

    # Memory utilization
    memory = psutil.virtual_memory() 
    for key in memory._fields:
        if key in ['total', 'available', 'used']:
            memory = memory._replace(**{key: getattr(memory, key) / (1024**3)})  # Convert bytes to GB

    # Disk utilization (root/system drive)
    disk_path = "C:\\" if platform.system() == "Windows" else "/"
    disk = psutil.disk_usage(disk_path)     
    for key in disk._fields:
        if key in ['total', 'used', 'free']:
            disk = disk._replace(**{key: getattr(disk, key) / (1024**3)})  # Convert bytes to GB

    # Network utilization
    net_io = psutil.net_io_counters()
    for key in net_io._fields:
        if key in ['bytes_sent', 'bytes_recv']:
            net_io = net_io._replace(**{key: getattr(net_io, key) / (1024**2)})  # Convert bytes to MB

    # System uptime
    uptime_seconds = time.time() - psutil.boot_time()
    uptime_hours = uptime_seconds / 3600


    # Print utilization report
    if verbose:
        print("\n" + "=" * 60)
        print(f"Machine Utilization Report - {datetime.now()}")
        print("=" * 60)

        print(f"Hostname             : {platform.node()}")
        print(f"Operating System     : {platform.system()} {platform.release()}")
        print(f"CPU Cores (Logical)  : {cpu_count}")
        print(f"CPU Utilization      : {cpu_percent:.2f}%")

        print(f"\nTotal Memory         : {memory.total :.2f} GB")
        print(f"Used Memory          : {memory.used :.2f} GB")
        print(f"Available Memory     : {memory.available :.2f} GB")
        print(f"Memory Utilization   : {memory.percent:.2f}%")

        print(f"\nTotal Disk Space     : {disk.total :.2f} GB")
        print(f"Used Disk Space      : {disk.used :.2f} GB")
        print(f"Free Disk Space      : {disk.free :.2f} GB")
        print(f"Disk Utilization     : {disk.percent:.2f}%")

        print(f"\nNetwork Bytes Sent   : {net_io.bytes_sent :.2f} MB")
        print(f"Network Bytes Recv   : {net_io.bytes_recv :.2f} MB")

        print(f"\nSystem Uptime        : {uptime_hours:.2f} hours")
        print("=" * 60)
    
    dict_utilization = {
        "timestamp": datetime.now(),
        "machine_name": platform.node(),
        "machine_os": platform.system() + " " + platform.release(),
        "cpu_percent": cpu_percent,
        "cpu_count": cpu_count,
        "memory_total_GB": memory.total,
        "memory_used_GB": memory.used,
        "memory_available_GB": memory.available,
        "memory_percent": memory.percent,
        "disk_total_GB": disk.total,
        "disk_used_GB": disk.used,
        "disk_free_GB": disk.free,
        "disk_percent": disk.percent,
        "net_bytes_sent_MB": net_io.bytes_sent,
        "net_bytes_recv_MB": net_io.bytes_recv,
        "uptime_hours": uptime_hours
    }
    
    return dict_utilization

def log_utilization_to_file(utilization: dict, filename: str = "utilization_log.csv") -> None:
    """Log the machine utilization metrics to a csv file."""
    with open(filename, "a") as f:
        if f.tell() == 0:  # If file is empty, write header
            f.write(str.join(",", list(utilization.keys()) + ["\n"]))
        f.write(str.join(",", [str(v) for v in utilization.values()] + ["\n"]))


if __name__ == "__main__":
    allowed_times_logger = {
        "work_days": (pendulum.time(0, 0), pendulum.time(23, 59, 59)),  # 9 PM to 6 AM - night time
        "weekends": (pendulum.time(0, 0), pendulum.time(23, 59, 59)),  # All day Saturday and Sunday
    }
    allowed_times_computator = {
        "work_days": (pendulum.time(0, 0), pendulum.time(23, 59, 59)),  # 9 PM to 6 AM - night time
        "weekends": (pendulum.time(0, 0), pendulum.time(23, 59, 59)),  # All day Saturday and Sunday
    }
    
    def repeating_logger(log_interval_s: int = 5,):
        while True:
            utilization = get_machine_utilization(verbose=False)
            log_utilization_to_file(utilization)
            time.sleep(log_interval_s)  # Log every log_interval_s seconds


    task_logger = Task(name="Logger", command=lambda: repeating_logger(log_interval_s=5),
                       assumed_task_duration=pendulum.duration(seconds=1),  # 1 second
                       utilization_limits={}, allowed_times=allowed_times_logger, workload=0)
    random_task0 = Task(name="RandomTask0", command="python randomtask.py --n 50000 >> output.log 2>&1",
                       assumed_task_duration=pendulum.duration(minutes=1),  # 10 seconds
                       utilization_limits={"cpu_percent": 70, "memory_percent": 80}, allowed_times=allowed_times_computator, workload=0.4)
    random_task1 = Task(name="RandomTask1", command="python randomtask.py --n 10 >> output.log 2>&1",
                       assumed_task_duration=pendulum.duration(minutes=1),  # 10 seconds
                       utilization_limits={"cpu_percent": 70, "memory_percent": 80}, allowed_times=allowed_times_computator, workload=0.4)
    random_task2 = Task(name="RandomTask2", command="python randomtask.py --n 200 >> output.log 2>&1",
                       assumed_task_duration=pendulum.duration(minutes=2),  # 10 seconds
                       utilization_limits={"cpu_percent": 70, "memory_percent": 80}, allowed_times=allowed_times_computator, workload=0.4)
    
    
    scheduler = Scheduler(tasks=[task_logger, random_task0, random_task1, random_task2])  # Add the same task twice for demonstration
    scheduler.run()

    scheduler = Scheduler(tasks=[], wait_time=5)

    # Start the scheduler in the background.
    scheduler_thread = threading.Thread(
        target=scheduler.run,
        daemon=True
    )
    scheduler_thread.start()

    # The scheduler is now running.
    time.sleep(10)

    # Add a new task while the scheduler is running.
    new_task = random_task0
    scheduler.add_task(new_task)

    time.sleep(2)
    # Add another task later.
    another_task = random_task1
    scheduler.add_task(another_task)

    while True:
        utilization = get_machine_utilization()
        scheduler.run()



    try:
        pids = []
        while True:
            utilization = get_machine_utilization()
            log_utilization_to_file(utilization)
            print("pids", pids)
            print(f"PROCESS STARTED: {len(pids)} process(es) running.")
            print("\nRefreshing in 5 seconds... (Press Ctrl+C to stop)")
            if utilization["cpu_percent"] > 70:
                while pids:
                    pid = pids.pop()
                    print(f"Terminating process with PID: {pid.pid}")
                    pid.terminate()
            if utilization["cpu_percent"] < 70:
                pids.append(subprocess.Popen("python randomtask.py >> output.log 2>&1", shell=True))
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nMachine utilization monitoring stopped.")