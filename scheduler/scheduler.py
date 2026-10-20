from typing import Callable
import threading
import copy
from colorama import init
import psutil
import platform
import time
from datetime import datetime
import pendulum
import subprocess
import argparse
import json
from filelock import FileLock
import pathlib
import sqlite3
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, Text, JSON, distinct
from sqlalchemy.orm import declarative_base, sessionmaker, Session 
import sqlalchemy
from warnings import filterwarnings, warn

from functools import wraps

parser = argparse.ArgumentParser(description="Scheduler Simulation")
# parser.add_argument("--wait", type=int, default=5, help="Wait time between scheduler s runs in seconds")
parser.add_argument("--wait", type=int, default=3, help="Wait time between scheduler s runs in seconds")
parser.add_argument("--allowed_workload", type=int, default=1, help="Maximum allowed computational cost for tasks")
parser.add_argument("--log_interval", type=int, default=10, help="Interval for logging machine utilization in seconds")
parser.add_argument("--log_file", type=str, default="utilization_log.csv", help="Path to the log file for machine utilization metrics")
parser.add_argument("--db_URL", type=str, default="sqlite:///queue.db", help="Database URL for the job queue")

JOB_QUEUE: pathlib.Path = pathlib.Path("queue.json")
SQLITE_DB: pathlib.Path = pathlib.Path("jobs.db")
# DATABASE_URL = "sqlite:///queue.db"
DATABASE_URL = parser.parse_args().db_URL  # Use the database URL from command-line arguments
DB_LOCK = pathlib.Path("queue.db.lock")

engine = create_engine( DATABASE_URL, connect_args={"check_same_thread": False}, )
SessionLocal = sessionmaker( bind=engine, expire_on_commit=False, )
Base = declarative_base()

PYTHON_TASK_REGISTRY = {}

# ------------------
# JOB SUBMITTER
# ------------------
def computed_duration(started_on, finished_on):
    duration = None
    if started_on is not None and finished_on is not None: 
        start = pendulum.instance(started_on) 
        finish = pendulum.instance(finished_on) 
        duration = (finish - start).total_seconds()
    return duration

class JOB(Base):
    
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    job_id = Column(String, index=True, unique=True)
    command = Column(String)
    timestamp = Column(DateTime, default=pendulum.now())
    state = Column(String, default="pending")  # pending, started, finished
    scheduled_on = Column(String, default=None)
    started_on = Column(DateTime, default=None)
    finished_on = Column(DateTime, default=None)
    mc_args = Column(JSON, default={})
    # 
    assumed_task_duration = Column(Integer, default=3600)  # Default assumed duration in seconds (1 hour)
    true_task_duration = Column(Float, default=None)  # Default assumed duration in seconds (1 hour)
    allowed_times = Column(JSON)
    utilization_limits = Column(JSON, default={"cpu_percent": 70, "memory_percent": 80})
    workload = Column(Float, default=1)  # Computational cost of the task
    # 
    claimed = Column(Boolean, default=False)  # Indicates if the job has been claimed by a scheduler
        
    
    @property
    def name(self):
        return self.job_id
    
    @property
    def assumed_duration(self):
        return pendulum.duration(seconds=self.assumed_task_duration) if self.assumed_task_duration is not None else None
    
    
    def allowed_to_run(self, utilization: dict, time: pendulum.DateTime) -> bool:
        """Check if the task is allowed to run based on the current time."""

        if not is_time_allowed(time, self.assumed_task_duration, self.allowed_times ):
            return False
        
        # check if the current utilization is below the defined limits
        for metric, limit in self.utilization_limits.items():
            if utilization.get(metric, 0) > limit:
                return False
        return True

    def run(self):
        """Run the scheduled task."""
        print(f"Running task: {self.name} at {datetime.now()}")
        self.started_on = pendulum.now()
        update_job(self.job_id, state="running", started_on=self.started_on)
        
        try:
            if "REGISTRY:" in self.command:
                func_name = self.command.split("REGISTRY:")[1]
                try:
                    PYTHON_TASK_REGISTRY[func_name]()
                    func_name()
                    update_job(self.job_id, state="finished")
                except Exception as e:
                    self.finished_on = pendulum.now()
                    update_job(self.job_id, state="ERROR", finished_on=self.finished_on)
                    print(f"Error running registered function {func_name}: {e}")
            else:
                return_code = subprocess.run(self.command, shell=True, capture_output=True, text=True)
                witherr = return_code.stderr
                if return_code.returncode != 0:
                    print(f"Error running command {self.command}: {witherr}")
                    self.finished_on = pendulum.now()
                    update_job(self.job_id, state="ERROR", finished_on=self.finished_on)
                else:
                    self.finished_on = pendulum.now()
                    update_job(self.job_id, state="finished", finished_on=self.finished_on)
        except Exception as e:
            print(f"Error running task {self.name}: {e}")
            self.finished_on = pendulum.now()
            update_job(self.job_id, state="ERROR-INRUN", finished_on=self.finished_on)

        duration = computed_duration(self.started_on, self.finished_on)
        update_job(self.job_id, true_task_duration=duration)
        return return_code

    
    def __repr__(self):
        return f"<Job(job_id={self.job_id}, state={self.state}, timestamp={self.timestamp})>"
    def __str__(self):
        return f"Job(job_id={self.job_id}, state={self.state}, timestamp={self.timestamp})"
    
    

def with_session(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        session = SessionLocal()
        try:
            result = func(session, *args, **kwargs)
            session.commit()
            return result
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    return wrapper


@with_session
def submit_job(session, job: JOB):
    if session.query(JOB).filter_by(job_id=job.job_id).first():
        print(f"Job {job.job_id} already exists. Skipping submission.")
        return
    else:
        session.add(job)
        print(f"Job {job.job_id} submitted successfully.")
    

@with_session
def update_job(session, job_id: str, **kwargs):

        valid_keys = list(JOB.__table__.columns.keys())
        kwargs = {k: v for k, v in kwargs.items() if v is not None and k in valid_keys}    

        with FileLock(DB_LOCK):
            job_record = session.query(JOB).filter_by(job_id=job_id).with_for_update().first()  # Lock the row for update
            
            if job_record is None:
                print(f"Job {job_id} not found.")
                return

            for key, value in kwargs.items():
                if hasattr(job_record, key):
                    setattr(job_record, key, value)

        return job_record

@with_session
def delete_job(session, job_id, force=False):
    with FileLock(DB_LOCK):
        job_records = session.query(JOB).filter_by(job_id=job_id).with_for_update()
    
        if job_records.count() == 0:
            print(f"Job {job_id} not found.")
            return

        for job_record in job_records:
            if job_record.state != "pending":
                if force:
                    session.delete(job_record)
                    print(f"Job {job_id} deleted successfully.")
                else:
                    print(f"Cannot delete job {job_id} as it is no loger in 'pending' state. Use force=True to delete it anyway.")
            else:
                session.delete(job_record)
                print(f"Job {job_id} deleted successfully.")
                        
    return
    
@with_session        
def count_jobs(session, state=None):
    jobs_query = session.query(JOB)
    if state is not None:
        jobs_query = jobs_query.filter_by(state=state)
    return jobs_query.count()

    
def init_db(): 
    """Create database tables if they do not exist.""" 
    Base.metadata.create_all(bind=engine)

@with_session
def db_to_csv(session, csv_path="jobs_export.csv"):
    """Export the jobs table to a CSV file."""
    jobs = session.query(JOB).all()
    with FileLock(DB_LOCK):
        with open(csv_path, "w") as f:
            # Write header
            f.write(";".join([column.name for column in JOB.__table__.columns]) + "\n")
            # Write rows
            for job_record in jobs:
                row = [str(getattr(job_record, column.name)) for column in JOB.__table__.columns]
                f.write(";".join(row) + "\n")
    print(f"Exported {len(jobs)} jobs to {csv_path}")

# ------------------
# SCHEDULER AND TASK CLASSES
# ------------------
def is_time_allowed(
    current_datetime_time: pendulum.DateTime,
    assumed_task_duration: pendulum.Duration = pendulum.duration(hours=1),
    allowed_times: dict = None,
) -> bool:
    """Check whether the task can fit within the allowed time interval."""

    if allowed_times is None:
        raise ValueError("allowed_times must be provided.")

    # Select the appropriate time interval.
    if current_datetime_time.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY):
        start_time, end_time = pendulum.parse(allowed_times["weekends"][0]), pendulum.parse(allowed_times["weekends"][1])
    else:
        start_time, end_time = pendulum.parse(allowed_times["work_days"][0]), pendulum.parse(allowed_times["work_days"][1])

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
    assumed_task_duration = assumed_task_duration if isinstance(assumed_task_duration, pendulum.Duration) else pendulum.duration(seconds=assumed_task_duration)
    latest_start_time = end_datetime - assumed_task_duration

    return start_datetime <= current_datetime_time <= latest_start_time



class Scheduler:
    def __init__(self, tasks: list = [], wait_time: int = 5, allowed_workload: int = 1):
        """Initialize the scheduler."""
        self.claimed_tasks = tasks
        self.wait_time = wait_time
        self.lock = threading.Lock()
        # 
        self.claimed_workload = sum(task.workload for task in self.claimed_tasks)
        self.allowed_workload = allowed_workload
        self.current_workload = 0
        # 
        self.machine_name = get_machine_utilization(verbose=False)["machine_name"]  # Print the current machine utilization

        # Track currently running tasks and their threads.
        self.running_tasks = {}
        
        if not self.claimed_tasks:
            self.claime_tasks()
            
    @with_session
    def claime_tasks(session, self):
        """Claim tasks from DB for the scheduler to manage."""
        # claime just the pending tasks
        with FileLock(DB_LOCK):
            while self.claimed_workload < self.allowed_workload:
                task = session.query(JOB).filter_by(state="pending", claimed=False).first()
                if not task:
                    break
                if self.claimed_workload + task.workload > self.allowed_workload:
                    print(f"Cannot claim task {task.name} as it would exceed the allowed workload.")
                    break
                self.claimed_tasks.append(task)
                session.query(JOB).filter_by(id=task.id, state="pending", claimed=False).update({"claimed": True, "scheduled_on": self.machine_name})
                session.commit() #commit after each claim to ensure the task is marked as claimed in the DB
                self.claimed_workload += task.workload
            print(f"Total claimed workload after claiming tasks: {self.claimed_workload}")
            print(f"CLAIMED TASKS: {[task.name for task in self.claimed_tasks]}")
        db_to_csv(csv_path="jobs_export.csv")  # Export the current state of the jobs table to CSV


    def _run_task(self, task):
        """Execute a task in a background thread and track its completion."""
        try:
            print(f"Task {task.name} started.")
            self.claimed_workload -= task.workload
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
                self.claimed_tasks.remove(task)  # Remove the task from the scheduler after completion
            print(f"Task {task.name} finished.")


    def is_task_running(self, task_name: str) -> bool:
        """Return whether a task is currently running."""
        with self.lock:
            return task_name in self.running_tasks

    def run(self):
        """Run the scheduler continuously."""
        while True:
            current_time = pendulum.now()

            # Take a snapshot of the task list safely.
            with self.lock:
                tasks_snapshot = list(self.claimed_tasks)

            for task in tasks_snapshot:
                # Skip tasks that are already running.
                with self.lock:
                    if task.name in self.running_tasks:
                        continue

                utilization = get_machine_utilization(verbose=False)

                # Check if the task is allowed to run - utilization and time constraints.
                if not task.allowed_to_run(utilization, current_time):
                    continue

                # Reserve workload and register the thread atomically.
                with self.lock:
                    if task.name in self.running_tasks:
                        continue

                    # task not allowed to run if it exceeds the allowed workload
                    if self.current_workload + task.workload > self.allowed_workload:
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
                    
            self.claime_tasks()  # Attempt to claim more tasks if possible
            time.sleep(self.wait_time)

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
    init_db()  # Initialize the database and create tables if they don't exist
    args = parser.parse_args()
    
    allowed_times_logger = {
        "work_days": (str(pendulum.time(0, 0)), str(pendulum.time(23, 59, 59))),  # 9 PM to 6 AM - night time
        "weekends": (str(pendulum.time(0, 0)), str(pendulum.time(23, 59, 59))),  # All day Saturday and Sunday
    }
    
    allowed_times_tasks = {
        "work_days": (str(pendulum.time(0, 0)), str(pendulum.time(23, 59, 59))),  # 9 PM to 6 AM - night time
        "weekends": (str(pendulum.time(0, 0)), str(pendulum.time(23, 59, 59))),  # All day Saturday and Sunday
    }

    
    def repeating_logger(log_interval_s: int = 5, filepath: str = "utilization_log.csv"):
        while True:
            utilization = get_machine_utilization(verbose=False)
            log_utilization_to_file(utilization, filename=filepath)
            time.sleep(log_interval_s)  # Log every log_interval_s seconds
   
    def db_logger(log_interval_s: int = 5, filepath: str = "jobs_export.csv"):
        while True:
            db_to_csv(csv_path=filepath)
            time.sleep(log_interval_s)  # Log every log_interval_s seconds

    try:
        # OLD API
        # task_logger = Task(name="Logger", command=lambda: repeating_logger(log_interval_s=args.log_interval, filepath=args.log_file),
        #                 assumed_task_duration=pendulum.duration(seconds=1),  # 1 second
        #                 utilization_limits={}, allowed_times=allowed_times_logger, workload=0)
        # task_db_logger = Task(name="DB Logger", command=lambda: db_logger(log_interval_s=args.log_interval, filepath=args.log_file),
        #                 assumed_task_duration=pendulum.duration(seconds=1),  # 1 second
        #                 utilization_limits={}, allowed_times=allowed_times_logger, workload=0)

        PYTHON_TASK_REGISTRY["logger"] = lambda: repeating_logger(log_interval_s=args.log_interval, filepath=args.log_file)
        PYTHON_TASK_REGISTRY["db_logger"] = lambda: db_logger(log_interval_s=args.log_interval)
        # NEW API
        # with SessionLocal() as session:
        utilization_logger = JOB(
            job_id="logger",
            command="REGISTRY:logger",  # Indicate that this is a registered Python function SYNAX: "REGISTRY:<function_name>"
            state="pending",
            timestamp=pendulum.now(),
            assumed_task_duration=1,  # 1 second
            allowed_times=allowed_times_logger,
            utilization_limits={},  # No limits for the logger
            workload=0,  # No workload for the logger
        )
        submit_job(utilization_logger)
        
        db_logger_task = JOB(
            job_id="db_logger",
            command="REGISTRY:db_logger",  # Indicate that this is a registered Python function SYNAX: "REGISTRY:<function_name>"
            state="pending",
            timestamp=pendulum.now(),
            assumed_task_duration=1,  # 1 second
            allowed_times=allowed_times_logger,
            utilization_limits={},  # No limits for the logger
            workload=0,  # No workload for the logger
        )
        submit_job(db_logger_task)
    
        for i in range(3):
            job = JOB(
                job_id=f"job_00{i+1}",
                command="echo 'Hello World!'",
                timestamp=pendulum.now(),
                assumed_task_duration=356,
                allowed_times=allowed_times_tasks,
                workload=0.3,  # Computational cost of the task
            )
            submit_job(job)
            
        for i in range(3):
            job = JOB(
                job_id=f"job_00{i+1}",
                command="echo 'Hello World!'",
                timestamp=pendulum.now(),
                assumed_task_duration=356,
                allowed_times=allowed_times_tasks,
                workload=0.6,  # Computational cost of the task
            )
            submit_job(job)
            
        for i in range(3):
            job = JOB(
                job_id=f"job_random_{i+1}",
                command="echo 'Hello World!'",
                timestamp=pendulum.now(),
                assumed_task_duration=1000,
                allowed_times=allowed_times_tasks,
                workload=0.6,  # Computational cost of the task
            )
            submit_job(job)
            
            
            
        db_to_csv(csv_path="jobs_export.csv")    
        # time.sleep(5)
        # for i in range(3):
        #     update_job(f"job_00{i+1}", state="started", scheduled_on="TSXX")
        #     db_to_csv(csv_path="jobs_export.csv")    
        #     time.sleep(5)
        
        
        scheduler = Scheduler(wait_time=args.wait, allowed_workload=args.allowed_workload)
        scheduler.run()
        
        with SessionLocal() as session:
           
            job_ids = session.scalars(sqlalchemy.select(JOB.job_id).distinct()).all()
            print("Current jobs in the database:")
            print(job_ids)
            
            
            for job_id in job_ids:
                delete_job(job_id, force=True)
        db_to_csv(csv_path="jobs_export.csv")
        # scheduler = Scheduler(tasks=[task_logger,])  # Add the same task twice for demonstration
        # scheduler.run()


    except KeyboardInterrupt:
        print("\nMachine utilization monitoring stopped.")