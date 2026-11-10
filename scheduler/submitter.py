# submitter.py
from .scheduler import  init_db, submit_job, JOB, db_to_csv, delete_job 
import pendulum
import argparse
import time

if __name__ == "__main__":
    init_db()  # Initialize the database and create tables if they don't exist
        
    allowed_times_tasks = {
        "work_days": [(str(pendulum.time(0, 0, 0)), str(pendulum.time(7, 0, 0))),     # 1st interval - Morning
                      (str(pendulum.time(14, 30)), str(pendulum.time(23, 59, 59))),],  # 2nd interval - Evening
        # "work_days": (str(pendulum.time(16, 0)), str(pendulum.time(6, 00, 00))),  # 4 PM to 6 AM - night time
        "weekends": [(str(pendulum.time(0, 0)), str(pendulum.time(23, 59, 59)))],  # All day Saturday and Sunday
    }
   
    try:            
        for i in range(100):
            job = JOB(
                job_id=f"QMC_{i}",
                command=f"python run_openLAB.py --path X:/qmc/{i}",
                timestamp=pendulum.now(),
                assumed_task_duration=1200,
                min_wait_time=60, #Minimum wait time of 60 seconds before the next iteration
                allowed_times=allowed_times_tasks,
                workload=0.25,  # Computational cost of the task
            )
            submit_job(job)
                        
        db_to_csv(csv_path="jobs_export.csv")    
        
    except KeyboardInterrupt:
        print("\nMachine utilization monitoring stopped.")