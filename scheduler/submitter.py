# from pathlib import Path
# import json
# import pathlib
# import click
# from config import CONFIG_FILE, DEFAULT_NOTES_DIR, load_config, save_config
# import crud
# import bulk_crud

# @click.group()
# @click.version_option()
# @click.pass_context
# def cli(ctx: click.Context):
#     """A simple note-taking application."""
#     config = load_config()

#     notes_directory = config.get("notes_directory", DEFAULT_NOTES_DIR)

#     ctx.ensure_object(dict)
#     ctx.obj["config"] = config
#     ctx.obj["notes_directory"] = Path(notes_directory)


# @cli.group()
# def bulk():
#     """Bulk operations on notes."""
#     pass



# @config.command()
# def show():
#     """Show the current configuration."""
#     if not CONFIG_FILE.exists():
#         click.echo("No configuration found.")
#         return

#     config = load_config()
#     click.echo(f"Notes directory: {config.get('notes_directory', DEFAULT_NOTES_DIR)}")


# @config.command()
# @click.option("--notes_directory", "-n", type=click.Path(exists=True))
# def update(notes_dir: str):
#     """Setup the notes directory."""
#     config = load_config()
#     config["notes_directory"] = notes_dir

#     save_config(config)
#     click.echo(f"Notes directory set to '{notes_dir}'.")


# bulk.add_command(bulk_crud.create)
# bulk.add_command(bulk_crud.delete)
# bulk.add_command(bulk_crud.update)
# bulk.add_command(bulk_crud.rename)


# cli.add_command(crud.create)
# cli.add_command(crud.read)
# cli.add_command(crud.update)
# cli.add_command(crud.delete)
# cli.add_command(crud.show)

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
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Text, JSON, distinct
from sqlalchemy.orm import declarative_base, sessionmaker, Session
import sqlalchemy
from warnings import filterwarnings, warn

from functools import wraps


WAIT = 7 



parser = argparse.ArgumentParser(description="Scheduler Simulation")
parser.add_argument("--wait", type=int, default=5, help="Wait time between scheduler s runs in seconds")
parser.add_argument("--queue_path", type=str, default="queue.json", help="Path to the queue file")

JOB_QUEUE: pathlib.Path = pathlib.Path("queue.json")
SQLITE_DB: pathlib.Path = pathlib.Path("jobs.db")
DATABASE_URL = "sqlite:///queue.db"
DB_LOCK = pathlib.Path("queue.db.lock")

engine = create_engine( DATABASE_URL, connect_args={"check_same_thread": False}, )
SessionLocal = sessionmaker( bind=engine, expire_on_commit=False, )
Base = declarative_base()

# SQLite database file DATABASE_URL = "sqlite:///queue.db" # Create the database engine engine = create_engine( DATABASE_URL, connect_args={"check_same_thread": False}, ) # Create the session factory SessionLocal = sessionmaker( bind=engine, expire_on_commit=False, )
class job(Base):
    
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    job_id = Column(String, index=True)
    command = Column(String)
    timestamp = Column(DateTime)
    state = Column(String)
    scheduled_on = Column(DateTime)
    started_on = Column(DateTime)
    finished_on = Column(DateTime)
    duration = Column(Integer)
    mc_args = Column(JSON)
    
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
def submitt_job(session, job_id, job_command, wait_time=5):
        new_job = job(
            job_id=job_id,
            command=job_command,
            timestamp=pendulum.now(),
            state="pending",
            scheduled_on=None,
            started_on=None,
            finished_on=None,
            duration=None,
            mc_args=json.dumps({}),
        )
        session.add(new_job)

@with_session
def update_job(session, job_id: str, **kwargs):

        valid_keys = list(job.__table__.columns.keys())
        kwargs = {k: v for k, v in kwargs.items() if v is not None and k in valid_keys}    

        job_record = session.query(job).filter_by(job_id=job_id).first().with_for_update()  # Lock the row for update
        if job_record is None:
            raise ValueError(f"Job {kwargs.get('job_id')} not found.")

        for key, value in kwargs.items():
            if hasattr(job_record, key):
                setattr(job_record, key, value)

        return job_record

@with_session
def delete_job(session, job_id, force=False):
    job_records = session.query(job).filter_by(job_id=job_id).with_for_update()
    
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
    jobs_query = session.query(job)
    if state is not None:
        jobs_query = jobs_query.filter_by(state=state)
    return jobs_query.count()

    
def init_db(): 
    """Create database tables if they do not exist.""" 
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    init_db()

    submitt_job("job_001", "echo 'Hello, World!'")

    submitt_job("job_002", "echo 'Hello, World!'")
    submitt_job("job_002", "echo 'HelSlo, World!'")
    update_job("job_002", state="started")

    with SessionLocal() as session:
        job_ids = session.query(distinct(job.job_id)).all()
        print("Current jobs in the database:")
        print(job_ids)
        
        
    for job_id in ["job_001", "job_002"]:
        delete_job(job_id, force=True)
    
    