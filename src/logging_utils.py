"""
Dr Nneoma O
Logging utility:  The purpose of this is to see exactly what the pipeline is doing, for easily diagnosis and clear audit trail.
This essentially replaces using print() after each block of code and is better as it has more information, which can be saved in files

"""
# Import relevant libraries
import logging
import os
from datetime import datetime


# I define a function that creates and returns a configured logger
# The optional 'name' argument allows me to create different loggers for different parts of the pipeline if required

def get_logger(name: str = "pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)   
    if logger.handlers:
        return logger # If this logger already has handlers attached, I return it immediately to avoid adding duplicate handlers
    
  os.makedirs("outputs/reports", exist_ok=True) # I ensure the reports output directory exists, and exist_ok=True prevents an error if the folder already exists

    # I build a file path for the log file, including today’s date so each run is logged to a daily file
    log_path = os.path.join(
        "outputs/reports",
        f"run_log_{datetime.now():%Y%m%d}.log"
    )

    # I define a standard log message format that includes: timestamp, log level (INFO, WARNING, ERROR, etc.), the actual message
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    # Create a stream handler so log messages are printed to the console while the pipeline is running
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    # Next create a file handler so the same log messages are written to a log file on disk
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)

    # Attach both handlers to the logger so every log message goes to both the console and the log file
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger
