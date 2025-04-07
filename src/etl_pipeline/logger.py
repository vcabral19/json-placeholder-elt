import logging
import os
import time

def get_logger(name: str = __name__) -> logging.Logger:
    """
    Returns a configured logger that writes logs to both the console
    and a file at logs/etl.log.
    """
    # Ensure the logs directory exists
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Avoid duplicate handlers if logger is already configured
    if not logger.handlers:
        # File handler: logs everything at DEBUG level and above
        file_handler = logging.FileHandler(os.path.join(log_dir, "etl.log"))
        file_handler.setLevel(logging.DEBUG)
        
        # Console handler: logs INFO level and above
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        formatter.converter = time.gmtime
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
