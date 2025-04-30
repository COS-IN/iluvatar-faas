from typing import Optional
import logging
import sys

def create_logger(log_file: Optional[str], level = logging.INFO) -> logging.Logger:
    if log_file is not None:
        logging.basicConfig(filename=log_file, filemode='a', level=level, force=True)
    else:
        logging.basicConfig(stream=sys.stdout, level=level, force=True)
    return logging.getLogger("iluvatar")