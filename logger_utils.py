import sys
import traceback


def log_error(e):
    with open("error.log", "w") as f:
        f.write(str(e) + "\n")
        traceback.print_exc(file=f)
