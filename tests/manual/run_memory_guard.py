"""Run a diagnostic with an external process-tree memory limit.

Example: pixi run python tests/manual/run_memory_guard.py --limit-gib 12 -- python tests/manual/example.py
The limit is sampled, so leave enough headroom for allocations between samples.
"""

import argparse
import subprocess
import sys
import time

import psutil


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-gib", type=float, required=True)
    parser.add_argument("--reserve-gib", type=float, default=8.0)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command or args.limit_gib <= 0 or args.reserve_gib < 0 or args.interval <= 0:
        parser.error("provide a command and positive limit and sampling interval")
    if command[0] == "python":
        command[0] = sys.executable

    child = subprocess.Popen(command)
    root = psutil.Process(child.pid)
    peak = 0
    try:
        while child.poll() is None:
            try:
                processes = [root] + root.children(recursive=True)
                rss = 0
                for process in processes:
                    try:
                        rss += process.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                peak = max(peak, rss)
                available = psutil.virtual_memory().available
                if rss > args.limit_gib * 1024**3 or available < args.reserve_gib * 1024**3:
                    print(f"Memory guard stopping process tree: RSS {rss / 1024**3:.2f} GiB; "
                          f"system available {available / 1024**3:.2f} GiB", flush=True)
                    # Kill descendants and parent even when native code is running.
                    for process in reversed(processes):
                        try:
                            process.kill()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                    child.wait()
                    return 137
            except psutil.NoSuchProcess:
                break
            time.sleep(args.interval)
        return child.wait()
    finally:
        print(f"Memory guard sampled peak: {peak / 1024**3:.2f} GiB", flush=True)


if __name__ == "__main__":
    sys.exit(main())
