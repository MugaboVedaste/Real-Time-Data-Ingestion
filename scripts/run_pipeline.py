"""Run the whole pipeline with one command.

    python scripts/run_pipeline.py

Starts the Spark streaming job, waits until it is ready, starts the data
generator, and when you press CTRL+C (or the generator finishes) stops both and
prints a performance summary for this session.

The individual scripts still work on their own:
    python scripts/spark_streaming_to_postgres.py
    python scripts/data_generator.py
    python scripts/measure_performance.py

Options:
    --events N      events per CSV file (generator, default 10)
    --interval S    seconds between files (generator, default 5)
    --files N       stop after N files, let Spark finish, then exit
                    (default: run until CTRL+C)
    --no-measure    skip the performance summary at the end
"""

import argparse
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPTS_DIR.parent

READY_LINE = "Waiting for incoming events"
STARTUP_TIMEOUT_SECONDS = 120
DRAIN_SECONDS = 12  # let Spark pick up and write the last files


def forward_output(process):
    """Copy a process's output to this console until it ends."""

    for line in process.stdout:
        print(f"[spark] {line}", end="", flush=True)


def wait_until_ready(spark):
    """Block until Spark prints its ready line. Return True on success."""

    deadline = time.time() + STARTUP_TIMEOUT_SECONDS

    for line in spark.stdout:
        print(f"[spark] {line}", end="", flush=True)

        if READY_LINE in line:
            return True

        if time.time() > deadline:
            return False

    return False  # process ended before it became ready


def stop(process, name):
    """Ask a child to stop, then force it if it does not."""

    if process.poll() is not None:
        return

    process.terminate()

    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()

    print(f"Stopped {name}.", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the generator and the Spark job together."
    )
    parser.add_argument("--events", type=int, default=None)
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--files", type=int, default=0)
    parser.add_argument("--no-measure", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()

    session_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    generator_cmd = [sys.executable, str(SCRIPTS_DIR / "data_generator.py")]
    if args.events is not None:
        generator_cmd += ["--events", str(args.events)]
    if args.interval is not None:
        generator_cmd += ["--interval", str(args.interval)]
    if args.files:
        generator_cmd += ["--files", str(args.files)]

    print("Starting Spark Structured Streaming...", flush=True)

    spark = subprocess.Popen(
        [sys.executable, "-u", str(SCRIPTS_DIR / "spark_streaming_to_postgres.py")],
        cwd=PROJECT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    generator = None
    finished_normally = False

    try:
        if not wait_until_ready(spark):
            print(
                "Spark did not start. See the messages above "
                "(is PostgreSQL running and .env filled in?).",
                flush=True,
            )
            return 1

        threading.Thread(target=forward_output, args=(spark,), daemon=True).start()

        print("Starting data generator... (CTRL+C to stop)\n", flush=True)
        generator = subprocess.Popen(generator_cmd, cwd=PROJECT_DIR)

        while generator.poll() is None:
            if spark.poll() is not None:
                print("Spark stopped unexpectedly.", flush=True)
                return 1
            time.sleep(1)

        # Generator finished (--files was given): let Spark process the rest.
        print(f"\nGenerator finished. Waiting {DRAIN_SECONDS}s for Spark...", flush=True)
        time.sleep(DRAIN_SECONDS)
        finished_normally = True

    except KeyboardInterrupt:
        print("\nStopping...", flush=True)
        finished_normally = True

    finally:
        if generator is not None:
            stop(generator, "generator")
        stop(spark, "Spark")

    if finished_normally and not args.no_measure:
        print("\n=== Performance summary for this session ===", flush=True)
        subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "measure_performance.py"),
                "--since", session_start,
            ],
            cwd=PROJECT_DIR,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
