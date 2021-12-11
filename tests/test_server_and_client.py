#!/usr/bin/env python

from subprocess import Popen, run
import time


_TESTS_DIR = 'tests'


def main():
    # start server (async)
    server_process = Popen([f"{_TESTS_DIR}/rds_server.py"])

    # run client and check return value is 0
    time.sleep(1)
    client_process = run([f"{_TESTS_DIR}/rds_client.py", "-s", "127.0.0.1:5000/"])
    assert client_process.returncode == 0, (
        f"Non-zero return code: {client_process.returncode}")
    print(f"client return code is {client_process.returncode} (ok)")

    # kill server
    server_process.terminate()
    server_process.wait()
    assert server_process.returncode == -15, "Shut down due to SIGTERM"
    print(f"server return code is {server_process.returncode} (ok)")


main()
