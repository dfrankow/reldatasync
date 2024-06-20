#!/usr/bin/env python

import time
from subprocess import Popen, run

from tests.rds_test_server import SERVER_ROOT

_TESTS_DIR = "tests"


def main():
    # start server (async)
    with Popen([f"{_TESTS_DIR}/rds_test_server.py"]) as server_process:
        # run client and check return value is 0
        time.sleep(1)
        client_process = run(
            [
                f"{_TESTS_DIR}/rds_test_client.py",
                "-s",
                f"127.0.0.1:5000/{SERVER_ROOT}/",
            ],
            check=True,
        )
        assert (
            client_process.returncode == 0
        ), f"Non-zero return code: {client_process.returncode}"
        print(f"client return code is {client_process.returncode} (ok)")

        # kill server
        server_process.terminate()
        server_process.wait()
        assert server_process.returncode == -15, "Shut down due to SIGTERM"
        print(f"server return code is {server_process.returncode} (ok)")


main()
