import shutil
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Union

import pytest
import requests

from pyorderly.outpack.init import outpack_init
from pyorderly.outpack.root import OutpackRoot


def _wait_ready(p, url, args, timeout=2):
    # This function waits for outpack server to become ready while monitoring
    # the child process to detect any start-up errors.
    #
    # In a modern (post-2019) Linux world, the foolproof way to do this is as
    # follows:
    # - Setup a lightweight IPC between us and the server used to communicate
    #   readiness. This can be an sd_notify socket, an eventfd or just a pipe.
    # - Setup a pidfd that can be used to watch the child
    #   process.
    # - Use select(2) to wait for the two events simultaneously.
    #
    # The IPC can probably be designed to be portable, albeit not using the
    # sd_notify protocol. Other platforms have equivalents to the pidfd and
    # select combo (namely kqueue on macOS, or WaitForMultipleObjects on
    # Windows), but these are all very-platform specific and don't have
    # bindings in the Python stdlib.
    #
    # asyncio should provide a platform-independent way to do this kind of
    # stuff, but it's a lot of boilerplate and the asyncio module don't have a
    # way to monitor an existing child process. It's not obvious what would
    # happend if we launched the child process using the asyncio module and
    # then shut down the event loop once the server is ready.
    #
    # In the end, we do the low-tech thing and poll the HTTP endpoint until it
    # becomes ready. Each time that fails, we also check the process to see if
    # it has quit. It can add a bit of latency due to the sleep in the polling
    # loop, but it is more portable and simpler than any other solution.

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        code = p.poll()
        if code is not None:
            raise subprocess.CalledProcessError(code, args)

        try:
            r = requests.get(url, timeout=1)
            r.raise_for_status()
            return
        except requests.ConnectionError:
            pass

        time.sleep(0.05)

    msg = "outpack_server failed to start in time"
    raise Exception(msg)


@contextmanager
def start_outpack_server(root: Union[Path, OutpackRoot], port: int = 8080):
    binary = shutil.which("outpack")
    if binary is None:
        pytest.skip("outpack_server not installed")

    if isinstance(root, OutpackRoot):
        root_path = str(root.path)
    else:
        root_path = str(root)

    outpack_init(
        root_path,
        require_complete_tree=True,
        use_file_store=True,
        path_archive=None,
    )

    args = [
        binary,
        "start-server",
        "--root",
        root_path,
        "--listen",
        f"0.0.0.0:{port}",
    ]

    url = f"http://127.0.0.1:{port}"
    with subprocess.Popen(args) as p:  # noqa: S603
        try:
            _wait_ready(p, url, args)
            yield url

            # We expect the server to still be running after the yield returns.
            # If it isn't then that suggests something we did made it crash.
            code = p.poll()
            if code is not None:
                raise subprocess.CalledProcessError(p.returncode, args)

        finally:
            p.terminate()
