"""
Pytest configuration and shared fixtures for metaflow-kestra tests.
"""
import os
import subprocess
import sys

import pytest

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def compile_flow(flow_file: str, output_file: str, extra_args=None) -> str:
    """Compile a Metaflow flow to a Kestra YAML file.

    Returns the content of the generated YAML.
    Raises subprocess.CalledProcessError on failure.
    """
    cmd = [
        sys.executable, flow_file,
        "--no-pylint",
        "kestra", "create", output_file,
    ]
    if extra_args:
        cmd.extend(extra_args)
    # Always compile with local metadata/datastore so the generated YAML
    # works with standard open-source metaflow in the Docker container.
    env = os.environ.copy()
    env["METAFLOW_DEFAULT_METADATA"] = "local"
    env["METAFLOW_DEFAULT_DATASTORE"] = "local"
    env["METAFLOW_DEFAULT_ENVIRONMENT"] = "local"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        err = subprocess.CalledProcessError(
            result.returncode, cmd,
            output=result.stdout,
            stderr=result.stderr,
        )
        # Attach human-readable output so pytest shows it in the error
        err.args = (
            "exit %d\nSTDOUT: %s\nSTDERR: %s"
            % (result.returncode, result.stdout[:2000], result.stderr[:2000]),
        )
        raise err
    with open(output_file) as f:
        return f.read()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def kestra_host():
    return os.environ.get("KESTRA_HOST", "http://localhost:8090")


@pytest.fixture
def kestra_client(kestra_host):
    """A requests.Session pointed at the local Kestra instance."""
    try:
        import requests
    except ImportError:
        pytest.skip("requests not installed")
    session = requests.Session()
    session._kestra_host = kestra_host
    # Apply credentials if provided
    user = os.environ.get("KESTRA_USER", "admin@kestra.io")
    password = os.environ.get("KESTRA_PASSWORD", "Kestra1234!")
    if user and password:
        session.auth = (user, password)
    # Check connectivity
    try:
        resp = session.get("%s/api/v1/flows/search" % kestra_host, timeout=5)
        if resp.status_code >= 500:
            pytest.skip("Kestra server returned HTTP %d" % resp.status_code)
        if resp.status_code == 401:
            pytest.skip("Kestra server requires auth — set KESTRA_USER/KESTRA_PASSWORD")
    except Exception:
        pytest.skip("Kestra server not reachable at %s" % kestra_host)
    return session
