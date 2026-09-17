"""Temporary local Redis processes; no deployed environment is touched."""

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import redis

from python_stdx.redis import RedisTopology


@pytest.fixture(scope="module", name="redis_processes")
def fixture_redis_processes(tmp_path_factory):
    executable = os.getenv("REDIS_SERVER") or shutil.which("redis-server")
    if not executable and Path("/opt/homebrew/opt/redis/bin/redis-server").is_file():
        executable = "/opt/homebrew/opt/redis/bin/redis-server"
    if not executable:
        pytest.skip("Install redis-server or set REDIS_SERVER for socket integration tests")
    directory = tmp_path_factory.mktemp("redis-pools")
    processes = []

    def start(*, sentinel_master=None, cluster=False, replica_of=None):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config = directory / f"{port}.conf"
        lines = [
            f"port {port}",
            "bind 127.0.0.1",
            'save ""',
            "appendonly no",
            "repl-diskless-sync-delay 0",
            f"dir {directory}",
        ]
        if replica_of:
            lines += [f"replicaof 127.0.0.1 {replica_of}"]
        if sentinel_master:
            lines += [f"sentinel monitor mymaster 127.0.0.1 {sentinel_master} 1"]
        if cluster:
            # An independent bus port avoids collisions with other local processes.
            with socket.socket() as bus:
                bus.bind(("127.0.0.1", 0))
                bus_port = bus.getsockname()[1]
            lines += [
                "cluster-enabled yes",
                f"cluster-config-file nodes-{port}.conf",
                f"cluster-port {bus_port}",
                "cluster-node-timeout 1000",
            ]
        config.write_text("\n".join(lines) + "\n")
        command = [executable, str(config)]
        if sentinel_master:
            command.append("--sentinel")
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        processes.append(process)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if process.poll() is not None:
                pytest.fail("Test Redis failed to start")
            try:
                with redis.Redis(host="127.0.0.1", port=port) as client:
                    client.ping()
                return port
            except redis.ConnectionError:
                time.sleep(0.02)
        pytest.fail("Test Redis did not become ready")

    yield start
    for process in processes:
        process.terminate()
    for process in processes:
        process.wait(timeout=5)


@pytest.fixture(scope="module", name="redis_cluster")
def fixture_redis_cluster(redis_processes):
    ports = [redis_processes(cluster=True) for _ in range(3)]
    with redis.Redis(host="127.0.0.1", port=ports[0]) as first:
        for port in ports[1:]:
            with redis.Redis(host="127.0.0.1", port=port, decode_responses=True) as peer:
                bus_port = peer.config_get("cluster-port")["cluster-port"]
            first.execute_command("CLUSTER", "MEET", "127.0.0.1", port, bus_port)
    for index, port in enumerate(ports):
        with redis.Redis(host="127.0.0.1", port=port) as node:
            start = index * 16384 // 3
            end = (index + 1) * 16384 // 3
            node.execute_command("CLUSTER", "ADDSLOTS", *range(start, end))
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        states = []
        for port in ports:
            with redis.Redis(host="127.0.0.1", port=port) as node:
                states.append(node.cluster("INFO").get("cluster_state"))
        if states == ["ok"] * 3:
            return ports[0], RedisTopology.CLUSTER
        time.sleep(0.05)
    pytest.fail("Test cluster did not become ready")


@pytest.fixture(scope="module", params=list(RedisTopology))
def target(request, redis_processes):
    if request.param is RedisTopology.CLUSTER:
        return request.getfixturevalue("redis_cluster")
    port = redis_processes()
    if request.param is RedisTopology.SENTINEL:
        port = redis_processes(sentinel_master=port)
    return port, request.param
