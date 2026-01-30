#!/usr/bin/env python3
"""
Data Transfer Tool - Universal incremental data sync using rsync daemon pattern.

This tool provides reliable, incremental transfers between different storage systems using
rsync's proven delta-sync algorithm. It works by temporarily starting an rsync daemon on
the source, establishing a network connection, and pulling data to the target.

URL Schema:
  docker-volume:/volume-name[/path]            - Docker volume (host from FROM_HOST/TO_HOST)
  k8s-pvc:/namespace/pvc-name[/path]           - K8s PVC (context from k8s:CONTEXT host)
  directory:/path                               - Filesystem directory
  weaviate://[api-key:KEY@]host[:port]         - Weaviate instance
  postgres://user:pass@container-or-pod/database - Postgres database (via pg_dump/restore)

Transfer Methods:
  - File-based (docker-volume, k8s-pvc, directory): Uses rsync daemon for incremental sync
  - Weaviate: REST API-based object transfer with batch import
  - Postgres: pg_dump → rsync-p2p (Malai P2P) → pg_restore with NAT traversal

Examples:
  # Docker volume → K8s PVC (rsync daemon, incremental)
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/data_volume->k8s-pvc:/default/data-pvc

  # K8s PVC → K8s PVC (cross-cluster, rsync daemon)
  transfer-data.py k8s:cluster1 k8s:cluster2 \\
    --copy k8s-pvc:/default/data-pvc->k8s-pvc:/production/data-pvc

  # Directory → K8s PVC (rsync daemon)
  transfer-data.py root@remote.host k8s:orbstack \\
    --copy directory:/var/lib/data->k8s-pvc:/default/app-data

  # Docker volume → Directory (rsync daemon)
  transfer-data.py root@docker.host root@file.server \\
    --copy docker-volume:/backup->directory:/mnt/backups/myapp

  # With exclusions and subpaths
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/my-volume/app/data->k8s-pvc:/default/my-pvc/data \\
    --exclude '*.temp,*.log,cache/*'

  # Postgres database (rsync-p2p with Malai NAT traversal)
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy postgres://dagster:dagster@ai-platform-dagster_postgres-1/dagster->postgres://postgres@postgres-1/dagster

  # Weaviate instance (REST API batch transfer)
  transfer-data.py localhost localhost \\
    --copy weaviate://localhost:8080->weaviate://api-key:secret@localhost:18080

  # Multiple copies in one command
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/vol1->k8s-pvc:/default/pvc1 \\
    --copy docker-volume:/vol2->k8s-pvc:/default/pvc2 \\
    --copy postgres://user:pass@postgres-container/db->postgres://user:pass@postgres-pod/db \\
    --dry-run

  # Debug mode for troubleshooting
  transfer-data.py root@docker.host k8s:orbstack \\
    --copy docker-volume:/data->k8s-pvc:/default/data \\
    --debug

Features:
  ✅ Incremental sync - only transfers differences (rsync delta algorithm)
  ✅ NAT traversal - peer-to-peer via Malai, works across firewalls
  ✅ Universal support - all combinations of docker/k8s/directory
  ✅ Subpath support - transfer specific subdirectories
  ✅ Exclusion patterns - skip files/directories matching patterns
  ✅ Cross-cluster K8s - works across isolated networks
  ✅ Resume capability - handles interrupted transfers
  ✅ Compression - efficient network usage
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass
from typing import Optional, List, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class Location:
    """Represents a source or destination location."""
    scheme: str
    host: Optional[str]
    path: Optional[str]
    username: Optional[str]
    password: Optional[str]
    port: Optional[int]
    service_name: Optional[str] = None

    @classmethod
    def parse(cls, url: str, default_host: Optional[str] = None) -> 'Location':
        """Parse a URL into a Location object."""
        parsed = urlparse(url)

        if parsed.scheme == 'docker-volume':
            parts = parsed.path.lstrip('/').split('/', 1)
            volume = parts[0] if parts else None
            subpath = '/' + parts[1] if len(parts) > 1 else None

            return cls(
                scheme='docker-volume',
                host=default_host,
                path=f"{volume}{subpath or ''}",
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'k8s-pvc':
            path_parts = parsed.path.lstrip('/').split('/', 2)

            if len(path_parts) == 0:
                raise ValueError("k8s-pvc URL must specify at least namespace/pvc-name")
            elif len(path_parts) == 1:
                namespace = 'default'
                pvc = path_parts[0]
                subpath = None
            elif len(path_parts) == 2:
                namespace = path_parts[0]
                pvc = path_parts[1]
                subpath = None
            else:
                namespace = path_parts[0]
                pvc = path_parts[1]
                subpath = '/' + path_parts[2]

            return cls(
                scheme='k8s-pvc',
                host=default_host,
                path=f"{namespace}/{pvc}{subpath or ''}",
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'directory':
            if parsed.netloc:
                host = parsed.netloc
                path = parsed.path
            else:
                host = default_host
                path = '/' + parsed.path.lstrip('/')
            return cls(
                scheme='directory',
                host=host,
                path=path,
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'weaviate':
            port = parsed.port
            if port is None:
                port = 80
            return cls(
                scheme='weaviate',
                host=default_host,
                path=parsed.path or '/',
                username=parsed.username,
                password=parsed.password,
                port=port,
                service_name=parsed.hostname
            )

        elif parsed.scheme == 'postgres':
            database = parsed.path.lstrip('/')
            return cls(
                scheme='postgres',
                host=default_host,
                path=database,
                username=parsed.username,
                password=parsed.password,
                port=parsed.port or 5432,
                service_name=parsed.hostname
            )

        else:
            raise ValueError(f"Unsupported scheme: {parsed.scheme}")


class TransferEngine:
    """Handles data transfers between different storage types."""

    TRANSFER_P2P_IMAGE = "ghcr.io/nightscape/transfer-p2p:main"

    def __init__(self, from_host: str, to_host: str, dry_run: bool = False, debug: bool = False):
        self.from_host = from_host
        self.to_host = to_host
        self.dry_run = dry_run
        self.debug = debug

    # --- Helpers ---

    def _generate_unique_id(self) -> str:
        return subprocess.run(
            ['date', '+%s%N'], capture_output=True, text=True
        ).stdout.strip()[:16]

    def _parse_malai_id(self, output: str) -> Optional[str]:
        for line in output.splitlines():
            if line.startswith('MALAI_ID='):
                return line.split('=', 1)[1].strip()
        return None

    def _require_malai_id(self, output: str, context_msg: str) -> str:
        malai_id = self._parse_malai_id(output)
        if not malai_id:
            raise Exception(f"Failed to get Malai ID from {context_msg}. Logs: {output}")
        if self.debug:
            print(f"[DEBUG] Got Malai ID: {malai_id}")
        return malai_id

    def _kubectl_apply_spec(self, context: str, spec: dict):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(spec, f)
            spec_file = f.name
        try:
            self.execute_kubectl(context, f"apply -f {spec_file}")
        finally:
            os.unlink(spec_file)

    def _parse_postgres_service(self, service_name: str) -> Tuple[str, str]:
        """Returns (pod_name, namespace) from a postgres service_name like 'pod.namespace.svc.cluster.local'."""
        parts = service_name.split('.')
        pod = parts[0]
        if len(parts) >= 2 and parts[1] not in ('svc', 'cluster', 'local'):
            namespace = parts[1]
        else:
            namespace = 'default'
        return pod, namespace

    def _start_docker_get_malai_id(self, host: str, start_cmd: str, container_name: str) -> str:
        """Start a Docker container and extract its MALAI_ID from logs."""
        self.execute_on_host(host, start_cmd)
        time.sleep(3)
        result = self.execute_on_host(host, f"docker logs {container_name}")
        return self._require_malai_id(result.stdout, f"container {container_name}")

    def _log_docker_container(self, host: str, container_name: str):
        try:
            result = self.execute_on_host(host, f"docker logs {container_name}")
            print(f"[DEBUG] {container_name} output:\n{result.stdout}")
            if result.stderr:
                print(f"[DEBUG] {container_name} errors:\n{result.stderr}")
        except Exception as e:
            print(f"[DEBUG] Failed to get logs for {container_name}: {e}")

    def _run_docker_with_cleanup(self, host: str, container_name: str, run_cmd: str):
        """Run a Docker container, show logs on debug/failure, always cleanup."""
        try:
            self.execute_on_host(host, run_cmd)
            if self.debug:
                self._log_docker_container(host, container_name)
        except subprocess.CalledProcessError as e:
            if self.debug:
                print(f"[DEBUG] Container {container_name} failed with exit code {e.returncode}")
                self._log_docker_container(host, container_name)
            raise
        finally:
            try:
                self.execute_on_host(host, f"docker rm -f {container_name}")
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup container {container_name}: {e}")

    def _weaviate_network_mode(self, service_name: str) -> str:
        if service_name in ('localhost', '127.0.0.1', '::1'):
            return "host"
        return f"container:{service_name}"

    # --- Host execution ---

    def is_localhost(self, host: str) -> bool:
        return host in ('localhost', '127.0.0.1', '::1') or host == subprocess.run(
            ['hostname'], capture_output=True, text=True
        ).stdout.strip()

    def is_k8s_context(self, host: str) -> bool:
        return host and host.startswith('k8s:')

    def get_k8s_context(self, host: str) -> str:
        return host.replace('k8s:', '')

    def execute_on_host(self, host: str, command: str) -> subprocess.CompletedProcess:
        if self.dry_run:
            print(f"[DRY RUN] Would execute on {host}: {command}")
            return subprocess.CompletedProcess(args=[], returncode=0, stdout='', stderr='')

        if self.debug:
            print(f"[DEBUG] Executing on {host}: {command}")

        if self.is_localhost(host):
            result = subprocess.run(command, shell=True, capture_output=True, text=True, check=False)
        else:
            result = subprocess.run(
                ['ssh', host, command],
                capture_output=True,
                text=True,
                check=False
            )

        if self.debug:
            print(f"[DEBUG] Command exit code: {result.returncode}")
            if result.stdout:
                print(f"[DEBUG] stdout: {result.stdout[:500]}")
            if result.stderr:
                print(f"[DEBUG] stderr: {result.stderr[:500]}")

        if result.returncode != 0:
            print(f"  Command failed (exit {result.returncode}) on {host}: {command}")
            if result.stdout:
                print(f"  stdout: {result.stdout[:2000]}")
            if result.stderr:
                print(f"  stderr: {result.stderr[:2000]}")
            raise subprocess.CalledProcessError(
                result.returncode, result.args, result.stdout, result.stderr
            )

        return result

    def execute_kubectl(self, context: str, command: str) -> subprocess.CompletedProcess:
        if self.dry_run:
            print(f"[DRY RUN] Would execute: kubectl --context={context} {command}")
            return subprocess.CompletedProcess(args=[], returncode=0, stdout='', stderr='')

        full_command = f"kubectl --context={context} {command}"
        if self.debug:
            print(f"[DEBUG] Executing kubectl: {full_command}")

        result = subprocess.run(
            full_command,
            shell=True,
            capture_output=True,
            text=True,
            check=False
        )

        if self.debug:
            print(f"[DEBUG] kubectl exit code: {result.returncode}")
            if result.stdout:
                print(f"[DEBUG] stdout: {result.stdout[:500]}")
            if result.stderr:
                print(f"[DEBUG] stderr: {result.stderr[:500]}")

        if result.returncode != 0:
            print(f"  kubectl failed (exit {result.returncode}): {full_command}")
            if result.stdout:
                print(f"  stdout: {result.stdout[:2000]}")
            if result.stderr:
                print(f"  stderr: {result.stderr[:2000]}")
            raise subprocess.CalledProcessError(
                result.returncode, full_command, result.stdout, result.stderr
            )

        return result

    # --- TCP tunnel (Weaviate P2P) ---

    def _start_tcp_tunnel(self, location: Location, unique_id: str) -> dict:
        """Start TCP tunnel for exposing a service via Malai P2P.

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        port = location.port
        service_name = location.service_name

        if self.is_k8s_context(location.host):
            return self._start_tcp_tunnel_k8s(location, unique_id, port, service_name)
        else:
            return self._start_tcp_tunnel_docker(location, unique_id, port, service_name)

    def _start_tcp_tunnel_k8s(self, location, unique_id, port, service_name):
        context = self.get_k8s_context(location.host)

        if '/' in location.host:
            parts = location.host.split('/', 1)
            namespace = parts[0].replace('k8s:', '')
        else:
            namespace = 'default'

        if not service_name:
            raise ValueError("service_name required for K8s weaviate tunneling")

        target_pod = service_name
        ephemeral_container_name = f"malai-{unique_id}"

        get_container_cmd = f"get pod {target_pod} -n {namespace} -o jsonpath='{{.spec.containers[0].name}}'"
        container_result = self.execute_kubectl(context, get_container_cmd)
        target_container = container_result.stdout.strip() or service_name

        print(f"  🚀 Attaching ephemeral tunnel container to pod {target_pod} (container: {target_container})...")
        if self.debug:
            print(f"[DEBUG] Using ephemeral container: {ephemeral_container_name}")
            print(f"[DEBUG] Target container: {target_container}")
            print(f"[DEBUG] Shares network namespace, so localhost:{port} = {service_name}:{port}")

        debug_cmd = (
            f"debug {target_pod} -n {namespace} "
            f"--profile=general "
            f"--image={self.TRANSFER_P2P_IMAGE} "
            f"--target={target_container} "
            f"--container={ephemeral_container_name} "
            f"--env=QUIET=true "
            f"-- /usr/local/bin/entrypoint.sh tcp server {port}"
        )

        if self.debug:
            print(f"[DEBUG] Debug command: kubectl --context={context} {debug_cmd}")

        full_cmd = f"kubectl --context={context} {debug_cmd}"

        if self.dry_run:
            print(f"[DRY RUN] Would execute: {full_cmd}")
            return {
                'type': 'k8s-ephemeral-tunnel',
                'pod': target_pod,
                'container': ephemeral_container_name,
                'namespace': namespace,
                'context': context,
                'malai_id': 'dry-run-id',
                'port': port
            }

        subprocess.Popen(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        print(f"  ⏳ Waiting for Malai P2P tunnel to initialize (30-60s)...")
        time.sleep(10)

        max_attempts = 12
        malai_id = None
        logs_cmd = f"logs {target_pod} -n {namespace} -c {ephemeral_container_name}"

        for attempt in range(max_attempts):
            time.sleep(5)
            try:
                result = self.execute_kubectl(context, logs_cmd)
                malai_id = self._parse_malai_id(result.stdout)
                if malai_id:
                    if self.debug:
                        print(f"[DEBUG] Found Malai ID: {malai_id}")
                    break
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Attempt {attempt+1}/{max_attempts}: {e}")
                continue

        if not malai_id:
            try:
                result = self.execute_kubectl(context, logs_cmd)
                raise Exception(f"Failed to get Malai ID from ephemeral container. Logs: {result.stdout}")
            except Exception:
                raise Exception(f"Failed to get Malai ID from ephemeral container after {max_attempts} attempts")

        if self.debug:
            print(f"[DEBUG] Got Malai ID from ephemeral container: {malai_id}")

        return {
            'type': 'k8s-ephemeral-tunnel',
            'pod': target_pod,
            'container': ephemeral_container_name,
            'namespace': namespace,
            'context': context,
            'malai_id': malai_id,
            'port': port
        }

    def _start_tcp_tunnel_docker(self, location, unique_id, port, service_name):
        container_name = f"tcp-tunnel-{unique_id}"

        if service_name:
            network_mode = f"--network container:{service_name}"
        else:
            network_mode = "--network host"

        start_cmd = (
            f"docker run --pull always -d --name {container_name} "
            f"{network_mode} "
            f"-e QUIET=true "
            f"{self.TRANSFER_P2P_IMAGE} tcp server {port}"
        )

        print(f"  🚀 Starting TCP tunnel (Malai P2P) on {location.host} for {service_name or 'localhost'}:{port}...")
        if self.debug:
            print(f"[DEBUG] Start command: {start_cmd}")
            print(f"[DEBUG] Network mode: {network_mode}")

        malai_id = self._start_docker_get_malai_id(location.host, start_cmd, container_name)

        return {
            'type': 'docker-tcp-tunnel',
            'name': container_name,
            'host': location.host,
            'malai_id': malai_id,
            'port': port
        }

    def _cleanup_tcp_tunnel(self, tunnel_info: dict):
        print(f"  🧹 Cleaning up TCP tunnel...")

        if tunnel_info['type'] == 'docker-tcp-tunnel':
            try:
                self.execute_on_host(tunnel_info['host'], f"docker rm -f {tunnel_info['name']}")
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup tunnel container: {e}")

        elif tunnel_info['type'] == 'k8s-ephemeral-tunnel':
            if self.debug:
                print(f"[DEBUG] Ephemeral container {tunnel_info['container']} will persist in pod {tunnel_info['pod']} until pod deletion")
            print(f"  ℹ️  Ephemeral container will remain in pod metadata (terminated state)")

    # --- Weaviate transfer ---

    def transfer_weaviate_to_weaviate(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Transfer data between Weaviate instances using P2P tunnel.

        Architecture:
        1. Source: Start TCP tunnel exposing source Weaviate via Malai P2P
        2. Target: Run copy container that connects to both via P2P bridge + container network
        """
        print(f"🔄 Transferring Weaviate → Weaviate (P2P): {source.service_name} → {target.service_name}")

        if self.debug:
            print(f"[DEBUG] Source: host={source.host}, service={source.service_name}, port={source.port}, has_auth={bool(source.password)}")
            print(f"[DEBUG] Target: host={target.host}, service={target.service_name}, port={target.port}, has_auth={bool(target.password)}")

        source_unique_id = self._generate_unique_id()
        time.sleep(0.1)
        copy_unique_id = self._generate_unique_id()

        source_tunnel = None
        copy_container_name = f"weaviate-copy-{copy_unique_id}"

        try:
            source_tunnel = self._start_tcp_tunnel(source, source_unique_id)
            source_malai_id = source_tunnel['malai_id']
            print(f"  📡 Source Malai P2P ID: {source_malai_id}")

            source_bridge_port = 18080
            target_local_port = target.port

            copy_args = [
                "--source-url", f"http://localhost:{source_bridge_port}",
                "--target-url", f"http://localhost:{target_local_port}",
            ]

            if source.password:
                copy_args.extend(["--source-auth", source.password])
            if target.password:
                copy_args.extend(["--target-auth", target.password])
            if exclude:
                copy_args.extend(["--exclude", ','.join(exclude)])

            print(f"  🚀 Starting copy container on {target.host}...")

            copy_script = f"""#!/bin/bash
set -e

malai tcp-bridge {source_malai_id} {source_bridge_port} > /tmp/bridge.log 2>&1 &
BRIDGE_PID=$!

sleep 5

echo "=== Checking bridge status ===" >&2
if kill -0 $BRIDGE_PID 2>/dev/null; then
    echo "Bridge process is running (PID: $BRIDGE_PID)" >&2
else
    echo "Bridge process is NOT running!" >&2
fi
netstat -ln | grep 18080 || echo "Port 18080 is NOT listening" >&2
echo "=============================" >&2

echo "=== Bridge log (before copy) ===" >&2
cat /tmp/bridge.log 2>&1 || echo "No bridge log found" >&2
echo "================================" >&2

python3 /usr/local/bin/copy_weaviate.py {' '.join(copy_args)}
COPY_EXIT=$?

kill $BRIDGE_PID 2>/dev/null || true

exit $COPY_EXIT
"""

            network_mode = self._weaviate_network_mode(target.service_name)

            if self.is_k8s_context(target.host):
                raise NotImplementedError("K8s as target for Weaviate copying not yet implemented")
            elif '@' in target.host:
                # Remote Docker host via SSH
                copy_cmd = (
                    f"docker run --pull always --rm --name {copy_container_name} "
                    f"--network {network_mode} "
                    f"--entrypoint bash "
                    f"{self.TRANSFER_P2P_IMAGE} -c {subprocess.list2cmdline([copy_script])}"
                )

                if self.debug:
                    print(f"[DEBUG] Copy command: ssh {target.host} {copy_cmd}")

                result = subprocess.run(
                    ['ssh', target.host, copy_cmd],
                    capture_output=True,
                    text=True,
                    check=False
                )
            else:
                # Local Docker
                copy_cmd = [
                    "docker", "run", "--rm", "--name", copy_container_name,
                    "--network", network_mode,
                    "--entrypoint", "bash",
                    self.TRANSFER_P2P_IMAGE,
                    "-c", copy_script
                ]

                if self.debug:
                    print(f"[DEBUG] Copy command: {' '.join(copy_cmd)}")

                result = subprocess.run(
                    copy_cmd,
                    capture_output=True,
                    text=True,
                    check=False
                )

            if result.stdout:
                print(f"Copy container output:\n{result.stdout}")
            if result.stderr:
                print(f"Copy container stderr:\n{result.stderr}")

            if result.returncode != 0:
                raise Exception(f"Copy container failed with exit code {result.returncode}")

            print("  ✅ Weaviate → Weaviate transfer completed successfully")

        finally:
            if source_tunnel:
                self._cleanup_tcp_tunnel(source_tunnel)

    # --- Rsync daemon (file-based transfers) ---

    def _start_rsync_daemon(self, location: Location, unique_id: str) -> dict:
        """Start rsync daemon container/pod using rsync-p2p (with Malai P2P).

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        if self.dry_run:
            subpath = ''
            if location.scheme in ('docker-volume', 'directory'):
                parts = location.path.split('/', 1)
                subpath = '/' + parts[1] if len(parts) > 1 else ''
            elif location.scheme == 'k8s-pvc':
                parts = location.path.split('/', 2)
                subpath = '/' + parts[2] if len(parts) > 2 else ''
            print(f"[DRY RUN] Would start rsync-p2p daemon on {location.host} for {location.scheme}:{location.path}")
            return {
                'type': 'dry-run',
                'malai_id': 'dry-run-id',
                'subpath': subpath
            }

        if location.scheme in ('docker-volume', 'directory'):
            if location.scheme == 'docker-volume':
                parts = location.path.split('/', 1)
                volume_mount = f"-v {parts[0]}:/data:ro"
                subpath = '/' + parts[1] if len(parts) > 1 else ''
            else:
                volume_mount = f"-v {location.path}:/data:ro"
                subpath = ''

            container_name = f"rsync-daemon-{unique_id}"
            start_cmd = (
                f"docker run --pull always -d --name {container_name} "
                f"{volume_mount} "
                f"-e QUIET=true "
                f"{self.TRANSFER_P2P_IMAGE} rsync server"
            )

            print(f"  🚀 Starting rsync-p2p daemon (Malai P2P) on {location.host}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")

            malai_id = self._start_docker_get_malai_id(location.host, start_cmd, container_name)

            return {
                'type': 'docker-p2p',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'subpath': subpath
            }

        elif location.scheme == 'k8s-pvc':
            parts = location.path.split('/', 2)
            namespace = parts[0]
            pvc = parts[1]
            subpath = '/' + parts[2] if len(parts) > 2 else ''

            pod_name = f"rsync-daemon-{unique_id}"
            context = self.get_k8s_context(location.host)

            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": namespace
                },
                "spec": {
                    "containers": [{
                        "name": "rsync-p2p-server",
                        "image": self.TRANSFER_P2P_IMAGE,
                        "imagePullPolicy": "Always",
                        "args": ["rsync", "server"],
                        "env": [{"name": "QUIET", "value": "true"}],
                        "volumeMounts": [{
                            "name": "data",
                            "mountPath": "/data",
                            "readOnly": True
                        }]
                    }],
                    "volumes": [{
                        "name": "data",
                        "persistentVolumeClaim": {
                            "claimName": pvc
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }

            print(f"  🚀 Starting rsync-p2p daemon pod (Malai P2P) in K8s...")

            self._kubectl_apply_spec(context, pod_spec)

            print(f"  ⏳ Waiting for rsync-p2p daemon pod to start...")
            self.execute_kubectl(
                context,
                f"wait --for=jsonpath='{{.status.phase}}'=Running pod/{pod_name} -n {location.namespace} --timeout=120s",
            )
            time.sleep(2)

            logs_cmd = f"logs {pod_name} -n {namespace}"
            result = self.execute_kubectl(context, logs_cmd)

            malai_id = self._require_malai_id(result.stdout, f"K8s pod {pod_name}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID from K8s pod: {malai_id}")

            return {
                'type': 'k8s-p2p',
                'name': pod_name,
                'namespace': namespace,
                'context': context,
                'malai_id': malai_id,
                'subpath': subpath
            }
        else:
            raise ValueError(f"Unsupported scheme for rsync daemon: {location.scheme}")

    def _get_malai_connection_info(self, daemon_info: dict) -> dict:
        return {
            'malai_id': daemon_info['malai_id'],
            'subpath': daemon_info.get('subpath', '')
        }

    def _run_rsync_client(
        self,
        target: Location,
        malai_connection: dict,
        unique_id: str,
        exclude: Optional[List[str]] = None
    ):
        """Run rsync-p2p client to pull from daemon via Malai P2P."""
        if self.dry_run:
            print(f"[DRY RUN] Would run rsync-p2p client on {target.host} for {target.scheme}:{target.path}")
            return

        malai_id = malai_connection['malai_id']
        source_subpath = malai_connection.get('subpath', '')

        if exclude and self.debug:
            print(f"[DEBUG] Note: Exclusions {exclude} not yet supported with rsync-p2p")

        if target.scheme in ('docker-volume', 'directory'):
            if target.scheme == 'docker-volume':
                parts = target.path.split('/', 1)
                volume_mount = f"-v {parts[0]}:/data"
                target_subpath = '/' + parts[1] if len(parts) > 1 else ''
            else:
                volume_mount = f"-v {target.path}:/data"
                target_subpath = ''

            container_name = f"rsync-client-{unique_id}"
            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = f"/data{target_subpath}/"

            client_cmd = (
                f"docker run --pull always --name {container_name} "
                f"{volume_mount} "
                f"{self.TRANSFER_P2P_IMAGE} rsync client {malai_id} {source_path} {dest_path}"
            )

            print(f"  🔄 Running rsync-p2p client (Malai P2P) on {target.host}...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")
                print(f"[DEBUG] Source subpath: {source_subpath}")
                print(f"[DEBUG] Source path: {source_path}")
                print(f"[DEBUG] Dest path: {dest_path}")
                print(f"[DEBUG] Client command: {client_cmd}")

            self._run_docker_with_cleanup(target.host, container_name, client_cmd)

        elif target.scheme == 'k8s-pvc':
            parts = target.path.split('/', 2)
            namespace = parts[0]
            pvc = parts[1]
            target_subpath = '/' + parts[2] if len(parts) > 2 else ''

            pod_name = f"rsync-client-{unique_id}"
            context = self.get_k8s_context(target.host)

            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = f"/data{target_subpath}/"

            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": namespace
                },
                "spec": {
                    "containers": [{
                        "name": "rsync-p2p-client",
                        "image": self.TRANSFER_P2P_IMAGE,
                        "imagePullPolicy": "Always",
                        "args": ["rsync", "client", malai_id, source_path, dest_path],
                        "volumeMounts": [{
                            "name": "data",
                            "mountPath": "/data"
                        }]
                    }],
                    "volumes": [{
                        "name": "data",
                        "persistentVolumeClaim": {
                            "claimName": pvc
                        }
                    }],
                    "restartPolicy": "Never"
                }
            }

            print(f"  🔄 Running rsync-p2p client pod (Malai P2P) in K8s...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")

            self._kubectl_apply_spec(context, pod_spec)

            print(f"  ⏳ Waiting for rsync-p2p client to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {namespace} --timeout=300s"
            pod_succeeded = False
            try:
                self.execute_kubectl(context, wait_cmd)
                pod_succeeded = True
            except subprocess.CalledProcessError:
                status_cmd = f"get pod/{pod_name} -n {namespace} -o jsonpath='{{.status.phase}}'"
                result = self.execute_kubectl(context, status_cmd)
                pod_phase = result.stdout.strip()
                if pod_phase == 'Succeeded':
                    pod_succeeded = True

            if not pod_succeeded or self.debug:
                logs_cmd = f"logs {pod_name} -n {namespace}"
                try:
                    result = self.execute_kubectl(context, logs_cmd)
                    label = "Rsync-p2p client logs" if not pod_succeeded else "[DEBUG] Rsync-p2p output"
                    print(f"  {label}:\n{result.stdout}")
                    if result.stderr:
                        print(f"  stderr:\n{result.stderr}")
                except Exception as log_err:
                    print(f"  Failed to fetch pod logs: {log_err}")

            delete_cmd = f"delete pod {pod_name} -n {namespace} --force --grace-period=0"
            try:
                self.execute_kubectl(context, delete_cmd)
            except Exception as cleanup_err:
                print(f"  Failed to cleanup rsync client pod: {cleanup_err}")

            if not pod_succeeded:
                raise Exception(f"Rsync client pod failed with status: {pod_phase}")

        else:
            raise ValueError(f"Unsupported scheme for rsync client: {target.scheme}")

    def _cleanup_rsync_daemon(self, daemon_info: dict):
        if daemon_info['type'] == 'dry-run':
            return

        print(f"  🧹 Cleaning up rsync-p2p daemon...")

        if daemon_info['type'] in ('docker-p2p', 'docker'):
            try:
                self.execute_on_host(daemon_info['host'], f"docker rm -f {daemon_info['name']}")
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup daemon container: {e}")

        elif daemon_info['type'] in ('k8s-p2p', 'k8s'):
            if daemon_info.get('port_forward_process'):
                try:
                    daemon_info['port_forward_process'].terminate()
                    daemon_info['port_forward_process'].wait(timeout=5)
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Failed to stop port-forward: {e}")

            delete_cmd = f"delete pod {daemon_info['name']} -n {daemon_info['namespace']} --force --grace-period=0"
            try:
                self.execute_kubectl(daemon_info['context'], delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to delete daemon pod: {e}")

    # --- Postgres transfer ---

    def _create_temp_volume(self, location: Location, unique_id: str) -> str:
        volume_name = f"pg-dump-{unique_id}"

        if self.dry_run:
            if self.is_k8s_context(location.host):
                print(f"[DRY RUN] Would create temporary PVC: default/{volume_name}")
                return f"default/{volume_name}"
            else:
                print(f"[DRY RUN] Would create temporary Docker volume: {volume_name}")
                return volume_name

        if self.is_k8s_context(location.host):
            context = self.get_k8s_context(location.host)
            if '/' in location.host:
                namespace, _ = location.host.split('/', 1)
            else:
                namespace = 'default'

            pvc_name = volume_name

            pvc_spec = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "metadata": {
                    "name": pvc_name,
                    "namespace": namespace
                },
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {
                        "requests": {
                            "storage": "10Gi"
                        }
                    }
                }
            }

            print(f"  📦 Creating temporary PVC for dump...")
            self._kubectl_apply_spec(context, pvc_spec)

            return f"{namespace}/{pvc_name}"
        else:
            print(f"  📦 Creating temporary Docker volume for dump...")
            self.execute_on_host(location.host, f"docker volume create {volume_name}")
            return volume_name

    def _cleanup_temp_volume(self, location: Location, volume_identifier: str):
        if self.dry_run:
            return

        print(f"  🧹 Cleaning up temporary volume...")

        if self.is_k8s_context(location.host):
            context = self.get_k8s_context(location.host)
            namespace, pvc_name = volume_identifier.split('/', 1)
            try:
                self.execute_kubectl(context, f"delete pvc {pvc_name} -n {namespace}")
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup PVC: {e}")
        else:
            try:
                self.execute_on_host(location.host, f"docker volume rm {volume_identifier}")
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup volume: {e}")

    def _pg_dump_to_volume(self, source: Location, volume_identifier: str, unique_id: str):
        if self.dry_run:
            print(f"[DRY RUN] Would run pg_dump from {source.service_name} to volume {volume_identifier}")
            return

        print(f"  📊 Running pg_dump to volume...")

        dump_file = "/dump/database.pgdump"

        if self.is_k8s_context(source.host):
            context = self.get_k8s_context(source.host)
            pod, namespace = self._parse_postgres_service(source.service_name)
            pvc_namespace, pvc_name = volume_identifier.split('/', 1)
            pod_name = f"pg-dump-{unique_id}"

            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": pvc_namespace
                },
                "spec": {
                    "containers": [{
                        "name": "pg-dump",
                        "image": "postgres:alpine",
                        "imagePullPolicy": "Always",
                        "command": ["sh", "-c"],
                        "args": [f"pg_dump -h {pod}.{namespace}.svc.cluster.local -U {source.username} -Fc -f {dump_file} {source.path}"],
                        "env": [{"name": "PGPASSWORD", "value": source.password or ""}],
                        "volumeMounts": [{
                            "name": "dump",
                            "mountPath": "/dump"
                        }]
                    }],
                    "volumes": [{
                        "name": "dump",
                        "persistentVolumeClaim": {"claimName": pvc_name}
                    }],
                    "restartPolicy": "Never"
                }
            }

            self._kubectl_apply_spec(context, pod_spec)

            print(f"  ⏳ Waiting for pg_dump to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {pvc_namespace} --timeout=600s"
            self.execute_kubectl(context, wait_cmd)

            self.execute_kubectl(context, f"delete pod {pod_name} -n {pvc_namespace} --force --grace-period=0")
        else:
            container_name = f"pg-dump-{unique_id}"
            pull_cmd = f"docker pull postgres:alpine"
            self.execute_on_host(source.host, pull_cmd)

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"--network container:{source.service_name} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={source.password or ''} "
                f"postgres:alpine "
                f"pg_dump -h localhost -U {source.username} -Fc -f {dump_file} {source.path}"
            )
            self.execute_on_host(source.host, start_cmd)

            wait_result = self.execute_on_host(source.host, f"docker wait {container_name}")
            exit_code = wait_result.stdout.strip()
            if exit_code != '0':
                logs = self.execute_on_host(source.host, f"docker logs {container_name}")
                self.execute_on_host(source.host, f"docker rm -f {container_name}")
                raise Exception(f"pg_dump container exited with code {exit_code}: {logs.stdout}")
            self.execute_on_host(source.host, f"docker rm -f {container_name}")

        print(f"  ✅ pg_dump completed")

    def _pg_restore_from_volume(self, target: Location, volume_identifier: str, unique_id: str):
        if self.dry_run:
            print(f"[DRY RUN] Would run pg_restore to {target.service_name} from volume {volume_identifier}")
            return

        print(f"  🚀 Running pg_restore from volume...")

        dump_file = "/dump/database.pgdump"

        if self.is_k8s_context(target.host):
            context = self.get_k8s_context(target.host)
            pod, namespace = self._parse_postgres_service(target.service_name)
            pvc_namespace, pvc_name = volume_identifier.split('/', 1)
            pod_name = f"pg-restore-{unique_id}"

            pod_spec = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": pod_name,
                    "namespace": pvc_namespace
                },
                "spec": {
                    "containers": [{
                        "name": "pg-restore",
                        "image": "postgres:alpine",
                        "imagePullPolicy": "Always",
                        "command": ["sh", "-c"],
                        "args": [f"pg_restore -h {pod}.{namespace}.svc.cluster.local -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file} 2>&1 | grep -v 'transaction_timeout' || true"],
                        "env": [{"name": "PGPASSWORD", "value": target.password or ""}],
                        "volumeMounts": [{
                            "name": "dump",
                            "mountPath": "/dump"
                        }]
                    }],
                    "volumes": [{
                        "name": "dump",
                        "persistentVolumeClaim": {"claimName": pvc_name}
                    }],
                    "restartPolicy": "Never"
                }
            }

            self._kubectl_apply_spec(context, pod_spec)

            print(f"  ⏳ Waiting for pg_restore to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {pvc_namespace} --timeout=600s"
            self.execute_kubectl(context, wait_cmd)

            if self.debug:
                logs_cmd = f"logs {pod_name} -n {pvc_namespace}"
                result = self.execute_kubectl(context, logs_cmd)
                print(f"[DEBUG] pg_restore output: {result.stdout}")

            self.execute_kubectl(context, f"delete pod {pod_name} -n {pvc_namespace} --force --grace-period=0")
        else:
            container_name = f"pg-restore-{unique_id}"
            pull_cmd = f"docker pull postgres:alpine"
            self.execute_on_host(target.host, pull_cmd)

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"--network container:{target.service_name} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={target.password or ''} "
                f"postgres:alpine "
                f"sh -c \"pg_restore -h localhost -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file} 2>&1 | grep -v 'transaction_timeout' || true\""
            )
            self.execute_on_host(target.host, start_cmd)

            wait_result = self.execute_on_host(target.host, f"docker wait {container_name}")
            exit_code = wait_result.stdout.strip()
            if exit_code != '0':
                logs = self.execute_on_host(target.host, f"docker logs {container_name}")
                self.execute_on_host(target.host, f"docker rm -f {container_name}")
                raise Exception(f"pg_restore container exited with code {exit_code}: {logs.stdout}")
            self.execute_on_host(target.host, f"docker rm -f {container_name}")

        print(f"  ✅ pg_restore completed")

    # --- Main transfer orchestrators ---

    def transfer_rsync_daemon(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Universal incremental transfer using rsync-p2p with Malai P2P."""
        print(f"🔄 Transferring via rsync-p2p (Malai P2P): {source.scheme}:{source.path} → {target.scheme}:{target.path}")

        unique_id = self._generate_unique_id()

        daemon_info = None
        try:
            daemon_info = self._start_rsync_daemon(source, unique_id)

            malai_connection = self._get_malai_connection_info(daemon_info)
            print(f"  📡 Malai P2P ID: {malai_connection['malai_id']}")

            self._run_rsync_client(target, malai_connection, unique_id, exclude)

            print("  ✅ Rsync-p2p (Malai P2P) transfer completed successfully")

        finally:
            if daemon_info:
                self._cleanup_rsync_daemon(daemon_info)

    def transfer_postgres_to_postgres(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Transfer Postgres database using pg_dump → rsync-p2p → pg_restore."""
        logger.debug("transfer_postgres_to_postgres() started")
        print(f"🔄 Transferring Postgres via rsync-p2p: {source.host}/{source.path} → {target.host}/{target.path}", flush=True)

        logger.debug("Generating unique_id")
        unique_id = self._generate_unique_id()
        logger.debug(f"unique_id={unique_id}")

        source_volume = None
        target_volume = None
        daemon_info = None

        try:
            logger.debug("Creating temp volume for source")
            source_volume = self._create_temp_volume(source, unique_id)
            logger.debug(f"Source volume created: {source_volume}")

            logger.debug("Running pg_dump")
            self._pg_dump_to_volume(source, source_volume, unique_id)
            logger.debug("pg_dump completed")

            target_volume = self._create_temp_volume(target, unique_id)

            if self.is_k8s_context(source.host):
                namespace, pvc_name = source_volume.split('/', 1)
                source_loc = Location.parse(f"k8s-pvc:/{namespace}/{pvc_name}", source.host)
            else:
                source_loc = Location.parse(f"docker-volume:/{source_volume}", source.host)

            daemon_info = self._start_rsync_daemon(source_loc, unique_id)
            malai_connection = self._get_malai_connection_info(daemon_info)
            print(f"  📡 Malai P2P ID: {malai_connection['malai_id']}")

            if self.is_k8s_context(target.host):
                namespace, pvc_name = target_volume.split('/', 1)
                target_loc = Location.parse(f"k8s-pvc:/{namespace}/{pvc_name}", target.host)
            else:
                target_loc = Location.parse(f"docker-volume:/{target_volume}", target.host)

            self._run_rsync_client(target_loc, malai_connection, unique_id, exclude)

            self._pg_restore_from_volume(target, target_volume, unique_id)

            print("  ✅ Postgres rsync-p2p transfer completed successfully")

        finally:
            if daemon_info:
                self._cleanup_rsync_daemon(daemon_info)
            if source_volume:
                self._cleanup_temp_volume(source, source_volume)
            if target_volume:
                self._cleanup_temp_volume(target, target_volume)

    def transfer(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Route transfer to appropriate handler based on source/target types."""
        transfer_key = f"{source.scheme}->{target.scheme}"

        specialized_handlers = {
            'weaviate->weaviate': self.transfer_weaviate_to_weaviate,
            'postgres->postgres': self.transfer_postgres_to_postgres,
        }

        if transfer_key in specialized_handlers:
            specialized_handlers[transfer_key](source, target, exclude)
            return

        if source.scheme in ('docker-volume', 'k8s-pvc', 'directory') and \
           target.scheme in ('docker-volume', 'k8s-pvc', 'directory'):
            self.transfer_rsync_daemon(source, target, exclude)
            return

        raise ValueError(f"Unsupported transfer type: {transfer_key}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('from_host', help='Source host (hostname, user@host, localhost, or k8s:context)')
    parser.add_argument('to_host', help='Target host (hostname, user@host, localhost, or k8s:context)')
    parser.add_argument(
        '--copy',
        action='append',
        dest='copies',
        metavar='SOURCE->TARGET',
        help='Copy specification in URL format (can be specified multiple times)'
    )
    parser.add_argument(
        '--exclude',
        metavar='PATTERNS',
        help='Comma-separated exclusion patterns (e.g., "*.temp,*.log")'
    )
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without doing it')
    parser.add_argument('--debug', action='store_true', help='Print additional debug information')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(levelname)s] %(message)s'
    )

    if not args.copies:
        parser.error("At least one --copy argument is required")

    logger.debug("Parsing exclusions")
    exclusions = args.exclude.split(',') if args.exclude else None

    logger.debug("Creating TransferEngine")
    engine = TransferEngine(args.from_host, args.to_host, dry_run=args.dry_run, debug=args.debug)
    logger.debug("TransferEngine created")

    print("🚀 Data Transfer Tool")
    print(f"📤 From: {args.from_host}")
    print(f"📥 To: {args.to_host}")
    print()

    if args.dry_run:
        print("🔍 DRY RUN MODE")
        print()

    if args.debug:
        print("🐛 DEBUG MODE ENABLED")
        print()

    logger.debug("Starting copy loop")
    for copy_spec in args.copies:
        logger.debug(f"Processing copy_spec: {copy_spec}")
        if '->' not in copy_spec:
            print(f"❌ Error: Invalid copy spec (missing '->'): {copy_spec}")
            sys.exit(1)

        source_url, target_url = copy_spec.split('->', 1)
        logger.debug(f"source_url={source_url}, target_url={target_url}")

        logger.info(f"Processing copy spec: {copy_spec}")
        logger.info(f"Source URL: {source_url.strip()}")
        logger.info(f"Target URL: {target_url.strip()}")

        try:
            logger.debug("Parsing source location")
            source = Location.parse(source_url.strip(), args.from_host)
            logger.debug(f"Source parsed: {source}")

            logger.debug("Parsing target location")
            target = Location.parse(target_url.strip(), args.to_host)
            logger.debug(f"Target parsed: {target}")

            logger.info(f"Parsed source Location: {source}")
            logger.info(f"Parsed target Location: {target}")

            logger.debug("Calling engine.transfer()")
            engine.transfer(source, target, exclusions)
            logger.debug("Transfer completed")
            print()

        except Exception as e:
            print(f"❌ Error: {e}")
            traceback.print_exc()
            sys.exit(1)

    print("✅ All transfers completed successfully!")


if __name__ == '__main__':
    main()
