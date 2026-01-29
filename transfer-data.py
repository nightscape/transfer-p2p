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
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from dataclasses import dataclass
from typing import Optional, List
import json
import logging

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
    service_name: Optional[str] = None  # For weaviate/postgres: actual service/container name

    @classmethod
    def parse(cls, url: str, default_host: Optional[str] = None) -> 'Location':
        """Parse a URL into a Location object."""
        parsed = urlparse(url)

        # Extract path components based on scheme
        if parsed.scheme == 'docker-volume':
            # docker-volume:/volume_name[/path]
            # Single slash, everything is in path
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
            # k8s-pvc:/namespace/pvc-name[/path]
            # Single slash, context comes from default_host (e.g., k8s:orbstack)
            # Format: namespace/pvc-name or namespace/pvc-name/subpath
            path_parts = parsed.path.lstrip('/').split('/', 2)

            if len(path_parts) == 0:
                raise ValueError("k8s-pvc URL must specify at least namespace/pvc-name")
            elif len(path_parts) == 1:
                # Just pvc name, use default namespace
                namespace = 'default'
                pvc = path_parts[0]
                subpath = None
            elif len(path_parts) == 2:
                # namespace/pvc
                namespace = path_parts[0]
                pvc = path_parts[1]
                subpath = None
            else:
                # namespace/pvc/subpath
                namespace = path_parts[0]
                pvc = path_parts[1]
                subpath = '/' + path_parts[2]

            return cls(
                scheme='k8s-pvc',
                host=default_host,  # Will be k8s:context
                path=f"{namespace}/{pvc}{subpath or ''}",
                username=None,
                password=None,
                port=None
            )

        elif parsed.scheme == 'directory':
            # directory://[host/]path
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
            # weaviate://[api-key:KEY@]service-or-container-name[:port]
            # The hostname is the service/container name (e.g., weaviate-0, ai-platform-weaviate-1)
            # The execution context (k8s:orbstack or root@host) comes from default_host
            port = parsed.port
            if port is None:
                # Default port is 80 for http, 443 for https
                port = 80
            return cls(
                scheme='weaviate',
                host=default_host,  # Execution context (k8s:context or ssh-host)
                path=parsed.path or '/',
                username=parsed.username,
                password=parsed.password,
                port=port,
                service_name=parsed.hostname  # Actual service/container name
            )

        elif parsed.scheme == 'postgres':
            # postgres://user:pass@service-name[.namespace]/database
            # The hostname can be:
            #   - pod-name (uses default namespace)
            #   - pod-name.namespace (explicit namespace, K8s DNS style)
            #   - pod-name.namespace.svc.cluster.local (full K8s DNS)
            # The execution context (k8s:orbstack or root@host) comes from default_host
            # We store the service/pod name in service_name and the execution context in host
            database = parsed.path.lstrip('/')
            return cls(
                scheme='postgres',
                host=default_host,  # Execution context (k8s:context or ssh-host)
                path=database,
                username=parsed.username,
                password=parsed.password,
                port=parsed.port or 5432,
                service_name=parsed.hostname  # service-name[.namespace[.svc.cluster.local]]
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

        # Pull latest transfer-p2p image
        self._pull_transfer_image()

    def _build_docker_run_cmd(
        self,
        name: str,
        command_args: list,
        detach: bool = False,
        rm: bool = False,
        network: str = None,
        volumes: list = None,
        env_vars: dict = None,
        entrypoint: str = None,
        extra_args: list = None
    ) -> list:
        """Build a docker run command with support for extra args from environment.

        Args:
            name: Container name
            command_args: Command and its arguments to run in container
            detach: Run in detached mode (-d)
            rm: Remove container after exit (--rm)
            network: Network mode (e.g., 'host', 'container:name')
            volumes: List of volume mounts (e.g., ['/host:/container'])
            env_vars: Dictionary of environment variables
            entrypoint: Override entrypoint
            extra_args: Additional docker run arguments

        Returns:
            List of command parts suitable for subprocess
        """
        import os

        cmd = ["docker", "run"]

        if detach:
            cmd.append("-d")
        if rm:
            cmd.append("--rm")

        cmd.extend(["--name", name])

        if network:
            cmd.extend(["--network", network])

        if volumes:
            for vol in volumes:
                cmd.extend(["-v", vol])

        if env_vars:
            for key, value in env_vars.items():
                cmd.extend(["-e", f"{key}={value}"])

        # Add extra args from environment variable
        extra_docker_args = os.environ.get('TRANSFER_P2P_DOCKER_ARGS', '').strip()
        if extra_docker_args:
            import shlex
            cmd.extend(shlex.split(extra_docker_args))

        # Add any additional args passed directly
        if extra_args:
            cmd.extend(extra_args)

        if entrypoint:
            cmd.extend(["--entrypoint", entrypoint])

        cmd.append(self.TRANSFER_P2P_IMAGE)
        cmd.extend(command_args)

        return cmd

    def is_localhost(self, host: str) -> bool:
        """Check if host is localhost."""
        return host in ('localhost', '127.0.0.1', '::1') or host == subprocess.run(
            ['hostname'], capture_output=True, text=True
        ).stdout.strip()

    def is_k8s_context(self, host: str) -> bool:
        """Check if host is a K8s context."""
        return host and host.startswith('k8s:')

    def get_k8s_context(self, host: str) -> str:
        """Extract K8s context from host."""
        return host.replace('k8s:', '')

    def execute_on_host(self, host: str, command: str) -> subprocess.CompletedProcess:
        """Execute command on host (localhost or remote via SSH)."""
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
        """Execute kubectl command with context."""
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

    def _pull_transfer_image(self):
        """Pull latest transfer-p2p image."""
        if self.dry_run:
            print(f"[DRY RUN] Would pull Docker image: {self.TRANSFER_P2P_IMAGE}")
            return

        print(f"🐳 Pulling latest {self.TRANSFER_P2P_IMAGE}...")
        try:
            result = subprocess.run(
                ["docker", "pull", self.TRANSFER_P2P_IMAGE],
                capture_output=True,
                text=True,
                check=False
            )
            if result.returncode != 0:
                print(f"  ⚠️  Warning: Failed to pull image, will use cached version")
                if self.debug:
                    print(f"[DEBUG] Pull error: {result.stderr}")
            else:
                print(f"  ✅ Image pulled successfully")
        except Exception as e:
            print(f"  ⚠️  Warning: Failed to pull image ({e}), will use cached version")

    def _start_tcp_tunnel(self, location: Location, unique_id: str) -> dict:
        """Start TCP tunnel for exposing a service via Malai P2P.

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        port = location.port
        service_name = location.service_name  # For weaviate: the actual service/container name

        if self.is_k8s_context(location.host):
            # For K8s, use ephemeral debug container attached to the target pod
            # This shares the network namespace - no socat needed!
            context = self.get_k8s_context(location.host)

            # Extract namespace if specified in host
            if '/' in location.host:
                parts = location.host.split('/', 1)
                namespace = parts[0].replace('k8s:', '')
            else:
                namespace = 'default'

            if not service_name:
                raise ValueError("service_name required for K8s weaviate tunneling")

            # Use the service_name as the target pod (e.g., weaviate-0)
            target_pod = service_name
            ephemeral_container_name = f"malai-{unique_id}"

            # Get the actual container name in the pod (not the pod name!)
            # Most StatefulSet pods have a single container with a different name
            get_container_cmd = f"get pod {target_pod} -n {namespace} -o jsonpath='{{.spec.containers[0].name}}'"
            container_result = self.execute_kubectl(context, get_container_cmd)
            target_container = container_result.stdout.strip()

            if not target_container:
                target_container = service_name  # Fallback to pod name

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

            # Run kubectl debug in background
            import subprocess
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

            # Start ephemeral container
            proc = subprocess.Popen(
                full_cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Wait for malai to initialize and output MALAI_ID
            # The tcp server with QUIET=true outputs: MALAI_ID=<id> and PORT=<port>
            print(f"  ⏳ Waiting for Malai P2P tunnel to initialize (30-60s)...")
            import time
            time.sleep(10)  # Give ephemeral container time to start

            # Get logs from ephemeral container
            max_attempts = 12  # 60 seconds total
            malai_id = None

            for attempt in range(max_attempts):
                time.sleep(5)
                logs_cmd = f"logs {target_pod} -n {namespace} -c {ephemeral_container_name}"
                try:
                    result = self.execute_kubectl(context, logs_cmd)

                    for line in result.stdout.splitlines():
                        if line.startswith('MALAI_ID='):
                            malai_id = line.split('=', 1)[1].strip()
                            break

                    if malai_id:
                        if self.debug:
                            print(f"[DEBUG] Found Malai ID: {malai_id}")
                        break
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Attempt {attempt+1}/{max_attempts}: {e}")
                    continue

            if not malai_id:
                # Get final logs for debugging
                try:
                    result = self.execute_kubectl(context, logs_cmd)
                    raise Exception(f"Failed to get Malai ID from ephemeral container. Logs: {result.stdout}")
                except:
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
        else:
            # For Docker, run TCP tunnel container connected to target container's network
            container_name = f"tcp-tunnel-{unique_id}"

            # If service_name is provided (Weaviate container), connect to its network
            if service_name:
                # Connect to the target container's network so we can access it as localhost
                network_mode = f"--network container:{service_name}"
                target_host = "localhost"
            elif self.is_localhost(location.host):
                network_mode = "--network host"
                target_host = "localhost"
            else:
                # For remote hosts, use host network
                network_mode = "--network host"
                target_host = "localhost"

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"{network_mode} "
                f"-e QUIET=true "
                f"{self.TRANSFER_P2P_IMAGE} tcp server {port}"
            )

            print(f"  🚀 Starting TCP tunnel (Malai P2P) on {location.host} for {service_name or 'localhost'}:{port}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")
                print(f"[DEBUG] Network mode: {network_mode}")

            self.execute_on_host(location.host, start_cmd)

            # Wait for container to start
            import time
            time.sleep(3)

            # Get logs to extract MALAI_ID
            logs_cmd = f"docker logs {container_name}"
            result = self.execute_on_host(location.host, logs_cmd)

            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from TCP tunnel. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID: {malai_id}")

            return {
                'type': 'docker-tcp-tunnel',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'port': port
            }

    def _cleanup_tcp_tunnel(self, tunnel_info: dict):
        """Cleanup TCP tunnel resources."""
        print(f"  🧹 Cleaning up TCP tunnel...")

        if tunnel_info['type'] == 'docker-tcp-tunnel':
            cleanup_cmd = f"docker rm -f {tunnel_info['name']}"
            try:
                self.execute_on_host(tunnel_info['host'], cleanup_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup tunnel container: {e}")

        elif tunnel_info['type'] == 'k8s-ephemeral-tunnel':
            # Ephemeral containers can't be removed directly, but we should note this
            # They will be cleaned up when the pod is deleted
            if self.debug:
                print(f"[DEBUG] Ephemeral container {tunnel_info['container']} will persist in pod {tunnel_info['pod']} until pod deletion")
            # Note: We could optionally delete the entire pod if we created it, but for
            # existing pods (like weaviate-0), we leave the ephemeral container metadata
            print(f"  ℹ️  Ephemeral container will remain in pod metadata (terminated state)")

        elif tunnel_info['type'] == 'k8s-tcp-tunnel':
            delete_cmd = f"delete pod {tunnel_info['name']} -n {tunnel_info['namespace']} --force --grace-period=0"
            try:
                self.execute_kubectl(tunnel_info['context'], delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to delete tunnel pod: {e}")

    def transfer_weaviate_to_weaviate(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Transfer data between Weaviate instances using P2P tunnel without localhost.

        Architecture:
        1. Source: Start TCP tunnel exposing source Weaviate via Malai P2P
        2. Target: Run copy container that:
           - Connects to source via Malai bridge (localhost:8080 → source P2P)
           - Connects to target Weaviate via container network (localhost:8081 → target)
           - Runs Python script to copy data between the two

        All data flows directly between source and target hosts via P2P, never through localhost.
        """
        print(f"🔄 Transferring Weaviate → Weaviate (P2P): {source.service_name} → {target.service_name}")

        if self.debug:
            print(f"[DEBUG] Source: host={source.host}, service={source.service_name}, port={source.port}, has_auth={bool(source.password)}")
            print(f"[DEBUG] Target: host={target.host}, service={target.service_name}, port={target.port}, has_auth={bool(target.password)}")

        import subprocess
        import time

        # Generate unique IDs for this transfer
        source_unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]
        time.sleep(0.1)
        copy_unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]

        source_tunnel = None
        copy_container_name = f"weaviate-copy-{copy_unique_id}"

        try:
            # Start TCP tunnel on source to expose Weaviate port via Malai P2P
            source_tunnel = self._start_tcp_tunnel(source, source_unique_id)
            source_malai_id = source_tunnel['malai_id']
            print(f"  📡 Source Malai P2P ID: {source_malai_id}")

            # Build copy command arguments
            # Source will be accessed via Malai bridge on port 18080
            # Target will be accessed via container network namespace on its actual port
            source_bridge_port = 18080
            target_local_port = target.port
            source_actual_port = source.port

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

            # Run copy container on target host
            # Container will:
            # 1. Run malai bridge to connect to source (localhost:8080 → source P2P)
            # 2. Connect to target weaviate via shared network namespace (localhost:target_port → target weaviate)
            # 3. Run copy_weaviate.py to transfer data
            print(f"  🚀 Starting copy container on {target.host}...")

            # Use entrypoint wrapper script that starts bridge then runs copy
            # Important: Bridge listens on source_bridge_port (18080) locally
            #            and connects to the source via P2P
            copy_script = f"""#!/bin/bash
set -e

# Start Malai bridge in background
# Bridge will listen on localhost:{source_bridge_port} and forward to source via P2P
malai tcp-bridge {source_malai_id} {source_bridge_port} > /tmp/bridge.log 2>&1 &
BRIDGE_PID=$!

sleep 5

# Check if bridge is running and listening
echo "=== Checking bridge status ===" >&2
if kill -0 $BRIDGE_PID 2>/dev/null; then
    echo "Bridge process is running (PID: $BRIDGE_PID)" >&2
else
    echo "Bridge process is NOT running!" >&2
fi
netstat -ln | grep 18080 || echo "Port 18080 is NOT listening" >&2
echo "=============================" >&2

# Output bridge log before running copy
echo "=== Bridge log (before copy) ===" >&2
cat /tmp/bridge.log 2>&1 || echo "No bridge log found" >&2
echo "================================" >&2

# Run copy script
python3 /usr/local/bin/copy_weaviate.py {' '.join(copy_args)}
COPY_EXIT=$?

# Cleanup
kill $BRIDGE_PID 2>/dev/null || true

exit $COPY_EXIT
"""

            if target.host.startswith('root@') or '@' in target.host:
                # Remote Docker host - run via SSH
                # Use --entrypoint to bypass the custom entrypoint and run bash directly
                # If target service is localhost, use host network instead of container network
                if target.service_name in ('localhost', '127.0.0.1', '::1'):
                    network_mode = "--network host"
                else:
                    network_mode = f"--network container:{target.service_name}"

                copy_cmd = (
                    f"docker run --rm --name {copy_container_name} "
                    f"{network_mode} "
                    f"--entrypoint bash "
                    f"{self.TRANSFER_P2P_IMAGE} -c {subprocess.list2cmdline([copy_script])}"
                )

                if self.debug:
                    print(f"[DEBUG] Copy command: ssh {target.host} {copy_cmd}")

                # Execute without check=True so we can see the error
                result = subprocess.run(
                    ['ssh', target.host, copy_cmd],
                    capture_output=True,
                    text=True,
                    check=False
                )

                # Always show stdout/stderr for debugging
                if result.stdout:
                    print(f"Copy container output:\n{result.stdout}")
                if result.stderr:
                    print(f"Copy container stderr:\n{result.stderr}")

                if result.returncode != 0:
                    raise Exception(f"Copy container failed with exit code {result.returncode}")

            elif self.is_k8s_context(target.host):
                # K8s context - not yet supported for target (only source)
                raise NotImplementedError("K8s as target for Weaviate copying not yet implemented")

            else:
                # Local Docker
                # If target service is localhost, use host network instead of container network
                if target.service_name in ('localhost', '127.0.0.1', '::1'):
                    network_args = ["--network", "host"]
                else:
                    network_args = ["--network", f"container:{target.service_name}"]

                copy_cmd = [
                    "docker", "run", "--rm", "--name", copy_container_name,
                ] + network_args + [
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

                # Always show stdout/stderr for debugging
                if result.stdout:
                    print(f"Copy container output:\n{result.stdout}")
                if result.stderr:
                    print(f"Copy container stderr:\n{result.stderr}")

                if result.returncode != 0:
                    raise Exception(f"Copy container failed with exit code {result.returncode}")

            print("  ✅ Weaviate → Weaviate transfer completed successfully")

        finally:
            # Cleanup source tunnel
            if source_tunnel:
                self._cleanup_tcp_tunnel(source_tunnel)

    def _start_rsync_daemon(self, location: Location, unique_id: str) -> dict:
        """Start rsync daemon container/pod for a location using rsync-p2p (with Malai P2P).

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        daemon_port = 873

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

        if location.scheme == 'docker-volume':
            # Parse volume name and subpath
            parts = location.path.split('/', 1)
            volume = parts[0]
            subpath = '/' + parts[1] if len(parts) > 1 else ''

            container_name = f"rsync-daemon-{unique_id}"

            # Start rsync-p2p server in quiet mode to get Malai ID
            # Mount volume with subpath if specified
            volume_mount = f"-v {volume}:/data:ro"

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"{volume_mount} "
                f"-e QUIET=true "
                f"{self.TRANSFER_P2P_IMAGE} rsync server"
            )

            print(f"  🚀 Starting rsync-p2p daemon (Malai P2P) on {location.host}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")

            self.execute_on_host(location.host, start_cmd)

            # Wait a moment for container to start and output Malai ID
            import time
            time.sleep(3)

            # Get logs to extract MALAI_ID
            logs_cmd = f"docker logs {container_name}"
            result = self.execute_on_host(location.host, logs_cmd)

            # Parse MALAI_ID from output (format: MALAI_ID=id52abc)
            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from daemon. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID: {malai_id}")

            return {
                'type': 'docker-p2p',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'subpath': subpath
            }

        elif location.scheme == 'k8s-pvc':
            # Parse namespace, PVC, subpath
            parts = location.path.split('/', 2)
            namespace = parts[0]
            pvc = parts[1]
            subpath = '/' + parts[2] if len(parts) > 2 else ''

            pod_name = f"rsync-daemon-{unique_id}"
            context = self.get_k8s_context(location.host)

            # Create pod spec with rsync-p2p server
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

            # Create pod using kubectl apply
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            # Wait for pod to be running (not ready, since server runs continuously)
            print(f"  ⏳ Waiting for rsync-p2p daemon pod to start...")
            import time
            time.sleep(5)  # Give pod time to start and output Malai ID

            # Get logs to extract MALAI_ID
            logs_cmd = f"logs {pod_name} -n {namespace}"
            result = self.execute_kubectl(context, logs_cmd)

            # Parse MALAI_ID from output
            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from K8s pod. Logs: {result.stdout}")

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

        elif location.scheme == 'directory':
            # Mount directory as volume
            container_name = f"rsync-daemon-{unique_id}"

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"-v {location.path}:/data:ro "
                f"-e QUIET=true "
                f"{self.TRANSFER_P2P_IMAGE} rsync server"
            )

            print(f"  🚀 Starting rsync-p2p daemon (Malai P2P) on {location.host}...")
            if self.debug:
                print(f"[DEBUG] Start command: {start_cmd}")

            self.execute_on_host(location.host, start_cmd)

            # Wait for container to start
            import time
            time.sleep(3)

            # Get logs to extract MALAI_ID
            logs_cmd = f"docker logs {container_name}"
            result = self.execute_on_host(location.host, logs_cmd)

            # Parse MALAI_ID
            malai_id = None
            for line in result.stdout.splitlines():
                if line.startswith('MALAI_ID='):
                    malai_id = line.split('=', 1)[1].strip()
                    break

            if not malai_id:
                raise Exception(f"Failed to get Malai ID from daemon. Logs: {result.stdout}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID: {malai_id}")

            return {
                'type': 'docker-p2p',
                'name': container_name,
                'host': location.host,
                'malai_id': malai_id,
                'subpath': ''
            }
        else:
            raise ValueError(f"Unsupported scheme for rsync daemon: {location.scheme}")

    def _get_malai_connection_info(self, daemon_info: dict) -> dict:
        """Get Malai P2P connection info from daemon info.

        Returns dict with 'malai_id' and 'subpath' for P2P client connection.
        """
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
        """Run rsync-p2p client to pull from daemon via Malai P2P.

        Args:
            malai_connection: Dict with 'malai_id' and 'subpath' from daemon
        """
        if self.dry_run:
            print(f"[DRY RUN] Would run rsync-p2p client on {target.host} for {target.scheme}:{target.path}")
            return

        malai_id = malai_connection['malai_id']
        source_subpath = malai_connection.get('subpath', '')

        # Note: rsync-p2p client handles rsync internally, exclusions would need to be
        # implemented in the rsync-p2p wrapper or passed through
        if exclude and self.debug:
            print(f"[DEBUG] Note: Exclusions {exclude} not yet supported with rsync-p2p")

        if target.scheme == 'docker-volume':
            # Parse volume name and subpath
            parts = target.path.split('/', 1)
            volume = parts[0]
            target_subpath = '/' + parts[1] if len(parts) > 1 else ''

            container_name = f"rsync-client-{unique_id}"

            # rsync-p2p client command: client <malai_id> /source/ /dest/
            # Source path: relative to rsync module root (module 'data' points to /data in daemon)
            # So subpath should be the path relative to /data, e.g., "/" for root, "/storage/" for subdir
            # Dest path: /data{target_subpath}/ in client container
            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = f"/data{target_subpath}/"

            client_cmd = (
                f"docker run --name {container_name} "
                f"-v {volume}:/data "
                f"{self.TRANSFER_P2P_IMAGE} rsync client {malai_id} {source_path} {dest_path}"
            )

            print(f"  🔄 Running rsync-p2p client (Malai P2P) on {target.host}...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")
                print(f"[DEBUG] Source subpath: {source_subpath}")
                print(f"[DEBUG] Source path: {source_path}")
                print(f"[DEBUG] Dest path: {dest_path}")
                print(f"[DEBUG] Client command: {client_cmd}")

            try:
                result = self.execute_on_host(target.host, client_cmd)

                # Show logs in debug mode on success
                if self.debug:
                    logs_cmd = f"docker logs {container_name}"
                    try:
                        logs_result = self.execute_on_host(target.host, logs_cmd)
                        print(f"[DEBUG] Rsync-p2p client output:\n{logs_result.stdout}")
                        if logs_result.stderr:
                            print(f"[DEBUG] Rsync-p2p client errors:\n{logs_result.stderr}")
                    except Exception as e:
                        if self.debug:
                            print(f"[DEBUG] Failed to get client logs: {e}")
            except subprocess.CalledProcessError as e:
                # On failure, try to get logs before cleanup
                if self.debug:
                    print(f"[DEBUG] Client command failed with exit code {e.returncode}")
                    logs_cmd = f"docker logs {container_name}"
                    try:
                        logs_result = self.execute_on_host(target.host, logs_cmd)
                        print(f"[DEBUG] Rsync-p2p client output:\n{logs_result.stdout}")
                        if logs_result.stderr:
                            print(f"[DEBUG] Rsync-p2p client errors:\n{logs_result.stderr}")
                    except Exception as log_err:
                        print(f"[DEBUG] Failed to get client logs: {log_err}")
                # Re-raise the original error after showing logs
                raise
            finally:
                # Always cleanup container
                cleanup_cmd = f"docker rm -f {container_name}"
                try:
                    self.execute_on_host(target.host, cleanup_cmd)
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Failed to cleanup container: {e}")

        elif target.scheme == 'k8s-pvc':
            # Parse namespace, PVC, subpath
            parts = target.path.split('/', 2)
            namespace = parts[0]
            pvc = parts[1]
            target_subpath = '/' + parts[2] if len(parts) > 2 else ''

            pod_name = f"rsync-client-{unique_id}"
            context = self.get_k8s_context(target.host)

            # Source path: relative to rsync module root (module 'data' points to /data in daemon)
            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = f"/data{target_subpath}/"

            # Create client pod with rsync-p2p
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

            # Create pod
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            # Wait for pod to complete
            print(f"  ⏳ Waiting for rsync-p2p client to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {namespace} --timeout=300s"
            pod_succeeded = False
            try:
                self.execute_kubectl(context, wait_cmd)
                pod_succeeded = True
            except subprocess.CalledProcessError:
                # Pod might have failed, check its status
                status_cmd = f"get pod/{pod_name} -n {namespace} -o jsonpath='{{.status.phase}}'"
                result = self.execute_kubectl(context, status_cmd)
                pod_phase = result.stdout.strip()
                if pod_phase == 'Succeeded':
                    pod_succeeded = True

            # Always fetch pod logs on failure, or in debug mode on success
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

            # Always delete client pod
            delete_cmd = f"delete pod {pod_name} -n {namespace} --force --grace-period=0"
            try:
                self.execute_kubectl(context, delete_cmd)
            except Exception as cleanup_err:
                print(f"  Failed to cleanup rsync client pod: {cleanup_err}")

            if not pod_succeeded:
                raise Exception(f"Rsync client pod failed with status: {pod_phase}")

        elif target.scheme == 'directory':
            container_name = f"rsync-client-{unique_id}"

            # Source path: relative to rsync module root (module 'data' points to /data in daemon)
            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = "/data/"

            client_cmd = (
                f"docker run --name {container_name} "
                f"-v {target.path}:/data "
                f"{self.TRANSFER_P2P_IMAGE} rsync client {malai_id} {source_path} {dest_path}"
            )

            print(f"  🔄 Running rsync-p2p client (Malai P2P) on {target.host}...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")
                print(f"[DEBUG] Source subpath: {source_subpath}")
                print(f"[DEBUG] Source path: {source_path}")
                print(f"[DEBUG] Dest path: {dest_path}")
                print(f"[DEBUG] Client command: {client_cmd}")

            try:
                result = self.execute_on_host(target.host, client_cmd)

                # Show logs in debug mode on success
                if self.debug:
                    logs_cmd = f"docker logs {container_name}"
                    try:
                        logs_result = self.execute_on_host(target.host, logs_cmd)
                        print(f"[DEBUG] Rsync-p2p client output:\n{logs_result.stdout}")
                        if logs_result.stderr:
                            print(f"[DEBUG] Rsync-p2p client errors:\n{logs_result.stderr}")
                    except Exception as e:
                        if self.debug:
                            print(f"[DEBUG] Failed to get client logs: {e}")
            except subprocess.CalledProcessError as e:
                # On failure, try to get logs before cleanup
                if self.debug:
                    print(f"[DEBUG] Client command failed with exit code {e.returncode}")
                    logs_cmd = f"docker logs {container_name}"
                    try:
                        logs_result = self.execute_on_host(target.host, logs_cmd)
                        print(f"[DEBUG] Rsync-p2p client output:\n{logs_result.stdout}")
                        if logs_result.stderr:
                            print(f"[DEBUG] Rsync-p2p client errors:\n{logs_result.stderr}")
                    except Exception as log_err:
                        print(f"[DEBUG] Failed to get client logs: {log_err}")
                # Re-raise the original error after showing logs
                raise
            finally:
                # Always cleanup container
                cleanup_cmd = f"docker rm -f {container_name}"
                try:
                    self.execute_on_host(target.host, cleanup_cmd)
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Failed to cleanup container: {e}")
        else:
            raise ValueError(f"Unsupported scheme for rsync client: {target.scheme}")

    def _cleanup_rsync_daemon(self, daemon_info: dict):
        """Cleanup rsync daemon resources."""
        if daemon_info['type'] == 'dry-run':
            return

        print(f"  🧹 Cleaning up rsync-p2p daemon...")

        if daemon_info['type'] in ('docker-p2p', 'docker'):
            cleanup_cmd = f"docker rm -f {daemon_info['name']}"
            try:
                self.execute_on_host(daemon_info['host'], cleanup_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup daemon container: {e}")

        elif daemon_info['type'] in ('k8s-p2p', 'k8s'):
            # Stop port-forward
            if daemon_info.get('port_forward_process'):
                try:
                    daemon_info['port_forward_process'].terminate()
                    daemon_info['port_forward_process'].wait(timeout=5)
                except Exception as e:
                    if self.debug:
                        print(f"[DEBUG] Failed to stop port-forward: {e}")

            # Delete pod
            delete_cmd = f"delete pod {daemon_info['name']} -n {daemon_info['namespace']} --force --grace-period=0"
            try:
                self.execute_kubectl(daemon_info['context'], delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to delete daemon pod: {e}")

    def _create_temp_volume(self, location: Location, unique_id: str) -> str:
        """Create a temporary volume for dump/restore operations.

        Returns volume name or path.
        """
        if self.dry_run:
            volume_name = f"pg-dump-{unique_id}"
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

            pvc_name = f"pg-dump-{unique_id}"

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
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pvc_spec, f)
                pvc_file = f.name

            try:
                apply_cmd = f"apply -f {pvc_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pvc_file)

            return f"{namespace}/{pvc_name}"
        else:
            volume_name = f"pg-dump-{unique_id}"
            create_cmd = f"docker volume create {volume_name}"
            print(f"  📦 Creating temporary Docker volume for dump...")
            self.execute_on_host(location.host, create_cmd)
            return volume_name

    def _cleanup_temp_volume(self, location: Location, volume_identifier: str):
        """Cleanup temporary volume."""
        if self.dry_run:
            return

        print(f"  🧹 Cleaning up temporary volume...")

        if self.is_k8s_context(location.host):
            context = self.get_k8s_context(location.host)
            namespace, pvc_name = volume_identifier.split('/', 1)
            delete_cmd = f"delete pvc {pvc_name} -n {namespace}"
            try:
                self.execute_kubectl(context, delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup PVC: {e}")
        else:
            delete_cmd = f"docker volume rm {volume_identifier}"
            try:
                self.execute_on_host(location.host, delete_cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup volume: {e}")

    def _pg_dump_to_volume(self, source: Location, volume_identifier: str, unique_id: str):
        """Run pg_dump and save output to a volume."""
        if self.dry_run:
            print(f"[DRY RUN] Would run pg_dump from {source.service_name} to volume {volume_identifier}")
            return

        print(f"  📊 Running pg_dump to volume...")

        dump_file = "/dump/database.pgdump"

        if self.is_k8s_context(source.host):
            context = self.get_k8s_context(source.host)

            # For postgres, service_name can be:
            #   - pod-name (default namespace)
            #   - pod-name.namespace (explicit namespace)
            #   - pod-name.namespace.svc.cluster.local (full DNS)
            service_parts = source.service_name.split('.')
            pod = service_parts[0]
            if len(service_parts) >= 2 and service_parts[1] not in ('svc', 'cluster', 'local'):
                namespace = service_parts[1]
            else:
                namespace = 'default'

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

            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            print(f"  ⏳ Waiting for pg_dump to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {pvc_namespace} --timeout=600s"
            self.execute_kubectl(context, wait_cmd)

            delete_cmd = f"delete pod {pod_name} -n {pvc_namespace} --force --grace-period=0"
            self.execute_kubectl(context, delete_cmd)
        else:
            # For Docker: service_name contains the container name
            container_name = f"pg-dump-{unique_id}"
            dump_cmd = (
                f"docker run --rm --name {container_name} "
                f"--network container:{source.service_name} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={source.password or ''} "
                f"postgres:alpine "
                f"pg_dump -h localhost -U {source.username} -Fc -f {dump_file} {source.path}"
            )
            self.execute_on_host(source.host, dump_cmd)

        print(f"  ✅ pg_dump completed")

    def _pg_restore_from_volume(self, target: Location, volume_identifier: str, unique_id: str):
        """Run pg_restore from a volume."""
        if self.dry_run:
            print(f"[DRY RUN] Would run pg_restore to {target.service_name} from volume {volume_identifier}")
            return

        print(f"  🚀 Running pg_restore from volume...")

        dump_file = "/dump/database.pgdump"

        if self.is_k8s_context(target.host):
            context = self.get_k8s_context(target.host)

            # For postgres, service_name can be:
            #   - pod-name (default namespace)
            #   - pod-name.namespace (explicit namespace)
            #   - pod-name.namespace.svc.cluster.local (full DNS)
            service_parts = target.service_name.split('.')
            pod = service_parts[0]
            if len(service_parts) >= 2 and service_parts[1] not in ('svc', 'cluster', 'local'):
                namespace = service_parts[1]
            else:
                namespace = 'default'

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

            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                import yaml
                yaml.dump(pod_spec, f)
                pod_file = f.name

            try:
                apply_cmd = f"apply -f {pod_file}"
                self.execute_kubectl(context, apply_cmd)
            finally:
                import os
                os.unlink(pod_file)

            print(f"  ⏳ Waiting for pg_restore to complete...")
            wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{pod_name} -n {pvc_namespace} --timeout=600s"
            self.execute_kubectl(context, wait_cmd)

            if self.debug:
                logs_cmd = f"logs {pod_name} -n {pvc_namespace}"
                result = self.execute_kubectl(context, logs_cmd)
                print(f"[DEBUG] pg_restore output: {result.stdout}")

            delete_cmd = f"delete pod {pod_name} -n {pvc_namespace} --force --grace-period=0"
            self.execute_kubectl(context, delete_cmd)
        else:
            # For Docker: service_name contains the container name
            container_name = f"pg-restore-{unique_id}"
            restore_cmd = (
                f"docker run --rm --name {container_name} "
                f"--network container:{target.service_name} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={target.password or ''} "
                f"postgres:alpine "
                f"sh -c \"pg_restore -h localhost -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file} 2>&1 | grep -v 'transaction_timeout' || true\""
            )
            self.execute_on_host(target.host, restore_cmd)

        print(f"  ✅ pg_restore completed")

    def transfer_rsync_daemon(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Universal incremental transfer using rsync-p2p with Malai P2P.

        This works for all storage types: docker-volume, k8s-pvc, directory.
        The pattern:
        1. Start rsync-p2p daemon on source (read-only) - gets Malai ID
        2. Establish P2P connection using Malai ID (automatic NAT traversal)
        3. Run rsync-p2p client on target to pull data
        4. Cleanup temporary resources
        """
        print(f"🔄 Transferring via rsync-p2p (Malai P2P): {source.scheme}:{source.path} → {target.scheme}:{target.path}")

        # Generate unique ID for this transfer
        unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]

        daemon_info = None
        try:
            # Start rsync-p2p daemon on source
            daemon_info = self._start_rsync_daemon(source, unique_id)

            # Get Malai P2P connection info
            malai_connection = self._get_malai_connection_info(daemon_info)
            print(f"  📡 Malai P2P ID: {malai_connection['malai_id']}")

            # Run rsync-p2p client on target
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
        """Transfer Postgres database using pg_dump → rsync-p2p → pg_restore.

        This method:
        1. Dumps database to a temporary volume on source
        2. Uses rsync-p2p (with Malai P2P) to transfer dump file
        3. Restores database from volume on target
        4. Cleans up temporary resources

        Benefits:
        - NAT traversal via Malai P2P - works across firewalls
        - No intermediate local storage required
        - Resume capability if transfer interrupted
        - Works across isolated networks
        """
        logger.debug("transfer_postgres_to_postgres() started")
        print(f"🔄 Transferring Postgres via rsync-p2p: {source.host}/{source.path} → {target.host}/{target.path}", flush=True)

        logger.debug("Generating unique_id")
        unique_id = subprocess.run(['date', '+%s%N'], capture_output=True, text=True).stdout.strip()[:16]
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

        # Specialized handlers for specific data types
        specialized_handlers = {
            'weaviate->weaviate': self.transfer_weaviate_to_weaviate,
            'postgres->postgres': self.transfer_postgres_to_postgres,
        }

        # Check for specialized handlers first
        if transfer_key in specialized_handlers:
            specialized_handlers[transfer_key](source, target, exclude)
            return

        # Use rsync-p2p (Malai) as universal handler for file-based storage
        # Supports: docker-volume, k8s-pvc, directory (all combinations)
        # Provides P2P connectivity with automatic NAT traversal
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

    # Setup logging based on debug flag
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(levelname)s] %(message)s'
    )

    if not args.copies:
        parser.error("At least one --copy argument is required")

    # Parse exclusions
    logger.debug("Parsing exclusions")
    exclusions = args.exclude.split(',') if args.exclude else None

    # Create transfer engine
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

    # Process each copy
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
            import traceback
            traceback.print_exc()
            sys.exit(1)

    print("✅ All transfers completed successfully!")


if __name__ == '__main__':
    main()
