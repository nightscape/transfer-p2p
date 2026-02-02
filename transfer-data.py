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
  weaviate://[api-key:KEY@]host[:port]         - Weaviate instance (P2P object copy)
  weaviate://host[:port]/docker-volume/VOL     - Weaviate with backup/restore via Docker volume
  weaviate://host[:port]/k8s-pvc/NS/PVC        - Weaviate with backup/restore via K8s PVC
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
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.request
from contextlib import contextmanager
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
    volume_type: Optional[str] = None
    volume_id: Optional[str] = None

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

            volume_type = None
            volume_id = None
            path = parsed.path or '/'
            path_stripped = path.strip('/')
            if path_stripped.startswith('docker-volume/'):
                volume_type = 'docker-volume'
                volume_id = path_stripped[len('docker-volume/'):]
            elif path_stripped.startswith('k8s-pvc/'):
                volume_type = 'k8s-pvc'
                volume_id = path_stripped[len('k8s-pvc/'):]

            return cls(
                scheme='weaviate',
                host=default_host,
                path=path,
                username=parsed.username,
                password=parsed.password,
                port=port,
                service_name=parsed.hostname,
                volume_type=volume_type,
                volume_id=volume_id,
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

    @property
    def subpath(self) -> str:
        if self.scheme == 'docker-volume':
            parts = self.path.split('/', 1)
            return '/' + parts[1] if len(parts) > 1 else ''
        elif self.scheme == 'k8s-pvc':
            parts = self.path.split('/', 2)
            return '/' + parts[2] if len(parts) > 2 else ''
        return ''

    @property
    def namespace(self) -> str:
        assert self.scheme == 'k8s-pvc', f"namespace only valid for k8s-pvc, got {self.scheme}"
        return self.path.split('/', 2)[0]

    @property
    def pvc_name(self) -> str:
        assert self.scheme == 'k8s-pvc', f"pvc_name only valid for k8s-pvc, got {self.scheme}"
        return self.path.split('/', 2)[1]

    @property
    def volume_name(self) -> str:
        assert self.scheme == 'docker-volume', f"volume_name only valid for docker-volume, got {self.scheme}"
        return self.path.split('/', 1)[0]

    def docker_volume_arg(self, mount_path: str = '/data', read_only: bool = False) -> str:
        ro = ':ro' if read_only else ''
        if self.scheme == 'docker-volume':
            return f"-v {self.volume_name}:{mount_path}{ro}"
        elif self.scheme == 'directory':
            return f"-v {self.path}:{mount_path}{ro}"
        raise ValueError(f"docker_volume_arg not supported for {self.scheme}")


class TransferEngine:
    """Handles data transfers between different storage types."""

    TRANSFER_P2P_IMAGE = "ghcr.io/nightscape/transfer-p2p:main"

    def __init__(self, from_host: str, to_host: str, dry_run: bool = False, debug: bool = False, timeout: int = 300):
        self.from_host = from_host
        self.to_host = to_host
        self.dry_run = dry_run
        self.debug = debug
        self.timeout = timeout

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

    def _find_selinux_level_for_pvc(self, context: str, namespace: str, pvc_name: str) -> Optional[str]:
        """Find the SELinux level of a running pod that mounts a given PVC.

        On clusters with SELinux enforcing, any new pod mounting a PVC triggers
        volume relabeling. If the new pod's SELinux level differs from the
        existing pod's level, the existing pod loses access to its files.
        By matching the SELinux level, we avoid this problem.
        """
        result = self.execute_kubectl(
            context,
            f"get pods -n {namespace} -o json"
        )
        pods = json.loads(result.stdout)
        for pod in pods.get("items", []):
            if pod.get("status", {}).get("phase") != "Running":
                continue
            for volume in pod.get("spec", {}).get("volumes", []):
                claim = volume.get("persistentVolumeClaim", {})
                if claim.get("claimName") == pvc_name:
                    pod_name = pod["metadata"]["name"]
                    try:
                        result = self.execute_kubectl(
                            context,
                            f"exec {pod_name} -n {namespace} -- cat /proc/1/attr/current"
                        )
                        # Format: system_u:system_r:container_t:s0:c749,c910
                        context_str = result.stdout.strip().rstrip('\x00')
                        parts = context_str.split(':')
                        if len(parts) >= 4:
                            level = ':'.join(parts[3:])
                            if self.debug:
                                print(f"[DEBUG] SELinux level for PVC {pvc_name} (from pod {pod_name}): {level}")
                            return level
                    except (subprocess.CalledProcessError, Exception) as e:
                        if self.debug:
                            print(f"[DEBUG] Failed to get SELinux level from pod {pod_name}: {e}")
                        continue
        return None

    def _find_node_for_pvc(self, context: str, namespace: str, pvc_name: str) -> Optional[str]:
        """Find the node where a PVC is currently mounted by querying running pods."""
        result = self.execute_kubectl(
            context,
            f"get pods -n {namespace} -o json"
        )
        pods = json.loads(result.stdout)
        for pod in pods.get("items", []):
            if pod.get("status", {}).get("phase") != "Running":
                continue
            for volume in pod.get("spec", {}).get("volumes", []):
                claim = volume.get("persistentVolumeClaim", {})
                if claim.get("claimName") == pvc_name:
                    node = pod.get("spec", {}).get("nodeName")
                    if node:
                        if self.debug:
                            print(f"[DEBUG] PVC {pvc_name} is mounted by pod {pod['metadata']['name']} on node {node}")
                        return node
        return None

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
            self.execute_on_host(host, run_cmd, stream=self.debug)
        except subprocess.CalledProcessError as e:
            if self.debug:
                print(f"[DEBUG] Container {container_name} failed with exit code {e.returncode}")
            raise
        finally:
            try:
                self.execute_on_host(host, f"docker rm -f {container_name}")
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Failed to cleanup container {container_name}: {e}")

    def _run_docker_to_completion(self, host: str, container_name: str, run_cmd: str):
        """Start a detached container, wait for exit, raise on failure, cleanup."""
        self.execute_on_host(host, run_cmd)
        wait_result = self.execute_on_host(host, f"docker wait {container_name}")
        exit_code = wait_result.stdout.strip()
        if exit_code != '0':
            logs = self.execute_on_host(host, f"docker logs {container_name}")
            self.execute_on_host(host, f"docker rm -f {container_name}")
            raise Exception(f"Container {container_name} exited with code {exit_code}: {logs.stdout}")
        self.execute_on_host(host, f"docker rm -f {container_name}")

    def _make_pvc_pod_spec(
        self, pod_name: str, namespace: str, pvc_name: str,
        container_name: str, image: str,
        command: Optional[List[str]] = None, args: Optional[List[str]] = None,
        env: Optional[List[dict]] = None, mount_path: str = '/data',
        read_only: bool = False, node_name: Optional[str] = None,
        se_linux_level: Optional[str] = None,
    ) -> dict:
        container = {
            "name": container_name,
            "image": image,
            "imagePullPolicy": "Always",
            "volumeMounts": [{"name": "data", "mountPath": mount_path}]
        }
        if read_only:
            container["volumeMounts"][0]["readOnly"] = True
        if command:
            container["command"] = command
        if args:
            container["args"] = args
        if env:
            container["env"] = env
        spec = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": namespace},
            "spec": {
                "containers": [container],
                "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": pvc_name}}],
                "restartPolicy": "Never"
            }
        }
        if node_name:
            spec["spec"]["nodeName"] = node_name
        if se_linux_level:
            spec["spec"]["securityContext"] = {
                "seLinuxOptions": {"level": se_linux_level}
            }
        return spec

    def _run_k8s_pod_to_completion(self, context: str, pod_spec: dict, timeout: Optional[int] = None) -> Optional[str]:
        """Apply pod, wait for Succeeded, return logs, cleanup. Raises on failure."""
        if timeout is None:
            timeout = self.timeout
        name = pod_spec['metadata']['name']
        ns = pod_spec['metadata']['namespace']

        self._kubectl_apply_spec(context, pod_spec)

        if self.debug:
            # Wait for container to start before streaming logs (avoids ContainerCreating race)
            try:
                self.execute_kubectl(context, f"wait --for=jsonpath='{{.status.phase}}'=Running pod/{name} -n {ns} --timeout=120s")
            except subprocess.CalledProcessError:
                pass  # Pod may have already completed or failed to start
            stream_cmd = f"kubectl --context={context} logs -f {name} -n {ns}"
            subprocess.run(stream_cmd, shell=True, check=False)

        wait_cmd = f"wait --for=jsonpath='{{.status.phase}}'=Succeeded pod/{name} -n {ns} --timeout={timeout}s"
        succeeded = False
        try:
            self.execute_kubectl(context, wait_cmd)
            succeeded = True
        except subprocess.CalledProcessError:
            result = self.execute_kubectl(context, f"get pod/{name} -n {ns} -o jsonpath='{{.status.phase}}'")
            if result.stdout.strip().strip("'") == 'Succeeded':
                succeeded = True

        logs = None
        if not succeeded:
            try:
                result = self.execute_kubectl(context, f"logs {name} -n {ns}")
                logs = result.stdout
                print(f"  Pod {name} output:\n{logs}")
                if result.stderr:
                    print(f"  stderr:\n{result.stderr}")
            except Exception as e:
                print(f"  Failed to fetch pod logs: {e}")

        try:
            self.execute_kubectl(context, f"delete pod {name} -n {ns} --force --grace-period=0")
        except Exception as e:
            if self.debug:
                print(f"[DEBUG] Failed to delete pod {name}: {e}")

        if not succeeded:
            raise Exception(f"Pod {name} failed. Logs: {logs}")

        return logs

    def _weaviate_network_mode(self, service_name: str) -> str:
        if service_name in ('localhost', '127.0.0.1', '::1'):
            return "host"
        return f"container:{service_name}"

    # --- Weaviate backup/restore API infrastructure ---

    def _find_free_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    def _resolve_weaviate_k8s_namespace(self, location: Location) -> str:
        assert location.volume_type == 'k8s-pvc'
        parts = location.volume_id.split('/', 1)
        assert len(parts) == 2, f"Expected namespace/pvc-name in volume_id, got: {location.volume_id}"
        return parts[0]

    def _fetch_k8s_weaviate_api_key(self, context: str, namespace: str) -> Optional[str]:
        import base64
        for key_name in ('key', 'api-key', 'apikey'):
            try:
                result = self.execute_kubectl(
                    context,
                    f"get secret weaviate-api-key -n {namespace} -o jsonpath='{{.data.{key_name}}}'"
                )
                encoded = result.stdout.strip().strip("'")
                if encoded:
                    decoded = base64.b64decode(encoded).decode()
                    if self.debug:
                        print(f"[DEBUG] Found K8s Weaviate API key in secret field '{key_name}'")
                    return decoded
            except subprocess.CalledProcessError:
                continue
        return None

    def _resolve_docker_container_name(self, host: str, service_name: str) -> str:
        """Resolve a compose service name to the actual Docker container name.

        Tries compose service label first, falls back to name filter.
        """
        cmd = f"docker ps --filter label=com.docker.compose.service={service_name} --format '{{{{.Names}}}}'"
        result = self.execute_on_host(host, cmd)
        names = [n for n in result.stdout.strip().splitlines() if n]
        if not names:
            cmd = f"docker ps --filter name={service_name} --format '{{{{.Names}}}}'"
            result = self.execute_on_host(host, cmd)
            names = [n for n in result.stdout.strip().splitlines() if n]
        assert len(names) == 1, f"Expected exactly 1 container matching '{service_name}', got {len(names)}: {names}"
        return names[0]

    def _fetch_docker_weaviate_api_key(self, host: str, container_name: str) -> Optional[str]:
        """Fetch AUTHENTICATION_APIKEY_ALLOWED_KEYS from a running Weaviate Docker container."""
        cmd = f"docker exec {container_name} printenv AUTHENTICATION_APIKEY_ALLOWED_KEYS"
        try:
            result = self.execute_on_host(host, cmd)
            keys = result.stdout.strip()
            if keys:
                return keys.split(',')[0]
        except subprocess.CalledProcessError:
            pass
        return None

    @contextmanager
    def _docker_weaviate_api(self, location: Location):
        """Context manager yielding an api_call(method, endpoint, body=None) function for Docker-hosted Weaviate."""
        service_name = location.service_name
        port = location.port
        host = location.host
        unique_id = self._generate_unique_id()

        if not self.dry_run:
            docker_container = self._resolve_docker_container_name(host, service_name)
            api_key = self._fetch_docker_weaviate_api_key(host, docker_container)
            if self.debug:
                print(f"[DEBUG] Resolved Docker container: {docker_container}, has_api_key={bool(api_key)}")
        else:
            docker_container = service_name
            api_key = None

        def api_call(method: str, endpoint: str, body: dict = None) -> dict:
            url = f"http://localhost:{port}{endpoint}"
            curl_args = ["-s", "-X", method, url, "-H", "Content-Type: application/json"]
            if api_key:
                curl_args.extend(["-H", f"Authorization: Bearer {api_key}"])
            if body is not None:
                body_json = json.dumps(body)
                curl_args.extend(["-d", body_json])

            container_name = f"weaviate-api-{unique_id}-{self._generate_unique_id()}"
            quoted_curl_args = ' '.join(shlex.quote(a) for a in curl_args)
            cmd = (
                f"docker run --rm --name {container_name} "
                f"--network container:{docker_container} "
                f"curlimages/curl {quoted_curl_args}"
            )

            if self.dry_run:
                print(f"[DRY RUN] Would execute on {host}: {cmd}")
                return {}

            if self.debug:
                print(f"[DEBUG] Weaviate API {method} {endpoint} via Docker curl")

            result = self.execute_on_host(host, cmd)
            if not result.stdout.strip():
                return {}
            return json.loads(result.stdout)

        yield api_call

    @contextmanager
    def _k8s_weaviate_api(self, location: Location):
        """Context manager yielding an api_call(method, endpoint, body=None) function for K8s-hosted Weaviate."""
        context = self.get_k8s_context(location.host)
        namespace = self._resolve_weaviate_k8s_namespace(location)
        api_key = self._fetch_k8s_weaviate_api_key(context, namespace)

        local_port = self._find_free_port()

        svc_name = location.service_name
        svc_port = location.port
        fwd_cmd = f"kubectl --context={context} port-forward -n {namespace} svc/{svc_name} {local_port}:{svc_port}"

        if self.dry_run:
            def api_call(method: str, endpoint: str, body: dict = None) -> dict:
                print(f"[DRY RUN] Would call Weaviate API: {method} {endpoint}")
                return {}
            yield api_call
            return

        if self.debug:
            print(f"[DEBUG] Starting port-forward: {fwd_cmd}")

        proc = subprocess.Popen(fwd_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(3)

        try:
            def api_call(method: str, endpoint: str, body: dict = None) -> dict:
                url = f"http://localhost:{local_port}{endpoint}"
                data = json.dumps(body).encode() if body is not None else None
                req = urllib.request.Request(url, data=data, method=method)
                req.add_header("Content-Type", "application/json")
                if api_key:
                    req.add_header("Authorization", f"Bearer {api_key}")

                if self.debug:
                    print(f"[DEBUG] Weaviate API {method} {endpoint} via port-forward :{local_port}, has_api_key={bool(api_key)}")

                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        resp_body = resp.read().decode()
                        if not resp_body.strip():
                            return {}
                        return json.loads(resp_body)
                except urllib.error.HTTPError as e:
                    error_body = e.read().decode() if e.fp else ""
                    raise Exception(f"Weaviate API {method} {endpoint} returned HTTP {e.code}: {error_body}") from e

            yield api_call
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    @contextmanager
    def _weaviate_api_context(self, location: Location):
        """Dispatch to Docker or K8s Weaviate API context manager."""
        assert location.volume_type in ('docker-volume', 'k8s-pvc'), \
            f"Cannot create Weaviate API context without volume_type, got: {location.volume_type}"

        if location.volume_type == 'k8s-pvc':
            with self._k8s_weaviate_api(location) as api_call:
                yield api_call
        else:
            with self._docker_weaviate_api(location) as api_call:
                yield api_call

    # --- Host execution ---

    def is_localhost(self, host: str) -> bool:
        return host in ('localhost', '127.0.0.1', '::1') or host == subprocess.run(
            ['hostname'], capture_output=True, text=True
        ).stdout.strip()

    def is_k8s_context(self, host: str) -> bool:
        return host and host.startswith('k8s:')

    def get_k8s_context(self, host: str) -> str:
        return host.replace('k8s:', '')

    def execute_on_host(self, host: str, command: str, stream: bool = False) -> subprocess.CompletedProcess:
        if self.dry_run:
            print(f"[DRY RUN] Would execute on {host}: {command}")
            return subprocess.CompletedProcess(args=[], returncode=0, stdout='', stderr='')

        if self.debug:
            print(f"[DEBUG] Executing on {host}: {command}")

        is_ssh = not self.is_localhost(host)
        max_attempts = 5 if is_ssh else 1

        for attempt in range(1, max_attempts + 1):
            capture_kwargs = {} if stream else {"capture_output": True, "text": True}
            if self.is_localhost(host):
                result = subprocess.run(command, shell=True, check=False, **capture_kwargs)
            else:
                result = subprocess.run(
                    ['ssh', host, command],
                    check=False,
                    **capture_kwargs
                )

            if self.debug and not stream:
                print(f"[DEBUG] Command exit code: {result.returncode}")
                if result.stdout:
                    print(f"[DEBUG] stdout: {result.stdout[:500]}")
                if result.stderr:
                    print(f"[DEBUG] stderr: {result.stderr[:500]}")

            if result.returncode == 0:
                return result

            # SSH exit code 255 = connection failure (transient, worth retrying)
            if is_ssh and result.returncode == 255 and attempt < max_attempts:
                delay = 2 ** attempt
                print(f"  SSH connection failed (attempt {attempt}/{max_attempts}), retrying in {delay}s...")
                time.sleep(delay)
                continue

            print(f"  Command failed (exit {result.returncode}) on {host}: {command}")
            if not stream:
                if result.stdout:
                    print(f"  stdout: {result.stdout[:2000]}")
                if result.stderr:
                    print(f"  stderr: {result.stderr[:2000]}")
            raise subprocess.CalledProcessError(
                result.returncode, result.args, result.stdout, result.stderr
            )

        assert False, "unreachable"

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

    # --- Weaviate backup/restore transfer ---

    def _weaviate_poll_status(self, api_fn, endpoint: str, timeout: Optional[int] = None) -> str:
        """Poll a Weaviate backup status endpoint until SUCCESS or FAILED."""
        if timeout is None:
            timeout = self.timeout
        if self.dry_run:
            print(f"[DRY RUN] Would poll {endpoint} until SUCCESS")
            return "SUCCESS"

        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = api_fn("GET", endpoint)
            status = resp.get("status", "UNKNOWN")
            if self.debug:
                print(f"[DEBUG] Poll {endpoint}: status={status}")
            if status == "SUCCESS":
                return status
            if status == "FAILED":
                raise Exception(f"Weaviate backup operation failed: {resp}")
            time.sleep(5)
        raise Exception(f"Weaviate backup operation timed out after {timeout}s on {endpoint}")

    def _make_volume_location(self, weaviate_loc: Location, subpath: str) -> Location:
        """Convert a weaviate Location to a file-based Location for rsync of backup files."""
        assert weaviate_loc.volume_type in ('docker-volume', 'k8s-pvc')

        if weaviate_loc.volume_type == 'docker-volume':
            return Location(
                scheme='docker-volume',
                host=weaviate_loc.host,
                path=f"{weaviate_loc.volume_id}/{subpath}",
                username=None,
                password=None,
                port=None,
            )
        else:
            ns, pvc = weaviate_loc.volume_id.split('/', 1)
            return Location(
                scheme='k8s-pvc',
                host=weaviate_loc.host,
                path=f"{ns}/{pvc}/{subpath}",
                username=None,
                password=None,
                port=None,
            )

    def _cleanup_weaviate_backup(self, location: Location, backup_id: str):
        """Remove backup files from the Weaviate data volume (best-effort)."""
        backup_path = f"backups/{backup_id}"

        if location.volume_type == 'docker-volume':
            container_name = f"weaviate-cleanup-{self._generate_unique_id()}"
            cmd = (
                f"docker run --rm --name {container_name} "
                f"-v {location.volume_id}:/data "
                f"alpine rm -rf /data/{backup_path}"
            )
            try:
                self.execute_on_host(location.host, cmd)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Backup cleanup failed on {location.host}: {e}")

        elif location.volume_type == 'k8s-pvc':
            context = self.get_k8s_context(location.host)
            ns, pvc = location.volume_id.split('/', 1)
            pod_name = f"weaviate-cleanup-{self._generate_unique_id()}"
            se_linux_level = self._find_selinux_level_for_pvc(context, ns, pvc)

            pod_spec = self._make_pvc_pod_spec(
                pod_name, ns, pvc,
                container_name="cleanup",
                image="alpine",
                command=["rm", "-rf", f"/data/{backup_path}"],
                se_linux_level=se_linux_level,
            )
            try:
                self._run_k8s_pod_to_completion(context, pod_spec, timeout=60)
            except Exception as e:
                if self.debug:
                    print(f"[DEBUG] Backup cleanup failed on K8s: {e}")

    def _get_weaviate_node_names(self, api_fn) -> List[str]:
        """Get node names from a Weaviate instance via its API."""
        resp = api_fn("GET", "/v1/nodes")
        if not resp:
            return []
        return [n["name"] for n in resp.get("nodes", [])]

    def _build_node_mapping(self, src_api_fn, tgt_api_fn) -> Optional[dict]:
        """Build a node_mapping dict for restore if source and target node names differ."""
        src_nodes = self._get_weaviate_node_names(src_api_fn)
        tgt_nodes = self._get_weaviate_node_names(tgt_api_fn)
        if not src_nodes or not tgt_nodes:
            return None
        if src_nodes == tgt_nodes:
            return None
        assert len(src_nodes) == len(tgt_nodes), (
            f"Source has {len(src_nodes)} nodes but target has {len(tgt_nodes)}. "
            f"Cross-cluster restore with different replica counts is not supported."
        )
        mapping = dict(zip(src_nodes, tgt_nodes))
        print(f"  🔀 Node mapping: {mapping}")
        return mapping

    def _rewrite_backup_node_names(self, location: Location, backup_id: str, node_mapping: dict):
        """Rewrite node names in backup files on the target volume.

        Weaviate's node_mapping restore parameter doesn't reliably remap shards
        in all versions. This directly renames node directories and rewrites
        backup_config.json to use the target node names.
        """
        # Rewrite all JSON files in the backup directory and rename node directories.
        # The node name appears in backup_config.json, backup.json (per-node), and
        # potentially other metadata files.
        sed_commands = []
        rename_commands = []
        for old_name, new_name in node_mapping.items():
            sed_commands.append(
                f"find /data/backups/{backup_id} -name '*.json' -exec "
                f"sed -i 's/{old_name}/{new_name}/g' {{}} +"
            )
            rename_commands.append(
                f'[ -d "/data/backups/{backup_id}/{old_name}" ] && '
                f'mv "/data/backups/{backup_id}/{old_name}" "/data/backups/{backup_id}/{new_name}"'
            )

        rewrite_script = " && ".join(sed_commands + rename_commands)

        if location.volume_type == 'docker-volume':
            container_name = f"backup-rewrite-{self._generate_unique_id()}"
            cmd = (
                f"docker run --rm --name {container_name} "
                f"-v {location.volume_id}:/data "
                f"alpine sh -c '{rewrite_script}'"
            )
            self.execute_on_host(location.host, cmd)

        elif location.volume_type == 'k8s-pvc':
            context = self.get_k8s_context(location.host)
            ns, pvc = location.volume_id.split('/', 1)
            pod_name = f"backup-rewrite-{self._generate_unique_id()}"
            se_linux_level = self._find_selinux_level_for_pvc(context, ns, pvc)

            pod_spec = self._make_pvc_pod_spec(
                pod_name, ns, pvc,
                container_name="rewrite",
                image="alpine",
                command=["sh", "-c", rewrite_script],
                se_linux_level=se_linux_level,
            )
            self._run_k8s_pod_to_completion(context, pod_spec, timeout=60)

    def transfer_weaviate_backup_restore(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None
    ):
        """Transfer Weaviate data using backup API → rsync → restore API.

        This avoids copying Raft consensus metadata which causes crashes when
        the target node has a different identity.
        """
        backup_id = f"transfer-{self._generate_unique_id()}"
        print(f"🔄 Transferring Weaviate via backup/restore: {source.service_name} → {target.service_name}")
        print(f"  📋 Backup ID: {backup_id}")

        if self.debug:
            print(f"[DEBUG] Source: host={source.host}, volume_type={source.volume_type}, volume_id={source.volume_id}")
            print(f"[DEBUG] Target: host={target.host}, volume_type={target.volume_type}, volume_id={target.volume_id}")

        # Phase 0: Detect node mapping (before backup, while both are accessible)
        node_mapping = None
        with self._weaviate_api_context(source) as src_api:
            with self._weaviate_api_context(target) as tgt_api:
                node_mapping = self._build_node_mapping(src_api, tgt_api)

        # Phase 1: Create backup on source
        print(f"  📦 Phase 1: Creating backup on source...")
        with self._weaviate_api_context(source) as src_api:
            src_api("POST", "/v1/backups/filesystem", {"id": backup_id})
            self._weaviate_poll_status(src_api, f"/v1/backups/filesystem/{backup_id}")
        print(f"  ✅ Backup created on source")

        try:
            # Phase 2: Transfer backup files via rsync
            print(f"  🔄 Phase 2: Transferring backup files...")
            source_loc = self._make_volume_location(source, f"backups/{backup_id}")
            target_loc = self._make_volume_location(target, f"backups/{backup_id}")
            self.transfer_rsync_daemon(
                source_loc, target_loc, exclude=None,
                post_sync_command=f"chmod -R a+rX /data/backups/{backup_id}",
            )
            print(f"  ✅ Backup files transferred")

            # Phase 3: Restore on target
            print(f"  🚀 Phase 3: Restoring on target...")
            restore_body = {}
            if node_mapping:
                restore_body["node_mapping"] = node_mapping
            with self._weaviate_api_context(target) as tgt_api:
                # Delete existing classes that conflict with the backup
                schema = tgt_api("GET", "/v1/schema")
                existing_classes = [c["class"] for c in schema.get("classes", [])] if schema else []
                if existing_classes:
                    print(f"  🗑️  Deleting existing classes on target: {existing_classes}")
                    for cls_name in existing_classes:
                        tgt_api("DELETE", f"/v1/schema/{cls_name}")
                    print(f"  ✅ Existing classes deleted")

                tgt_api("POST", f"/v1/backups/filesystem/{backup_id}/restore", restore_body)
                self._weaviate_poll_status(tgt_api, f"/v1/backups/filesystem/{backup_id}/restore")
            print(f"  ✅ Restore completed on target")

        finally:
            # Phase 4: Cleanup (best-effort)
            print(f"  🧹 Phase 4: Cleaning up backup files...")
            if not self.dry_run:
                self._cleanup_weaviate_backup(source, backup_id)
                self._cleanup_weaviate_backup(target, backup_id)
            print(f"  ✅ Cleanup done")

        print("  ✅ Weaviate backup/restore transfer completed successfully")

    # --- Rsync daemon (file-based transfers) ---

    def _start_rsync_daemon(self, location: Location, unique_id: str) -> dict:
        """Start rsync daemon container/pod using rsync-p2p (with Malai P2P).

        Returns dict with cleanup info and 'malai_id' for P2P connection.
        """
        if self.dry_run:
            print(f"[DRY RUN] Would start rsync-p2p daemon on {location.host} for {location.scheme}:{location.path}")
            return {
                'type': 'dry-run',
                'malai_id': 'dry-run-id',
                'subpath': location.subpath
            }

        if location.scheme in ('docker-volume', 'directory'):
            volume_mount = location.docker_volume_arg('/data', read_only=True)

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
                'subpath': location.subpath
            }

        elif location.scheme == 'k8s-pvc':
            pod_name = f"rsync-daemon-{unique_id}"
            context = self.get_k8s_context(location.host)

            node = self._find_node_for_pvc(context, location.namespace, location.pvc_name)
            se_linux_level = self._find_selinux_level_for_pvc(context, location.namespace, location.pvc_name)
            pod_spec = self._make_pvc_pod_spec(
                pod_name, location.namespace, location.pvc_name,
                container_name="rsync-p2p-server",
                image=self.TRANSFER_P2P_IMAGE,
                args=["rsync", "server"],
                env=[{"name": "QUIET", "value": "true"}],
                read_only=True,
                node_name=node,
                se_linux_level=se_linux_level,
            )

            if node:
                print(f"  📌 Pinning daemon pod to node {node} (co-locating with PVC {location.pvc_name})")
            if se_linux_level:
                print(f"  🔒 Using SELinux level {se_linux_level} (matching existing pod on PVC)")

            print(f"  🚀 Starting rsync-p2p daemon pod (Malai P2P) in K8s...")

            self._kubectl_apply_spec(context, pod_spec)

            print(f"  ⏳ Waiting for rsync-p2p daemon pod to start...")
            self.execute_kubectl(
                context,
                f"wait --for=jsonpath='{{.status.phase}}'=Running pod/{pod_name} -n {location.namespace} --timeout=120s",
            )
            time.sleep(2)

            result = self.execute_kubectl(context, f"logs {pod_name} -n {location.namespace}")
            malai_id = self._require_malai_id(result.stdout, f"K8s pod {pod_name}")

            if self.debug:
                print(f"[DEBUG] Got Malai ID from K8s pod: {malai_id}")

            return {
                'type': 'k8s-p2p',
                'name': pod_name,
                'namespace': location.namespace,
                'context': context,
                'malai_id': malai_id,
                'subpath': location.subpath
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
        exclude: Optional[List[str]] = None,
        post_sync_command: Optional[str] = None,
    ):
        """Run rsync-p2p client to pull from daemon via Malai P2P."""
        if self.dry_run:
            print(f"[DRY RUN] Would run rsync-p2p client on {target.host} for {target.scheme}:{target.path}")
            return

        malai_id = malai_connection['malai_id']
        source_subpath = malai_connection.get('subpath', '')

        exclude_flags = [f"--exclude={e}" for e in exclude] if exclude else []
        if exclude_flags and self.debug:
            print(f"[DEBUG] Rsync exclusions: {exclude_flags}")

        if target.scheme in ('docker-volume', 'directory'):
            volume_mount = target.docker_volume_arg('/data')

            container_name = f"rsync-client-{unique_id}"
            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = f"/data{target.subpath}/"

            exclude_str = ' '.join(shlex.quote(f) for f in exclude_flags)
            if post_sync_command:
                entrypoint_args = (
                    f"--entrypoint sh {self.TRANSFER_P2P_IMAGE} -c "
                    f"'/usr/local/bin/entrypoint.sh rsync client {malai_id} {source_path} {dest_path} {exclude_str} && {post_sync_command}'"
                )
            else:
                entrypoint_args = f"{self.TRANSFER_P2P_IMAGE} rsync client {malai_id} {source_path} {dest_path} {exclude_str}"

            client_cmd = (
                f"docker run --pull always --name {container_name} "
                f"{volume_mount} "
                f"{entrypoint_args}"
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
            pod_name = f"rsync-client-{unique_id}"
            context = self.get_k8s_context(target.host)

            source_path = f"{source_subpath}/" if source_subpath else "/"
            dest_path = f"/data{target.subpath}/"

            exclude_str = ' '.join(exclude_flags)
            if post_sync_command:
                command = ["sh", "-c"]
                args_list = [f"/usr/local/bin/entrypoint.sh rsync client {malai_id} {source_path} {dest_path} {exclude_str} && {post_sync_command}"]
            else:
                command = None
                args_list = ["rsync", "client", malai_id, source_path, dest_path] + exclude_flags

            node = self._find_node_for_pvc(context, target.namespace, target.pvc_name)
            se_linux_level = self._find_selinux_level_for_pvc(context, target.namespace, target.pvc_name)
            pod_spec = self._make_pvc_pod_spec(
                pod_name, target.namespace, target.pvc_name,
                container_name="rsync-p2p-client",
                image=self.TRANSFER_P2P_IMAGE,
                command=command,
                args=args_list,
                env=[{"name": "QUIET", "value": str(not self.debug).lower()}],
                node_name=node,
                se_linux_level=se_linux_level,
            )

            if node:
                print(f"  📌 Pinning client pod to node {node} (co-locating with PVC {target.pvc_name})")
            if se_linux_level:
                print(f"  🔒 Using SELinux level {se_linux_level} (matching existing pod on PVC)")

            print(f"  🔄 Running rsync-p2p client pod (Malai P2P) in K8s...")
            if self.debug:
                print(f"[DEBUG] Malai ID: {malai_id}")

            print(f"  ⏳ Waiting for rsync-p2p client to complete...")
            self._run_k8s_pod_to_completion(context, pod_spec)

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

            pod_spec = self._make_pvc_pod_spec(
                pod_name, pvc_namespace, pvc_name,
                container_name="pg-dump",
                image="postgres:alpine",
                command=["sh", "-c"],
                args=[f"pg_dump -h {pod}.{namespace}.svc.cluster.local -U {source.username} -Fc -f {dump_file} {source.path}"],
                env=[{"name": "PGPASSWORD", "value": source.password or ""}],
                mount_path="/dump",
            )

            print(f"  ⏳ Waiting for pg_dump to complete...")
            self._run_k8s_pod_to_completion(context, pod_spec)
        else:
            container_name = f"pg-dump-{unique_id}"
            self.execute_on_host(source.host, "docker pull postgres:alpine")

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"--network container:{source.service_name} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={source.password or ''} "
                f"postgres:alpine "
                f"pg_dump -h localhost -U {source.username} -Fc -f {dump_file} {source.path}"
            )
            self._run_docker_to_completion(source.host, container_name, start_cmd)

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

            pod_spec = self._make_pvc_pod_spec(
                pod_name, pvc_namespace, pvc_name,
                container_name="pg-restore",
                image="postgres:alpine",
                command=["sh", "-c"],
                args=[f"pg_restore -h {pod}.{namespace}.svc.cluster.local -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file} 2>&1 | grep -v 'transaction_timeout' || true"],
                env=[{"name": "PGPASSWORD", "value": target.password or ""}],
                mount_path="/dump",
            )

            print(f"  ⏳ Waiting for pg_restore to complete...")
            self._run_k8s_pod_to_completion(context, pod_spec)
        else:
            container_name = f"pg-restore-{unique_id}"
            self.execute_on_host(target.host, "docker pull postgres:alpine")

            start_cmd = (
                f"docker run -d --name {container_name} "
                f"--network container:{target.service_name} "
                f"-v {volume_identifier}:/dump "
                f"-e PGPASSWORD={target.password or ''} "
                f"postgres:alpine "
                f"sh -c \"pg_restore -h localhost -U {target.username} -j4 --no-owner --clean -d {target.path} {dump_file} 2>&1 | grep -v 'transaction_timeout' || true\""
            )
            self._run_docker_to_completion(target.host, container_name, start_cmd)

        print(f"  ✅ pg_restore completed")

    # --- Main transfer orchestrators ---

    def transfer_rsync_daemon(
        self,
        source: Location,
        target: Location,
        exclude: Optional[List[str]] = None,
        post_sync_command: Optional[str] = None,
    ):
        """Universal incremental transfer using rsync-p2p with Malai P2P."""
        print(f"🔄 Transferring via rsync-p2p (Malai P2P): {source.scheme}:{source.path} → {target.scheme}:{target.path}")

        unique_id = self._generate_unique_id()

        daemon_info = None
        try:
            daemon_info = self._start_rsync_daemon(source, unique_id)

            malai_connection = self._get_malai_connection_info(daemon_info)
            print(f"  📡 Malai P2P ID: {malai_connection['malai_id']}")

            self._run_rsync_client(target, malai_connection, unique_id, exclude, post_sync_command)

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

        if transfer_key == 'weaviate->weaviate':
            if source.volume_type and target.volume_type:
                self.transfer_weaviate_backup_restore(source, target, exclude)
            else:
                self.transfer_weaviate_to_weaviate(source, target, exclude)
            return

        specialized_handlers = {
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
    parser.add_argument('--timeout', type=int, default=300, metavar='SECONDS',
                        help='Timeout in seconds for long-running operations (default: 300)')
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
    engine = TransferEngine(args.from_host, args.to_host, dry_run=args.dry_run, debug=args.debug, timeout=args.timeout)
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
