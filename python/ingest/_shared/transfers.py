"""
Shared upload/download helpers for the ingest pipelines.

This module centralizes the network-transfer behavior that had drifted across
the individual source pipelines. The goals are:
- one env-driven set of transfer defaults
- one HTTP session configuration with connection pooling and retries
- one boto3 transfer configuration with explicit concurrency settings
- one set of helper functions that keep progress bars and print output
  consistent across pipelines

The helpers are intentionally thin. Individual pipelines still decide which
files to transfer and how to structure manifests, but they all use the same
transport layer and the same runtime defaults.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import boto3
import requests
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

TQDM_KW = {"file": sys.stdout} if sys.platform == "win32" else {}


def _env_int(name: str, default: int) -> int:
    """Parse one integer env var with a safe default fallback."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


DOWNLOAD_WORKERS_DEFAULT = _env_int("INGEST_DOWNLOAD_WORKERS", 6)
UPLOAD_WORKERS_DEFAULT = _env_int("INGEST_UPLOAD_WORKERS", 4)
VERIFY_WORKERS_DEFAULT = _env_int("INGEST_VERIFY_WORKERS", 12)
HTTP_CHUNK_MB_DEFAULT = _env_int("INGEST_HTTP_CHUNK_MB", 8)
HTTP_POOL_SIZE_DEFAULT = _env_int("INGEST_HTTP_POOL_SIZE", 16)
HTTP_RETRIES_DEFAULT = _env_int("INGEST_HTTP_RETRIES", 5)
S3_MULTIPART_THRESHOLD_MB_DEFAULT = _env_int("INGEST_S3_MULTIPART_THRESHOLD_MB", 8)
S3_MULTIPART_CHUNKSIZE_MB_DEFAULT = _env_int("INGEST_S3_MULTIPART_CHUNKSIZE_MB", 8)
S3_MAX_CONCURRENCY_DEFAULT = _env_int("INGEST_S3_MAX_CONCURRENCY", 24)
S3_MAX_POOL_CONNECTIONS_DEFAULT = _env_int("INGEST_S3_MAX_POOL_CONNECTIONS", 32)


@dataclass(frozen=True)
class TransferSettings:
    """Resolved transfer settings used for both HTTP and S3 helpers."""

    download_workers: int = DOWNLOAD_WORKERS_DEFAULT
    upload_workers: int = UPLOAD_WORKERS_DEFAULT
    verify_workers: int = VERIFY_WORKERS_DEFAULT
    http_chunk_mb: int = HTTP_CHUNK_MB_DEFAULT
    http_pool_size: int = HTTP_POOL_SIZE_DEFAULT
    http_retries: int = HTTP_RETRIES_DEFAULT
    s3_multipart_threshold_mb: int = S3_MULTIPART_THRESHOLD_MB_DEFAULT
    s3_multipart_chunksize_mb: int = S3_MULTIPART_CHUNKSIZE_MB_DEFAULT
    s3_max_concurrency: int = S3_MAX_CONCURRENCY_DEFAULT
    s3_max_pool_connections: int = S3_MAX_POOL_CONNECTIONS_DEFAULT

    @property
    def http_chunk_bytes(self) -> int:
        return max(1, self.http_chunk_mb) * 1024 * 1024

    @property
    def s3_multipart_threshold_bytes(self) -> int:
        return max(1, self.s3_multipart_threshold_mb) * 1024 * 1024

    @property
    def s3_multipart_chunksize_bytes(self) -> int:
        return max(1, self.s3_multipart_chunksize_mb) * 1024 * 1024


def transfer_settings() -> TransferSettings:
    """Return the current env-resolved transfer settings."""
    return TransferSettings()


def print_transfer_settings(*, label: str = "transfer") -> None:
    """Print the active shared transfer settings for one script startup."""
    settings = transfer_settings()
    print(
        f"[{label}] settings: "
        f"download_workers={settings.download_workers} "
        f"upload_workers={settings.upload_workers} "
        f"verify_workers={settings.verify_workers} "
        f"http_chunk_mb={settings.http_chunk_mb} "
        f"http_pool_size={settings.http_pool_size} "
        f"http_retries={settings.http_retries} "
        f"s3_threshold_mb={settings.s3_multipart_threshold_mb} "
        f"s3_chunk_mb={settings.s3_multipart_chunksize_mb} "
        f"s3_max_concurrency={settings.s3_max_concurrency} "
        f"s3_max_pool_connections={settings.s3_max_pool_connections}",
        flush=True,
    )


def build_http_session(*, settings: TransferSettings | None = None) -> requests.Session:
    """Build one pooled HTTP session with bounded retries."""
    resolved = settings or transfer_settings()
    retry = Retry(
        total=max(0, resolved.http_retries),
        connect=max(0, resolved.http_retries),
        read=max(0, resolved.http_retries),
        backoff_factor=1.0,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=max(1, resolved.http_pool_size),
        pool_maxsize=max(1, resolved.http_pool_size),
        max_retries=retry,
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def managed_requests(method: str, url: str, *, session: requests.Session | None = None, **kwargs: Any) -> requests.Response:
    """
    Execute one HTTP request using a pooled session.

    Callers can pass an existing session when they want to reuse connections
    across a file fan-out step. When no session is supplied we create and close
    one for the single request.
    """
    if session is not None:
        response = session.request(method, url, **kwargs)
        return response
    with build_http_session() as owned_session:
        response = owned_session.request(method, url, **kwargs)
        return response


class TqdmUpload:
    """boto3 callback wrapper so uploads render as tqdm progress bars."""

    def __init__(self, pbar: tqdm) -> None:
        self._pbar = pbar

    def __call__(self, bytes_amount: int) -> None:
        self._pbar.update(bytes_amount)


def s3_client(region: str, *, settings: TransferSettings | None = None) -> Any:
    """Build an S3 client with an increased connection pool for parallel steps."""
    resolved = settings or transfer_settings()
    config = BotoConfig(max_pool_connections=max(1, resolved.s3_max_pool_connections))
    return boto3.Session(region_name=region).client("s3", config=config)


def s3_transfer_config(*, settings: TransferSettings | None = None) -> TransferConfig:
    """Build the shared managed-transfer config used by uploads."""
    resolved = settings or transfer_settings()
    return TransferConfig(
        multipart_threshold=resolved.s3_multipart_threshold_bytes,
        multipart_chunksize=resolved.s3_multipart_chunksize_bytes,
        max_concurrency=max(1, resolved.s3_max_concurrency),
        use_threads=True,
    )


def download_with_progress(
    url: str,
    output_path: Path,
    *,
    expected_bytes: int | None = None,
    timeout: int = 120,
    session: requests.Session | None = None,
    position: int | None = None,
    desc: str | None = None,
    settings: TransferSettings | None = None,
) -> int:
    """Download one remote file to disk with a visible byte progress bar."""
    resolved = settings or transfer_settings()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    response = managed_requests("GET", url, session=session, stream=True, timeout=timeout)
    with response:
        response.raise_for_status()
        total = expected_bytes
        if total is None:
            header = response.headers.get("Content-Length")
            content_encoding = response.headers.get("Content-Encoding", "").lower()
            if header and header.isdigit() and content_encoding != "gzip":
                total = int(header)
        tqdm_kwargs = dict(TQDM_KW)
        if position is not None:
            tqdm_kwargs["position"] = position
        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc or f"download {output_path.name}",
            leave=True,
            **tqdm_kwargs,
        ) as pbar:
            with open(output_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=resolved.http_chunk_bytes):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    pbar.update(len(chunk))
    return output_path.stat().st_size


def measure_remote_streamed_bytes(
    url: str,
    *,
    timeout: int = 120,
    session: requests.Session | None = None,
    settings: TransferSettings | None = None,
) -> int:
    """Stream a remote asset and return the number of decoded bytes delivered."""
    resolved = settings or transfer_settings()
    total = 0
    response = managed_requests("GET", url, session=session, stream=True, timeout=timeout)
    with response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=resolved.http_chunk_bytes):
            if not chunk:
                continue
            total += len(chunk)
    return total


def upload_file_with_progress(
    local_path: Path,
    bucket: str,
    key: str,
    region: str,
    *,
    extra_args: dict[str, Any] | None = None,
    client: Any | None = None,
    transfer_config: TransferConfig | None = None,
    position: int | None = None,
    desc: str | None = None,
    settings: TransferSettings | None = None,
) -> None:
    """Upload one local file to S3 with a visible byte progress bar."""
    resolved = settings or transfer_settings()
    s3 = client or s3_client(region, settings=resolved)
    config = transfer_config or s3_transfer_config(settings=resolved)
    size = local_path.stat().st_size
    tqdm_kwargs = dict(TQDM_KW)
    if position is not None:
        tqdm_kwargs["position"] = position
    with tqdm(
        total=size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=desc or f"upload {local_path.name}",
        leave=True,
        **tqdm_kwargs,
    ) as pbar:
        callback = TqdmUpload(pbar)
        s3.upload_file(
            str(local_path),
            bucket,
            key,
            ExtraArgs=extra_args or {},
            Callback=callback,
            Config=config,
        )


def s3_object_size(bucket: str, key: str, region: str, *, client: Any | None = None, settings: TransferSettings | None = None) -> int | None:
    """Return the S3 ContentLength for one object, or None when missing."""
    s3 = client or s3_client(region, settings=settings)
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        return int(response.get("ContentLength", 0))
    except ClientError:
        return None


def should_skip_upload(
    local_path: Path,
    bucket: str,
    key: str,
    region: str,
    overwrite: bool,
    *,
    client: Any | None = None,
    settings: TransferSettings | None = None,
) -> bool:
    """True when the S3 object already matches the local file bytes."""
    if overwrite:
        return False
    existing_size = s3_object_size(bucket, key, region, client=client, settings=settings)
    if existing_size is None:
        return False
    return local_path.stat().st_size == existing_size


def parallel_map(
    items: Iterable[Any],
    *,
    worker_count: int,
    desc: str,
    unit: str,
    fn: Callable[[Any], Any],
) -> list[Any]:
    """
    Run one callable over items with a stable overall tqdm progress bar.

    The per-item operation decides whether it also wants a per-file progress bar.
    This helper only owns the coarse-grained file-count progress.
    """
    item_list = list(items)
    if not item_list:
        return []
    resolved_workers = max(1, min(worker_count, len(item_list)))
    results: list[Any] = []
    with ThreadPoolExecutor(max_workers=resolved_workers) as executor:
        future_to_item = {executor.submit(fn, item): item for item in item_list}
        with tqdm(total=len(item_list), desc=desc, unit=unit, **TQDM_KW) as overall:
            for future in as_completed(future_to_item):
                results.append(future.result())
                overall.update(1)
    return results


def batch_s3_object_sizes(
    tasks: Iterable[tuple[str, str]],
    *,
    region: str,
    worker_count: int,
    settings: TransferSettings | None = None,
) -> dict[tuple[str, str], int | None]:
    """Resolve many S3 object sizes in parallel."""
    task_list = list(tasks)
    if not task_list:
        return {}
    resolved = settings or transfer_settings()

    def _one(task: tuple[str, str]) -> tuple[tuple[str, str], int | None]:
        bucket, key = task
        client = s3_client(region, settings=resolved)
        return task, s3_object_size(bucket, key, region, client=client, settings=resolved)

    results = parallel_map(
        task_list,
        worker_count=worker_count,
        desc="verify s3 sizes",
        unit="file",
        fn=_one,
    )
    return dict(results)


def print_parallel_summary(*, label: str, pending: int, skipped: int, workers: int) -> None:
    """Print a consistent one-line summary before a bounded parallel fan-out."""
    print(
        f"[{label}] queued={pending} skipped={skipped} workers={max(1, workers)}",
        flush=True,
    )


def print_task_elapsed(start_ts: float, label: str) -> None:
    """Print elapsed time for one subtask."""
    elapsed = time.perf_counter() - start_ts
    print(f"[time] {label}: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)
