from __future__ import annotations

"""HTTP server with a fixed daemon worker pool and bounded pending work."""

from http.server import HTTPServer
import queue
import socket
import threading
import time
from typing import Any, Optional, Set, Tuple, Type


DEFAULT_HTTP_MAX_WORKERS = 32
DEFAULT_HTTP_QUEUE_CAPACITY = 128
DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC = 0.5
DEFAULT_SHUTDOWN_WORKER_GRACE_SEC = 0.2


class BoundedThreadPoolHTTPServer(HTTPServer):
    """Serve requests on a fixed-size pool with bounded admission.

    Accepted connections that cannot acquire either a worker or a queue slot
    are closed immediately. This keeps both worker threads and queued requests
    bounded under overload.
    """

    allow_reuse_address = True

    def __init__(
        self,
        server_address: Tuple[str, int],
        request_handler_class: Type[Any],
        *,
        max_workers: int = DEFAULT_HTTP_MAX_WORKERS,
        queue_capacity: int = DEFAULT_HTTP_QUEUE_CAPACITY,
        keepalive_idle_timeout_sec: float = DEFAULT_KEEPALIVE_IDLE_TIMEOUT_SEC,
        shutdown_worker_grace_sec: float = DEFAULT_SHUTDOWN_WORKER_GRACE_SEC,
        thread_name_prefix: str = "pycloud-http",
        bind_and_activate: bool = True,
    ) -> None:
        workers = int(max_workers)
        queued = int(queue_capacity)
        if workers <= 0:
            raise ValueError("max_workers must be greater than zero")
        if queued < 0:
            raise ValueError("queue_capacity must be non-negative")
        self.max_workers = workers
        self.queue_capacity = queued
        self.keepalive_idle_timeout_sec = max(0.1, float(keepalive_idle_timeout_sec))
        self.shutdown_worker_grace_sec = max(0.0, float(shutdown_worker_grace_sec))
        self.request_queue_size = max(1, workers + queued)
        self._request_slots = threading.BoundedSemaphore(workers + queued)
        self._close_lock = threading.Lock()
        self._requests_lock = threading.Lock()
        self._active_requests: Set[socket.socket] = set()
        self._workers_closed = False
        self.rejected_requests = 0
        super().__init__(server_address, request_handler_class, bind_and_activate=bind_and_activate)
        self._request_queue: "queue.Queue[Optional[Tuple[socket.socket, Tuple[str, int]]]]" = queue.Queue()
        self._worker_threads = []
        for index in range(workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"{thread_name_prefix}_{index}",
                daemon=True,
            )
            worker.start()
            self._worker_threads.append(worker)

    def process_request(self, request: socket.socket, client_address: Tuple[str, int]) -> None:
        request.settimeout(self.keepalive_idle_timeout_sec)
        with self._close_lock:
            if self._workers_closed or not self._request_slots.acquire(blocking=False):
                self.rejected_requests += 1
                self._force_close_request(request)
                return
            with self._requests_lock:
                self._active_requests.add(request)
            self._request_queue.put_nowait((request, client_address))

    def _worker_loop(self) -> None:
        while True:
            item = self._request_queue.get()
            try:
                if item is None:
                    return
                request, client_address = item
                self._process_request(request, client_address)
            finally:
                self._request_queue.task_done()

    def _process_request(self, request: socket.socket, client_address: Tuple[str, int]) -> None:
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)
            with self._requests_lock:
                self._active_requests.discard(request)
            self._request_slots.release()

    def server_close(self) -> None:
        with self._close_lock:
            if self._workers_closed:
                return
            self._workers_closed = True
            super().server_close()
        with self._requests_lock:
            active_requests = tuple(self._active_requests)
        for request in active_requests:
            self._force_close_request(request)
        while True:
            try:
                item = self._request_queue.get_nowait()
            except queue.Empty:
                break
            try:
                if item is None:
                    continue
                request, _client_address = item
                with self._requests_lock:
                    self._active_requests.discard(request)
                self._request_slots.release()
                self._force_close_request(request)
            finally:
                self._request_queue.task_done()
        for _worker in self._worker_threads:
            self._request_queue.put_nowait(None)
        deadline = time.monotonic() + self.shutdown_worker_grace_sec
        for worker in self._worker_threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                break
            worker.join(timeout=remaining)

    def _force_close_request(self, request: socket.socket) -> None:
        try:
            request.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.close_request(request)
