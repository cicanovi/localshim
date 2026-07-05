from __future__ import annotations
import argparse
import errno
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, TextIO
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request as UrlRequest
from urllib.request import urlopen
import uvicorn
from core.app import create_app
from core.config import load_config, resolve_config_path
from core.overrides import AppOverrides, apply_app_overrides
from core.runtime import ShimRuntime
from core.runtime_builder import create_initial_runtime

__version__ = "0.5.0-dev"
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
EXIT_UNREACHABLE = 3
DEFAULT_TARGET_SCHEME = "http"
DEFAULT_TARGET_HOST = "127.0.0.1"
DEFAULT_TARGET_PORT = 5413


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="localshim",
        description="LocalShim operator CLI for starting, inspecting, and controlling a local OpenAI-compatible shim.",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)
    _add_run_command(subparsers)
    _add_doctor_command(subparsers)
    _add_ping_command(subparsers)
    _add_models_command(subparsers)
    _add_status_command(subparsers)
    _add_runtime_command(subparsers)
    _add_events_command(subparsers)
    _add_config_commands(subparsers)
    _add_version_command(subparsers)
    return parser


def _add_run_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "run",
        help="Run LocalShim.",
        description="Run LocalShim with an optional config path and startup overrides.",
    )
    _add_config_path_argument(parser)
    _add_startup_override_arguments(parser)
    parser.add_argument(
        "--log-level",
        choices=("debug", "info", "warning", "error"),
        default="info",
        help="Logging level for the LocalShim process.",
    )
    parser.add_argument(
        "--access-log",
        dest="access_log",
        action="store_true",
        default=True,
        help="Enable uvicorn access logs.",
    )
    parser.add_argument(
        "--no-access-log",
        dest="access_log",
        action="store_false",
        help="Disable uvicorn access logs.",
    )
    parser.add_argument(
        "--banner",
        choices=("auto", "always", "never"),
        default="auto",
        help="Control startup banner display.",
    )
    parser.add_argument(
        "--title",
        choices=("auto", "always", "never"),
        default="auto",
        help="Control terminal title updates.",
    )
    parser.add_argument(
        "--reload", action="store_true", help="Enable uvicorn reload mode."
    )
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="Allow binding to a non-localhost interface.",
    )
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_run, command_path="run")


def _add_doctor_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "doctor",
        help="Validate LocalShim config and local runtime setup.",
        description="Validate config loading, overrides, runtime construction, plugin loading, and optionally backend reachability.",
    )
    _add_config_path_argument(parser)
    _add_startup_override_arguments(parser)
    parser.add_argument(
        "--check-backend",
        action="store_true",
        help="Also check whether the configured backend responds.",
    )
    parser.add_argument(
        "--backend-timeout",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="Backend check timeout in seconds.",
    )
    parser.add_argument(
        "--strict", action="store_true", help="Treat warnings as failures."
    )
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_doctor, command_path="doctor")


def _add_ping_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "ping",
        help="Ping a running LocalShim instance.",
        description="Call GET / on a running LocalShim instance.",
    )
    _add_target_arguments(parser)
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_ping, command_path="ping")


def _add_models_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "models",
        help="List models through a running LocalShim instance.",
        description="Call GET /v1/models on a running LocalShim instance.",
    )
    _add_target_arguments(parser)
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_models, command_path="models")


def _add_status_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "status",
        help="Show LocalShim service and runtime status.",
        description="Call GET /shim/status on a running LocalShim instance.",
    )
    _add_target_arguments(parser)
    _add_watch_arguments(parser)
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_status, command_path="status")


def _add_runtime_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "runtime",
        help="Show the active LocalShim runtime snapshot.",
        description="Call GET /shim/runtime on a running LocalShim instance.",
    )
    _add_target_arguments(parser)
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_runtime, command_path="runtime")


def _add_events_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "events",
        help="Show recent LocalShim events.",
        description="Call GET /shim/events on a running LocalShim instance.",
    )
    _add_target_arguments(parser)
    _add_watch_arguments(parser)
    parser.add_argument(
        "--limit", type=int, default=50, help="Maximum number of events to return."
    )
    parser.add_argument(
        "--since-id",
        type=int,
        default=None,
        help="Only return events with id greater than this value.",
    )
    parser.add_argument(
        "--event-type", default=None, help="Only return events with this event_type."
    )
    parser.add_argument(
        "--level",
        choices=("basic", "detailed", "debug"),
        default=None,
        help="Only return events with this event level.",
    )
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_events, command_path="events")


def _add_config_commands(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "config",
        help="Config inspection and runtime update commands.",
        description="Inspect configs or request runtime config updates.",
    )
    config_subparsers = parser.add_subparsers(
        dest="config_command", metavar="CONFIG_COMMAND", required=True
    )
    _add_config_render_command(config_subparsers)
    _add_config_apply_command(config_subparsers)
    _add_config_reload_command(config_subparsers)


def _add_config_render_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "render",
        help="Render the effective config.",
        description="Load a config, apply local overrides, and print the effective config. Output is redacted by default.",
    )
    _add_config_path_argument(parser)
    _add_startup_override_arguments(parser)
    parser.add_argument(
        "--show-secrets",
        action="store_true",
        help="Print full unredacted effective config.",
    )
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_config_render, command_path="config render")


def _add_config_apply_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "apply",
        help="Apply a candidate config to a running LocalShim instance.",
        description="Load a candidate config, apply local candidate overrides, and send the full candidate config to PUT /shim/config.",
    )
    parser.add_argument("candidate_config_path", help="Candidate config file to apply.")
    _add_target_arguments(
        parser, include_config_path=True, config_dest="target_config_path"
    )
    _add_startup_override_arguments(parser, include_host=False, include_port=False)
    parser.add_argument(
        "--mode",
        choices=("late_gate", "early_gate"),
        default="late_gate",
        help="Runtime config apply mode.",
    )
    parser.add_argument(
        "--allow-remote-control",
        action="store_true",
        help="Allow config apply against a non-localhost LocalShim target.",
    )
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_config_apply, command_path="config apply")


def _add_config_reload_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "reload",
        help="Reload the active config path on a running LocalShim instance.",
        description="Call POST /shim/config/reload on a running LocalShim instance.",
    )
    _add_target_arguments(parser)
    parser.add_argument(
        "--mode",
        choices=("late_gate", "early_gate"),
        default="late_gate",
        help="Runtime config reload apply mode.",
    )
    parser.add_argument(
        "--allow-remote-control",
        action="store_true",
        help="Allow config reload against a non-localhost LocalShim target.",
    )
    _add_common_display_arguments(parser)
    parser.set_defaults(handler=_handle_config_reload, command_path="config reload")


def _add_version_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "version",
        help="Print the LocalShim CLI version.",
        description="Print the LocalShim CLI version.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Print version information as JSON."
    )
    parser.set_defaults(handler=_handle_version, command_path="version")


def _add_config_path_argument(
    parser: argparse.ArgumentParser, *, dest: str = "config_path"
) -> None:
    parser.add_argument(
        "-c",
        "--config",
        dest=dest,
        default=None,
        help="Path to a LocalShim config file.",
    )


def _add_startup_override_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_host: bool = True,
    include_port: bool = True,
    include_backend_url: bool = True,
) -> None:
    if include_host:
        parser.add_argument("--host", default=None, help="Override server.host.")
    if include_port:
        parser.add_argument(
            "-p", "--port", type=int, default=None, help="Override server.port."
        )
    if include_backend_url:
        parser.add_argument("--backend-url", default=None, help="Override backend.url.")


def _add_target_arguments(
    parser: argparse.ArgumentParser,
    *,
    include_config_path: bool = True,
    config_dest: str = "config_path",
) -> None:
    parser.add_argument(
        "--url",
        default=None,
        help="Full LocalShim base URL, such as http://127.0.0.1:5413.",
    )
    parser.add_argument("--host", default=None, help="LocalShim host to contact.")
    parser.add_argument(
        "-p", "--port", type=int, default=None, help="LocalShim port to contact."
    )
    if include_config_path:
        _add_config_path_argument(parser, dest=config_dest)
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="HTTP timeout in seconds.",
    )


def _add_watch_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--watch", action="store_true", help="Repeat the command until interrupted."
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help="Refresh interval for --watch.",
    )


def _add_common_display_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json", action="store_true", help="Print machine-readable JSON output."
    )
    parser.add_argument(
        "--no-color", action="store_true", help="Disable colored output."
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Reduce non-essential output."
    )


def _set_not_implemented(parser: argparse.ArgumentParser, command_path: str) -> None:
    parser.set_defaults(handler=_handle_not_implemented, command_path=command_path)


def _handle_version(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    _ = stderr
    if args.json:
        print(
            json.dumps(
                {"service": "localshim", "version": __version__}, sort_keys=True
            ),
            file=stdout,
        )
        return EXIT_SUCCESS
    print(f"LocalShim {__version__}", file=stdout)
    return EXIT_SUCCESS


_LOCAL_BIND_HOSTS = {"127.0.0.1", "localhost", "::1"}


class RunStartupError(Exception):
    0


@dataclass(frozen=True)
class RunPlan:
    config_path: str
    effective_config: dict[str, Any]
    overrides: AppOverrides
    host: str
    port: int
    backend_url: str


def _handle_run(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    if args.reload:
        print("localshim: --reload is not supported by CLI run yet", file=stderr)
        return EXIT_FAILURE
    try:
        plan = _prepare_run_plan(args)
    except Exception as error:
        print(f"localshim: {error}", file=stderr)
        return EXIT_FAILURE
    if _should_print_run_banner(args):
        _print_run_banner(plan, stdout=stdout)
    try:
        app = create_app(config_path=plan.config_path, overrides=plan.overrides)
    except Exception as error:
        print(f"localshim: failed to create app: {error}", file=stderr)
        return EXIT_FAILURE
    try:
        uvicorn.run(
            app,
            host=plan.host,
            port=plan.port,
            log_level=args.log_level,
            access_log=args.access_log,
        )
    except Exception as error:
        print(f"localshim: uvicorn failed: {error}", file=stderr)
        return EXIT_FAILURE
    return EXIT_SUCCESS


def _prepare_run_plan(args: argparse.Namespace) -> RunPlan:
    config_path = _resolve_cli_config_path(args)
    try:
        loaded_config = load_config(config_path)
    except Exception as error:
        raise RunStartupError(
            f"failed to load config {config_path}: {error}"
        ) from error
    if not isinstance(loaded_config, dict):
        raise RunStartupError("config file must contain a JSON object")
    overrides = AppOverrides(
        host=args.host, port=args.port, backend_url=args.backend_url
    )
    try:
        effective_config = apply_app_overrides(loaded_config, overrides)
    except Exception as error:
        raise RunStartupError(f"failed to apply overrides: {error}") from error
    host = _get_required_config_string(
        effective_config, section_name="server", key_name="host"
    )
    if host is None:
        raise RunStartupError("config must contain server.host")
    port = _get_required_config_int(
        effective_config, section_name="server", key_name="port"
    )
    if port is None:
        raise RunStartupError("config must contain integer server.port")
    _validate_run_port(port)
    backend_url = _get_required_config_string(
        effective_config, section_name="backend", key_name="url"
    )
    if backend_url is None:
        raise RunStartupError("config must contain backend.url")
    if not args.allow_network and not _is_local_bind_host(host):
        raise RunStartupError(
            f"refusing to bind non-localhost host {host} without --allow-network"
        )
    _ensure_bind_available(host=host, port=port)
    return RunPlan(
        config_path=config_path,
        effective_config=effective_config,
        overrides=overrides,
        host=host,
        port=port,
        backend_url=backend_url,
    )


def _validate_run_port(port: int) -> None:
    if port < 1 or port > 65535:
        raise RunStartupError("server.port must be between 1 and 65535")


def _is_local_bind_host(host: str) -> bool:
    return host.lower() in _LOCAL_BIND_HOSTS


def _ensure_bind_available(*, host: str, port: int) -> None:
    try:
        address_infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as error:
        raise RunStartupError(f"cannot resolve bind host {host!r}: {error}") from error
    if not address_infos:
        raise RunStartupError(f"cannot resolve bind host {host!r}")
    bind_errors: list[OSError] = []
    bound_any_candidate = False
    for family, socktype, proto, _, sockaddr in address_infos:
        try:
            with socket.socket(family, socktype, proto) as probe_socket:
                probe_socket.bind(sockaddr)
                bound_any_candidate = True
        except OSError as error:
            if error.errno == errno.EADDRINUSE:
                raise RunStartupError(
                    f"cannot start server on {host}:{port}: address already in use"
                ) from error
            bind_errors.append(error)
    if not bound_any_candidate:
        if bind_errors:
            raise RunStartupError(
                f"cannot start server on {host}:{port}: {bind_errors[0]}"
            ) from bind_errors[0]
        raise RunStartupError(
            f"cannot start server on {host}:{port}: no bindable address"
        )


def _should_print_run_banner(args: argparse.Namespace) -> bool:
    if args.quiet:
        return False
    return args.banner != "never"


def _print_run_banner(plan: RunPlan, *, stdout: TextIO) -> None:
    print("LocalShim starting", file=stdout)
    print(f"pid: {os.getpid()}", file=stdout)
    print(f"config: {plan.config_path}", file=stdout)
    print(f"server: {_format_server_url(plan.host, plan.port)}", file=stdout)
    print(f"backend: {plan.backend_url}", file=stdout)


def _format_server_url(host: str, port: int) -> str:
    if ":" in host and not host.startswith("["):
        return f"http://[{host}]:{port}"
    return f"http://{host}:{port}"


class TargetResolutionError(Exception):
    0


@dataclass(frozen=True)
class TargetPlan:
    base_url: str
    source: str
    config_path: str | None = None


def _resolve_target_base_url(args: argparse.Namespace) -> str:
    return _resolve_target_plan(args).base_url


def _resolve_target_plan(args: argparse.Namespace) -> TargetPlan:
    raw_url = getattr(args, "url", None)
    if raw_url is not None:
        return TargetPlan(base_url=_normalize_target_base_url(raw_url), source="url")
    raw_host = getattr(args, "host", None)
    raw_port = getattr(args, "port", None)
    if raw_host is not None or raw_port is not None:
        host = _resolve_target_host(raw_host)
        port = _resolve_target_port(raw_port)
        return TargetPlan(base_url=_format_server_url(host, port), source="args")
    config_path = _get_explicit_target_config_path(args)
    if config_path is not None:
        host, port = _load_target_host_and_port_from_config(config_path)
        return TargetPlan(
            base_url=_format_server_url(host, port),
            source="config",
            config_path=config_path,
        )
    return TargetPlan(
        base_url=_format_server_url(DEFAULT_TARGET_HOST, DEFAULT_TARGET_PORT),
        source="default",
    )


def _normalize_target_base_url(raw_url: str) -> str:
    url = raw_url.strip()
    if not url:
        raise TargetResolutionError("target URL must not be empty")
    if "://" not in url:
        url = f"{DEFAULT_TARGET_SCHEME}://{url}"
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise TargetResolutionError("target URL scheme must be http or https")
    if not parsed.netloc:
        raise TargetResolutionError("target URL must include a host")
    if parsed.query or parsed.fragment:
        raise TargetResolutionError("target URL must not include query or fragment")
    normalized_path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", ""))


def _resolve_target_host(raw_host: str | None) -> str:
    if raw_host is None:
        return DEFAULT_TARGET_HOST
    host = raw_host.strip()
    if not host:
        raise TargetResolutionError("target host must not be empty")
    return host


def _resolve_target_port(raw_port: int | None) -> int:
    if raw_port is None:
        return DEFAULT_TARGET_PORT
    _validate_target_port(raw_port)
    return raw_port


def _validate_target_port(port: int) -> None:
    if isinstance(port, bool) or not isinstance(port, int):
        raise TargetResolutionError("target port must be an integer")
    if port < 1 or port > 65535:
        raise TargetResolutionError("target port must be between 1 and 65535")


def _get_explicit_target_config_path(args: argparse.Namespace) -> str | None:
    for attr_name in ("target_config_path", "config_path"):
        value = getattr(args, attr_name, None)
        if value is not None:
            return str(value)
    return None


def _load_target_host_and_port_from_config(config_path: str) -> tuple[str, int]:
    try:
        loaded_config = load_config(config_path)
    except Exception as error:
        raise TargetResolutionError(
            f"failed to load target config {config_path}: {error}"
        ) from error
    if not isinstance(loaded_config, dict):
        raise TargetResolutionError("target config file must contain a JSON object")
    host = _get_required_config_string(
        loaded_config, section_name="server", key_name="host"
    )
    if host is None:
        raise TargetResolutionError("target config must contain server.host")
    port = _get_required_config_int(
        loaded_config, section_name="server", key_name="port"
    )
    if port is None:
        raise TargetResolutionError("target config must contain integer server.port")
    _validate_target_port(port)
    return host, port


class CliConfigError(Exception):
    0


class CliHttpError(Exception):
    0


@dataclass(frozen=True)
class CliHttpResponse:
    url: str
    status_code: int
    payload: Any


def _handle_ping(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    return _handle_http_read_command(
        args,
        endpoint_path="/",
        formatter=_format_ping_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_models(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    return _handle_http_read_command(
        args,
        endpoint_path="/v1/models",
        formatter=_format_models_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_status(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    return _handle_watchable_http_read_command(
        args,
        endpoint_path="/shim/status",
        query=None,
        formatter=_format_status_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_runtime(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    return _handle_http_read_command(
        args,
        endpoint_path="/shim/runtime",
        formatter=_format_runtime_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_events(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    query = _build_events_query(args)
    return _handle_watchable_http_read_command(
        args,
        endpoint_path="/shim/events",
        query=query,
        formatter=_format_events_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_watchable_http_read_command(
    args: argparse.Namespace,
    *,
    endpoint_path: str,
    query: dict[str, Any] | None,
    formatter: Any,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    if not getattr(args, "watch", False):
        return _handle_http_read_command(
            args,
            endpoint_path=endpoint_path,
            query=query,
            formatter=formatter,
            stdout=stdout,
            stderr=stderr,
        )
    try:
        while True:
            result = _handle_http_read_command(
                args,
                endpoint_path=endpoint_path,
                query=query,
                formatter=formatter,
                stdout=stdout,
                stderr=stderr,
            )
            if result != EXIT_SUCCESS:
                return result
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return EXIT_SUCCESS


def _handle_http_read_command(
    args: argparse.Namespace,
    *,
    endpoint_path: str,
    formatter: Any,
    stdout: TextIO,
    stderr: TextIO,
    query: dict[str, Any] | None = None,
) -> int:
    try:
        base_url = _resolve_target_base_url(args)
    except TargetResolutionError as error:
        print(f"localshim: {error}", file=stderr)
        return EXIT_FAILURE
    try:
        response = _http_get_json(
            base_url=base_url,
            path=endpoint_path,
            query=query,
            timeout_seconds=args.timeout,
        )
    except CliHttpError as error:
        print(f"localshim: {error}", file=stderr)
        return EXIT_UNREACHABLE
    if args.json:
        print(json.dumps(response.payload, indent=2, sort_keys=True), file=stdout)
    elif not args.quiet:
        formatter(response.payload, response=response, stdout=stdout)
    if 200 <= response.status_code < 300:
        return EXIT_SUCCESS
    return EXIT_FAILURE


def _http_get_json(
    *,
    base_url: str,
    path: str,
    query: dict[str, Any] | None = None,
    timeout_seconds: float,
) -> CliHttpResponse:
    return _http_json_request(
        method="GET",
        base_url=base_url,
        path=path,
        query=query,
        json_body=None,
        timeout_seconds=timeout_seconds,
    )


def _http_json_request(
    *,
    method: str,
    base_url: str,
    path: str,
    query: dict[str, Any] | None = None,
    json_body: Any | None = None,
    timeout_seconds: float,
) -> CliHttpResponse:
    url = _build_http_url(base_url=base_url, path=path, query=query)
    headers = {"Accept": "application/json"}
    data: bytes | None = None
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = UrlRequest(url, data=data, headers=headers, method=method.upper())
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.status
            body = response.read()
    except HTTPError as error:
        body = error.read()
        return CliHttpResponse(
            url=url,
            status_code=error.code,
            payload=_decode_http_json_body(body, url=url),
        )
    except TimeoutError as error:
        raise CliHttpError(f"target unreachable: {url}: timed out") from error
    except URLError as error:
        raise CliHttpError(f"target unreachable: {url}: {error.reason}") from error
    return CliHttpResponse(
        url=url, status_code=status_code, payload=_decode_http_json_body(body, url=url)
    )


def _build_http_url(
    *, base_url: str, path: str, query: dict[str, Any] | None = None
) -> str:
    normalized_base = base_url.rstrip("/")
    normalized_path = path if path.startswith("/") else f"/{path}"
    url = f"{normalized_base}{normalized_path}"
    clean_query = {
        key: value for (key, value) in (query or {}).items() if value is not None
    }
    if clean_query:
        url = f"{url}?{urlencode(clean_query)}"
    return url


def _decode_http_json_body(body: bytes, *, url: str) -> Any:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise CliHttpError(
            f"target returned non-UTF-8 response from {url}: {error}"
        ) from error
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise CliHttpError(
            f"target returned non-JSON response from {url}: {error}"
        ) from error


def _build_events_query(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "limit": args.limit,
        "since_id": args.since_id,
        "event_type": args.event_type,
        "level": args.level,
    }


def _resolve_control_write_base_url(args: argparse.Namespace) -> str:
    base_url = _resolve_target_base_url(args)
    if getattr(args, "allow_remote_control", False):
        return base_url
    if _is_local_control_target(base_url):
        return base_url
    hostname = urlsplit(base_url).hostname or base_url
    raise TargetResolutionError(
        f"refusing to send control write to non-localhost target {hostname!r} without --allow-remote-control"
    )


def _is_local_control_target(base_url: str) -> bool:
    hostname = urlsplit(base_url).hostname
    if hostname is None:
        return False
    return hostname.lower() in _LOCAL_BIND_HOSTS


def _load_candidate_config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    candidate_config_path = str(args.candidate_config_path)
    try:
        loaded_config = load_config(candidate_config_path)
    except Exception as error:
        raise CliConfigError(
            f"failed to load candidate config {candidate_config_path}: {error}"
        ) from error
    if not isinstance(loaded_config, dict):
        raise CliConfigError("candidate config file must contain a JSON object")
    try:
        return apply_app_overrides(
            loaded_config, AppOverrides(backend_url=args.backend_url)
        )
    except Exception as error:
        raise CliConfigError(
            f"failed to apply candidate config overrides: {error}"
        ) from error


def _handle_config_apply(
    args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO
) -> int:
    try:
        base_url = _resolve_control_write_base_url(args)
    except TargetResolutionError as error:
        print(f"localshim: {error}", file=stderr)
        return EXIT_FAILURE
    try:
        candidate_config = _load_candidate_config_from_args(args)
    except CliConfigError as error:
        print(f"localshim: config apply failed: {error}", file=stderr)
        return EXIT_FAILURE
    return _handle_http_write_command(
        args,
        method="PUT",
        base_url=base_url,
        endpoint_path="/shim/config",
        query={"mode": args.mode},
        json_body=candidate_config,
        formatter=_format_config_apply_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_config_reload(
    args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO
) -> int:
    try:
        base_url = _resolve_control_write_base_url(args)
    except TargetResolutionError as error:
        print(f"localshim: {error}", file=stderr)
        return EXIT_FAILURE
    return _handle_http_write_command(
        args,
        method="POST",
        base_url=base_url,
        endpoint_path="/shim/config/reload",
        query={"mode": args.mode},
        json_body=None,
        formatter=_format_config_reload_response,
        stdout=stdout,
        stderr=stderr,
    )


def _handle_http_write_command(
    args: argparse.Namespace,
    *,
    method: str,
    base_url: str,
    endpoint_path: str,
    query: dict[str, Any] | None,
    json_body: Any | None,
    formatter: Any,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    try:
        response = _http_json_request(
            method=method,
            base_url=base_url,
            path=endpoint_path,
            query=query,
            json_body=json_body,
            timeout_seconds=args.timeout,
        )
    except CliHttpError as error:
        print(f"localshim: {error}", file=stderr)
        return EXIT_UNREACHABLE
    if args.json:
        print(json.dumps(response.payload, indent=2, sort_keys=True), file=stdout)
    elif not args.quiet or not _config_write_payload_applied(response.payload):
        formatter(response.payload, response=response, stdout=stdout)
    if response.status_code < 200 or response.status_code >= 300:
        return EXIT_FAILURE
    if _config_write_payload_applied(response.payload):
        return EXIT_SUCCESS
    return EXIT_FAILURE


def _config_write_payload_applied(payload: Any) -> bool:
    return isinstance(payload, dict) and payload.get("status") == "applied"


def _format_config_apply_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    _format_config_write_response(
        "Config apply", payload, response=response, stdout=stdout
    )


def _format_config_reload_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    _format_config_write_response(
        "Config reload", payload, response=response, stdout=stdout
    )


def _format_config_write_response(
    label: str, payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    if not isinstance(payload, dict):
        status = "applied" if 200 <= response.status_code < 300 else "error"
        print(f"{label}: {status}", file=stdout)
        return
    status = payload.get("status", "unknown")
    print(f"{label}: {status}", file=stdout)
    for key in (
        "apply_id",
        "previous_generation",
        "candidate_generation",
        "runtime_generation",
        "current_runtime_generation",
        "phase",
        "mode",
        "persisted",
        "old_runtime_preserved",
        "superseded_by",
        "message",
        "runtime_fingerprint_short",
    ):
        if key in payload:
            print(f"{key}: {payload[key]}", file=stdout)
    reload_metadata = payload.get("reload")
    if isinstance(reload_metadata, dict):
        source = reload_metadata.get("source")
        config_path = reload_metadata.get("config_path")
        print(f"reload_source: {source}", file=stdout)
        print(f"reload_config_path: {config_path}", file=stdout)
    error = payload.get("error")
    if isinstance(error, dict):
        error_type = error.get("type")
        error_message = error.get("message")
        print(f"error_type: {error_type}", file=stdout)
        print(f"error_message: {error_message}", file=stdout)


def _format_ping_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    status = None
    if isinstance(payload, dict):
        status = payload.get("status")
    if status is None:
        status = "ok" if 200 <= response.status_code < 300 else "error"
    print(f"LocalShim reachable: {status}", file=stdout)


def _format_models_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    _ = response
    models = []
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            models = [item for item in data if isinstance(item, dict)]
    print(f"Models: {len(models)}", file=stdout)
    for model in models:
        model_id = model.get("id")
        if isinstance(model_id, str):
            print(f"- {model_id}", file=stdout)


def _format_status_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    _ = response
    if not isinstance(payload, dict):
        print("LocalShim status: unavailable", file=stdout)
        return
    print(f"LocalShim status: {payload.get('status', 'unknown')}", file=stdout)
    for key in (
        "runtime_generation",
        "runtime_fingerprint_short",
        "backend_url",
        "active_runs",
        "gate_open",
    ):
        if key in payload:
            print(f"{key}: {payload[key]}", file=stdout)
    plugins = payload.get("plugins")
    if isinstance(plugins, dict):
        upstream_count = plugins.get("upstream_count")
        downstream_count = plugins.get("downstream_count")
        print(
            f"plugins: upstream={upstream_count} downstream={downstream_count}",
            file=stdout,
        )


def _format_runtime_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    _ = response
    if not isinstance(payload, dict):
        print("Runtime: unavailable", file=stdout)
        return
    runtime_generation = payload.get("runtime_generation")
    backend_url = payload.get("backend_url")
    print(f"Runtime generation: {runtime_generation}", file=stdout)
    if backend_url is not None:
        print(f"backend_url: {backend_url}", file=stdout)
    state = payload.get("state")
    if isinstance(state, dict):
        active_runs = state.get("active_runs")
        gate_open = state.get("gate_open")
        print(f"state: active_runs={active_runs} gate_open={gate_open}", file=stdout)
    control_policy = payload.get("control_policy")
    if isinstance(control_policy, dict):
        apply_mode = control_policy.get("apply_mode")
        gate_policy = control_policy.get("gate_policy")
        queue_policy = control_policy.get("queue_policy")
        print(
            f"control_policy: apply_mode={apply_mode} gate_policy={gate_policy} queue_policy={queue_policy}",
            file=stdout,
        )


def _format_events_response(
    payload: Any, *, response: CliHttpResponse, stdout: TextIO
) -> None:
    _ = response
    events: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        raw_events = payload.get("events")
        if isinstance(raw_events, list):
            events = [item for item in raw_events if isinstance(item, dict)]
    print(f"Events: {len(events)}", file=stdout)
    for event in events:
        event_id = event.get("id")
        event_type = event.get("event_type")
        level = event.get("level")
        phase = event.get("phase")
        print(f"- {event_id}: {event_type} level={level} phase={phase}", file=stdout)


DoctorCheck = dict[str, Any]
DoctorResult = dict[str, Any]


def _handle_doctor(args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO) -> int:
    _ = stderr
    result = _run_doctor(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True), file=stdout)
    else:
        _print_doctor_result(result, stdout=stdout, quiet=args.quiet)
    if result["status"] == "ok":
        return EXIT_SUCCESS
    return EXIT_FAILURE


def _run_doctor(args: argparse.Namespace) -> DoctorResult:
    checks: list[DoctorCheck] = []
    config_path = _resolve_cli_config_path(args)
    _add_doctor_check(
        checks,
        name="config_path_resolved",
        status="ok",
        message=f"Using config path: {config_path}",
        details={"config_path": config_path},
    )
    if not Path(config_path).is_file():
        _add_doctor_check(
            checks,
            name="config_exists",
            status="fail",
            message=f"Config file does not exist: {config_path}",
            details={"config_path": config_path},
        )
        return _build_doctor_result(checks, strict=args.strict, config_path=config_path)
    _add_doctor_check(
        checks,
        name="config_exists",
        status="ok",
        message="Config file exists.",
        details={"config_path": config_path},
    )
    try:
        loaded_config = load_config(config_path)
        if not isinstance(loaded_config, dict):
            raise ValueError("Config file must contain a JSON object")
    except Exception as error:
        _add_doctor_check(
            checks,
            name="config_loads",
            status="fail",
            message=f"Config failed to load: {error}",
            details={"error": str(error)},
        )
        return _build_doctor_result(checks, strict=args.strict, config_path=config_path)
    _add_doctor_check(
        checks, name="config_loads", status="ok", message="Config loaded successfully."
    )
    try:
        effective_config = apply_app_overrides(
            loaded_config,
            AppOverrides(host=args.host, port=args.port, backend_url=args.backend_url),
        )
    except Exception as error:
        _add_doctor_check(
            checks,
            name="overrides_apply",
            status="fail",
            message=f"Overrides failed to apply: {error}",
            details={"error": str(error)},
        )
        return _build_doctor_result(checks, strict=args.strict, config_path=config_path)
    _add_doctor_check(
        checks,
        name="overrides_apply",
        status="ok",
        message="Overrides applied successfully.",
    )
    backend_url = _get_required_config_string(
        effective_config, section_name="backend", key_name="url"
    )
    if backend_url is None:
        _add_doctor_check(
            checks,
            name="backend_url_present",
            status="fail",
            message="Config must contain backend.url.",
        )
    else:
        _add_doctor_check(
            checks,
            name="backend_url_present",
            status="ok",
            message=f"backend.url is set to {backend_url}",
            details={"backend_url": backend_url},
        )
    server_host = _get_required_config_string(
        effective_config, section_name="server", key_name="host"
    )
    if server_host is None:
        _add_doctor_check(
            checks,
            name="server_host_present",
            status="fail",
            message="Config must contain server.host.",
        )
    else:
        _add_doctor_check(
            checks,
            name="server_host_present",
            status="ok",
            message=f"server.host is set to {server_host}",
            details={"server_host": server_host},
        )
    server_port = _get_required_config_int(
        effective_config, section_name="server", key_name="port"
    )
    if server_port is None:
        _add_doctor_check(
            checks,
            name="server_port_present",
            status="fail",
            message="Config must contain integer server.port.",
        )
    else:
        _add_doctor_check(
            checks,
            name="server_port_present",
            status="ok",
            message=f"server.port is set to {server_port}",
            details={"server_port": server_port},
        )
    runtime: ShimRuntime | None = None
    if not _has_failed_check(checks):
        try:
            runtime = create_initial_runtime(
                config_path=config_path, config=effective_config, generation=1
            )
        except Exception as error:
            _add_doctor_check(
                checks,
                name="runtime_builds",
                status="fail",
                message=f"Runtime failed to build: {error}",
                details={"error": str(error)},
            )
        else:
            _add_doctor_check(
                checks,
                name="runtime_builds",
                status="ok",
                message="Runtime built successfully.",
                details={
                    "generation": runtime.generation,
                    "backend_url": runtime.backend_url,
                    "runtime_fingerprint": runtime.runtime_fingerprint,
                },
            )
            _add_doctor_check(
                checks,
                name="plugins_load",
                status="ok",
                message=f"Plugins loaded successfully upstream={len(runtime.upstream_plugins)} downstream={len(runtime.downstream_plugins)}.",
                details={
                    "upstream_count": len(runtime.upstream_plugins),
                    "downstream_count": len(runtime.downstream_plugins),
                },
            )
    if args.check_backend and runtime is not None:
        backend_check = _check_backend_models_reachable(
            runtime.backend_url, timeout_seconds=args.backend_timeout
        )
        _add_doctor_check(
            checks,
            name="backend_models_reachable",
            status=backend_check["status"],
            message=backend_check["message"],
            details=backend_check["details"],
        )
    return _build_doctor_result(
        checks,
        strict=args.strict,
        config_path=config_path,
        effective_config=effective_config,
        runtime=runtime,
    )


def _resolve_cli_config_path(args: argparse.Namespace) -> str:
    if args.config_path is not None:
        return str(args.config_path)
    return resolve_config_path()


def _get_required_config_string(
    config: dict[str, Any], *, section_name: str, key_name: str
) -> str | None:
    section = config.get(section_name)
    if not isinstance(section, dict):
        return None
    value = section.get(key_name)
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _get_required_config_int(
    config: dict[str, Any], *, section_name: str, key_name: str
) -> int | None:
    section = config.get(section_name)
    if not isinstance(section, dict):
        return None
    value = section.get(key_name)
    if isinstance(value, bool):
        return None
    if not isinstance(value, int):
        return None
    return value


def _check_backend_models_reachable(
    backend_url: str, *, timeout_seconds: float
) -> DoctorCheck:
    url = f"{backend_url.rstrip('/')}/v1/models"
    request = UrlRequest(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            status_code = response.status
            body = response.read()
    except HTTPError as error:
        status = "warning"
        if error.code >= 500:
            status = "fail"
        return {
            "status": status,
            "message": f"Backend /v1/models responded with HTTP {error.code}.",
            "details": {"url": url, "status_code": error.code},
        }
    except TimeoutError as error:
        return {
            "status": "fail",
            "message": f"Backend /v1/models timed out after {timeout_seconds} seconds.",
            "details": {"url": url, "error": str(error)},
        }
    except URLError as error:
        return {
            "status": "fail",
            "message": f"Backend /v1/models is unreachable: {error}",
            "details": {"url": url, "error": str(error)},
        }
    if status_code < 200 or status_code >= 300:
        return {
            "status": "warning",
            "message": f"Backend /v1/models responded with HTTP {status_code}.",
            "details": {"url": url, "status_code": status_code},
        }
    try:
        json.loads(body.decode("utf-8"))
    except Exception as error:
        return {
            "status": "warning",
            "message": f"Backend /v1/models responded successfully but did not return valid JSON: {error}",
            "details": {"url": url, "status_code": status_code, "error": str(error)},
        }
    return {
        "status": "ok",
        "message": "Backend /v1/models is reachable.",
        "details": {"url": url, "status_code": status_code},
    }


def _add_doctor_check(
    checks: list[DoctorCheck],
    *,
    name: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    check: DoctorCheck = {"name": name, "status": status, "message": message}
    if details is not None:
        check["details"] = details
    checks.append(check)


def _has_failed_check(checks: list[DoctorCheck]) -> bool:
    return any(check["status"] == "fail" for check in checks)


def _build_doctor_result(
    checks: list[DoctorCheck],
    *,
    strict: bool,
    config_path: str,
    effective_config: dict[str, Any] | None = None,
    runtime: ShimRuntime | None = None,
) -> DoctorResult:
    status = "ok"
    if any(check["status"] == "fail" for check in checks):
        status = "fail"
    elif strict and any(check["status"] == "warning" for check in checks):
        status = "fail"
    elif any(check["status"] == "warning" for check in checks):
        status = "warning"
    result: DoctorResult = {
        "service": "localshim",
        "status": status,
        "strict": strict,
        "config_path": config_path,
        "checks": checks,
    }
    if effective_config is not None:
        result["effective_config"] = redact_config(effective_config)
    if runtime is not None:
        result["runtime"] = {
            "generation": runtime.generation,
            "backend_url": runtime.backend_url,
            "runtime_fingerprint": runtime.runtime_fingerprint,
            "upstream_plugins": len(runtime.upstream_plugins),
            "downstream_plugins": len(runtime.downstream_plugins),
        }
    return result


def _print_doctor_result(result: DoctorResult, *, stdout: TextIO, quiet: bool) -> None:
    if quiet and result["status"] == "ok":
        return
    print(f"LocalShim doctor: {result['status']}", file=stdout)
    if quiet:
        for check in result["checks"]:
            if check["status"] != "ok":
                print(
                    f"{check['name']}: {check['status']} - {check['message']}",
                    file=stdout,
                )
        return
    print(f"config_path: {result['config_path']}", file=stdout)
    for check in result["checks"]:
        print(f"{check['name']}: {check['status']} - {check['message']}", file=stdout)
    runtime = result.get("runtime")
    if isinstance(runtime, dict):
        print(f"backend_url: {runtime['backend_url']}", file=stdout)
        print(
            f"plugins: upstream={runtime['upstream_plugins']} downstream={runtime['downstream_plugins']}",
            file=stdout,
        )


_REDACTED_VALUE = "[redacted]"
_SENSITIVE_CONFIG_KEY_FRAGMENTS = (
    "authorization",
    "api_key",
    "apikey",
    "access_token",
    "bearer",
    "cookie",
    "headers",
    "password",
    "secret",
    "token",
)


def _handle_config_render(
    args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO
) -> int:
    try:
        effective_config = _load_effective_config_from_args(args)
    except Exception as error:
        print(f"localshim: config render failed: {error}", file=stderr)
        return EXIT_FAILURE
    rendered_config = (
        effective_config if args.show_secrets else redact_config(effective_config)
    )
    print(json.dumps(rendered_config, indent=2, sort_keys=True), file=stdout)
    return EXIT_SUCCESS


def _load_effective_config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    loaded_config = load_config(args.config_path)
    if not isinstance(loaded_config, dict):
        raise ValueError("Config file must contain a JSON object")
    overrides = AppOverrides(
        host=args.host, port=args.port, backend_url=args.backend_url
    )
    return apply_app_overrides(loaded_config, overrides)


def redact_config(value: Any) -> Any:
    return _redact_config_value(value)


def _redact_config_value(value: Any, *, key: str | None = None) -> Any:
    if key is not None and _is_sensitive_config_key(key):
        return _REDACTED_VALUE
    if isinstance(value, dict):
        return {
            str(child_key): _redact_config_value(child_value, key=str(child_key))
            for (child_key, child_value) in value.items()
        }
    if isinstance(value, list):
        return [_redact_config_value(child_value) for child_value in value]
    return value


def _is_sensitive_config_key(key: str) -> bool:
    normalized_key = key.lower().replace("-", "_").replace(" ", "_")
    return any(
        fragment in normalized_key for fragment in _SENSITIVE_CONFIG_KEY_FRAGMENTS
    )


def _handle_not_implemented(
    args: argparse.Namespace, *, stdout: TextIO, stderr: TextIO
) -> int:
    _ = stdout
    command_path = getattr(args, "command_path", "command")
    print(f"localshim: {command_path!r} is not implemented yet", file=stderr)
    return EXIT_FAILURE


def main(
    argv: Sequence[str] | None = None,
    *,
    stdout: TextIO = sys.stdout,
    stderr: TextIO = sys.stderr,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help(stderr)
        return EXIT_USAGE
    return int(handler(args, stdout=stdout, stderr=stderr))


if __name__ == "__main__":
    raise SystemExit(main())
