import httpx
import requests
from core.errors import BackendRequestError, BackendResponseError
from core.logging import get_logger

logger = get_logger("localshim.forwarder")
HOP_BY_HOP_HEADERS = {
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "upgrade",
}


def forward_request(req_json, backend_url=None):
    if backend_url is None:
        logger.info("Using internal mock forwarder response")
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "mock response via forwarder",
                    }
                }
            ]
        }
    logger.info("Forwarding JSON request to backend: %s", backend_url)
    try:
        res = requests.post(
            f"{backend_url}/v1/chat/completions", json=req_json, timeout=3e1
        )
    except requests.RequestException as error:
        raise BackendRequestError(
            "Failed to reach backend", backend_url=backend_url
        ) from error
    if res.status_code >= 500:
        raise BackendRequestError(
            "Backend returned a retryable HTTP error status",
            backend_url=backend_url,
            status_code=res.status_code,
        )
    try:
        return res.json()
    except ValueError as error:
        raise BackendResponseError(
            "Backend returned a non-JSON response",
            backend_url=backend_url,
            status_code=res.status_code,
        ) from error


def filter_request_headers(headers):
    return {
        key: value
        for (key, value) in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }


def filter_response_headers(headers):
    return {
        key: value
        for (key, value) in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }


def forward_models(backend_url=None):
    if backend_url is None:
        logger.info("Using internal mock models response")
        return {"object": "list", "data": []}, 200, {}
    url = f"{backend_url}/v1/models"
    logger.info("Forwarding models request to backend: %s", url)
    try:
        res = requests.get(url, timeout=3e1)
    except requests.RequestException as error:
        raise BackendRequestError(
            "Failed to reach backend", backend_url=backend_url
        ) from error
    try:
        body = res.json()
    except ValueError as error:
        raise BackendResponseError(
            "Backend returned a non-JSON response",
            backend_url=backend_url,
            status_code=res.status_code,
        ) from error
    return body, res.status_code, filter_response_headers(res.headers)


def build_backend_url(request, backend_url):
    url = f"{backend_url}{request.url.path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"
    return url


async def forward_raw_stream(request, backend_url):
    url = build_backend_url(request, backend_url)
    headers = filter_request_headers(request.headers)
    client = httpx.AsyncClient(timeout=None)
    backend_request = client.build_request(
        method=request.method, url=url, headers=headers, content=request.stream()
    )
    logger.info("Forwarding raw streaming request to backend: %s", url)
    backend_response = await client.send(backend_request, stream=True)
    return client, backend_response


async def forward_raw_body(request, backend_url, body):
    url = build_backend_url(request, backend_url)
    headers = filter_request_headers(request.headers)
    client = httpx.AsyncClient(timeout=None)
    backend_request = client.build_request(
        method=request.method, url=url, headers=headers, content=body
    )
    logger.info("Forwarding raw body request to backend: %s", url)
    backend_response = await client.send(backend_request, stream=True)
    return client, backend_response


async def forward_streaming_json_request(request, backend_url, req_json):
    url = build_backend_url(request, backend_url)
    headers = filter_request_headers(request.headers)
    client = httpx.AsyncClient(timeout=None)
    backend_request = client.build_request(
        method=request.method, url=url, headers=headers, json=req_json
    )
    logger.info("Forwarding streaming JSON request to backend: %s", url)
    backend_response = await client.send(backend_request, stream=True)
    return client, backend_response
