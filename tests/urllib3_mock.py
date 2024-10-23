from http import client
from io import BytesIO

from urllib3.response import HTTPResponse


class MockSock(object):
    @classmethod
    def makefile(cls, *args, **kwargs):
        return


def mockChunckedBody(content: str) -> client.HTTPResponse:
    body = "".join([f"{hex(len(s))}\r\n{s}\r\n" for s in content])
    body = body.encode("utf-8") + b"0x0\r\n\r\n"
    resp = client.HTTPResponse(MockSock)
    resp.fp = BytesIO(body)
    resp.chunked = True
    resp.chunk_left = None
    return resp


def mockHttpResponse(method: str, url: str, **kwargs: dict) -> HTTPResponse:
    resp = HTTPResponse(
        headers={
            "X-ClickHouse-Summary": "{}",
            "X-ClickHouse-Query-Id": "test_query",
            "transfer-encoding": "chunked",
        },
        status=200,
        version=0,
        version_string="0",
        reason="test",
        preload_content=False,
        decode_content=False,
        request_url=url,
    )
    if method == "POST":
        if kwargs.get("body", "") == b"SELECT version(), timezone()":
            resp._body = b"24\tEurope/Moscow\n"
        elif b"FROM system.settings" in kwargs.get(
            "body", b""
        ) or b"SELECT 1 AS check" in kwargs.get("body", b""):
            resp._fp = mockChunckedBody(["\x00", "\x00"])
    return resp
