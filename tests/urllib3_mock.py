from io import BytesIO
from http.client import HTTPResponse


class MockSock(object):
    @classmethod
    def makefile(cls, *args, **kwargs):
        return
    

def mockChunckedBody(content: bytes = None):
    body = "".join([f"{hex(len(s))}\r\n{s}\r\n" for s in content])
    body = body.encode("utf-8") + b"0x0\r\n\r\n"
    resp = HTTPResponse(MockSock)
    resp.fp = BytesIO(body)
    resp.chunked = True
    resp.chunk_left = None
    return resp
