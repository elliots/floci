"""Real gRPC server with opaque byte messages for the on-demand compatibility tests."""

from concurrent import futures
import socket
import time

import grpc


def metadata(context):
    context.send_initial_metadata((('x-instance', socket.gethostname()), ('initial-bin', b'\x00\xff')))
    context.set_trailing_metadata((('result', 'complete'), ('grpc-status-details-bin', b'\x01\x02')))


def unary(request, context):
    metadata(context)
    if request == b'error':
        context.abort(grpc.StatusCode.PERMISSION_DENIED, 'denied here')
    if request == b'slow':
        while context.is_active():
            time.sleep(0.05)
        return b''
    headers = dict(context.invocation_metadata())
    if request != b'ready' and headers.get('authorization') != 'Bearer integration-test':
        context.abort(grpc.StatusCode.UNAUTHENTICATED, 'missing authorization')
    return request


def server_stream(request, context):
    metadata(context)
    for index in range(6):
        if not context.is_active():
            return
        yield request + bytes([index])
        time.sleep(1)


def client_stream(requests, context):
    metadata(context)
    return b''.join(requests)


def bidi(requests, context):
    metadata(context)
    for request in requests:
        yield request


server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('test.Echo', {
    'Unary': grpc.unary_unary_rpc_method_handler(unary),
    'ServerStream': grpc.unary_stream_rpc_method_handler(server_stream),
    'ClientStream': grpc.stream_unary_rpc_method_handler(client_stream),
    'Bidi': grpc.stream_stream_rpc_method_handler(bidi),
}),))
server.add_insecure_port('[::]:50051')
server.start()
server.wait_for_termination()
