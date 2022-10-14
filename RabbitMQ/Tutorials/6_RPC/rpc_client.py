from time import time
import pika
import sys
import uuid


class FibonacciRpcClient(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(n))
        timeout_in_seconds = 5
        self.connection.process_data_events(time_limit=timeout_in_seconds)
        try:
            return int(self.response)
        except TypeError as e:
            print(
                f" [TIMEOUT] Request may have timed out after waiting for {timeout_in_seconds} s. RPC Server may be offline.", file=sys.stderr)
            return None


fibonacci_rpc = FibonacciRpcClient()

print(" [Client] Requesting fib(%s)" %
      (sys.argv[1] if len(sys.argv) > 1 else 30))
response = fibonacci_rpc.call(sys.argv[1]) if len(
    sys.argv) > 1 else fibonacci_rpc.call(30)
print(" [x] Got %r" % response)
