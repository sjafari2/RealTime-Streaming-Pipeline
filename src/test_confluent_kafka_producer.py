\
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Your original producer + optional burst pacing.
Default behavior unchanged (per-message pacing). Enable with --pacingMode=burst.
"""

import argparse, time, sys, threading
from confluent_kafka import Producer

shutdown_event = threading.Event()

def sig_handler(*_):
    shutdown_event.set()

try:
    import signal
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
except Exception:
    pass

def build_arg_parser():
    p = argparse.ArgumentParser(description="Kafka producer (with optional burst pacing)")
    # ---- existing args you already have ----
    p.add_argument("--topicTitle", required=True)
    p.add_argument("--numTopics", type=int, default=1)
    p.add_argument("--delay", type=float, default=0.0)
    p.add_argument("--numPartitions", type=int, default=36)
    p.add_argument("--replica", type=int, default=1)
    p.add_argument("--randomRange", type=int, default=1000)
    p.add_argument("--lingerMs", type=int, default=10)
    p.add_argument("--compressionType", default="lz4")
    p.add_argument("--batchSize", type=int, default=262144)
    p.add_argument("--msgMaxBytes", type=int, default=1048576)
    p.add_argument("--metadataMaxAgeMs", type=int, default=120000)
    p.add_argument("--topicMetadataRefreshIntervalMs", type=int, default=120000)
    p.add_argument("--maxInFlightRequestsPerConnection", type=int, default=10)
    p.add_argument("--acks", default="0", choices=["0", "1"])
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--retryBackoffMs", type=int, default=5)
    p.add_argument("--reconnectBackoffMs", type=int, default=100)
    p.add_argument("--reconnectBackoffMaxMs", type=int, default=1000)
    p.add_argument("--minInSync", type=int, default=1)
    p.add_argument("--requestTimeoutMs", type=int, default=10000)
    p.add_argument("--deliveryTimeoutMs", type=int, default=120000)
    p.add_argument("--queueBufferingMaxMessages", type=int, default=200000)
    p.add_argument("--queueBufferingMaxKbytes", type=int, default=2097152)
    p.add_argument("--targetRate", type=float, default=250.0)
    p.add_argument("--connectionsMaxIdleMs", type=int, default=30000)
    p.add_argument("--socketKeepaliveEnable", action="store_true")
    # ---- NEW: optional burst pacing ----
    p.add_argument("--pacingMode", choices=["per_message", "burst"], default="per_message")
    p.add_argument("--pacingTickSec", type=float, default=0.01)
    p.add_argument("--minBurst", type=int, default=1)
    p.add_argument("--maxBurst", type=int, default=2000)
    return p

class MyProducer:
    def __init__(self, args):
        self.args = args
        self.producer = Producer({
            "bootstrap.servers": "kafka:9092",
            "batch.size": args.batchSize,
            "linger.ms": args.lingerMs,
            "compression.type": args.compressionType,
            "acks": args.acks,
            "message.max.bytes": args.msgMaxBytes,
            "metadata.max.age.ms": args.metadataMaxAgeMs,
            "topic.metadata.refresh.interval.ms": args.topicMetadataRefreshIntervalMs,
            "max.in.flight.requests.per.connection": args.maxInFlightRequestsPerConnection,
            "retries": args.retries,
            "retry.backoff.ms": args.retryBackoffMs,
            "reconnect.backoff.ms": args.reconnectBackoffMs,
            "reconnect.backoff.max.ms": args.reconnectBackoffMaxMs,
            "request.timeout.ms": args.requestTimeoutMs,
            "delivery.timeout.ms": args.deliveryTimeoutMs,
            "queue.buffering.max.messages": args.queueBufferingMaxMessages,
            "queue.buffering.max.kbytes": args.queueBufferingMaxKbytes,
            "connections.max.idle.ms": args.connectionsMaxIdleMs,
            "socket.keepalive.enable": args.socketKeepaliveEnable,
        })
        self.topic = args.topicTitle
        self.sent_total = 0
        self.target_rate = float(args.targetRate)
        # pacing
        self.pacing_mode = args.pacingMode
        self.pacing_tick = float(args.pacingTickSec)
        self.min_burst = int(args.minBurst)
        self.max_burst = int(args.maxBurst)
        self._burst_start = time.perf_counter()

    def send_one(self):
        self.producer.produce(self.topic, value=b"x" * 32768)
        self.sent_total += 1

    def _burst_to_send(self):
        now = time.perf_counter()
        should_have = int((now - self._burst_start) * self.target_rate)
        to_send = should_have - self.sent_total
        if to_send < self.min_burst: to_send = self.min_burst
        if to_send > self.max_burst: to_send = self.max_burst
        return max(0, to_send)

    def run(self):
        start = time.perf_counter()
        last_log = start
        while not shutdown_event.is_set():
            # always send at least one
            self.send_one()
            # pacing
            if self.pacing_mode == "burst":
                to_send = self._burst_to_send()
                for _ in range(max(0, to_send - 1)):  # we already sent one
                    self.send_one()
                self.producer.poll(0)
                shutdown_event.wait(self.pacing_tick)
            else:
                # original per-message sleep style
                per = 1.0 / max(1e-9, self.target_rate)
                shutdown_event.wait(per)
                self.producer.poll(0)
            # metrics
            now = time.perf_counter()
            if now - last_log >= 2.0:
                elapsed = now - start
                rate = self.sent_total / elapsed if elapsed > 0 else 0.0
                mbps = (self.sent_total * 32768) / (1024*1024) / elapsed if elapsed > 0 else 0.0
                print(f"[METRIC] Elapsed: {elapsed:.2f}s | Sent: {self.sent_total} msgs | Rate: {rate:.2f} msg/s | Throughput: {mbps:.4f} MB/s | Avg Msg Size: 32768.00 bytes")
                last_log = now
        self.producer.flush()
        print("[INFO] Producer stopped.")

def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    prod = MyProducer(args)
    prod.run()

if __name__ == "__main__":
    sys.exit(main())
