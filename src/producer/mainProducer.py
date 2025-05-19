import sys
import pandas as pd
import argparse
import KafkaProducer  # Assumes KafkaProducer.py is in the same directory

if __name__ == '__main__':
    print("Get Input Values")
    pd.set_option('display.max_colwidth', None)

    parser = argparse.ArgumentParser(description="Run Kafka Producer")
    parser.add_argument('-topicTitle', '--topicTitle', type=str, dest='topicTitle', default='test', help='Kafka Topic Title')
    parser.add_argument('-np', '--nprod', type=int, dest='nprod', default=1, help='Number of Producers per Pod')
    parser.add_argument('-nt', '--ntopics', type=int, dest='ntopics', default=10, help='Number of Kafka Topics')
    parser.add_argument('-pi', '--podindex', type=int, dest='podindex', default=0, help='Pod Ordinal Index')
    parser.add_argument('-pri', '--prodindex', type=int, dest='prodindex', default=0, help='Producer Index')
    parser.add_argument('-inputpath', '--inputpath', type=str, dest='inputpath', default="/data", help='Input Path')
    parser.add_argument('-bs', '--bsize', type=int, dest='bsize', default=100, help='Batch Size')
    parser.add_argument('-wtime', '--wtime', type=int, dest='wtime', default=10, help='Wait Time in Seconds')
    parser.add_argument('-cr', '--colrange', type=int, dest='colrange', default=500000, help='Range for Hashing Words')
    parser.add_argument('-uris', '--uris', nargs='+', dest='uris', default=['127.0.0.1:9092'], help='Kafka Server URIs')

    args = parser.parse_args(sys.argv[1:])

    print("Create Producer Object")
    producer = KafkaProducer.Producer(serveruri=args.uris)

    print(" Start Streaming Data")
    producer.stream_data(
        wait_time=args.wtime,
        topicTitle=args.topicTitle,
        nprod=args.nprod,
        num_topics=args.ntopics,
        podindex=args.podindex,
        prodindex=args.prodindex,
        batchsize=args.bsize,
        column_range=args.colrange,
        input_path=args.inputpath
    )

