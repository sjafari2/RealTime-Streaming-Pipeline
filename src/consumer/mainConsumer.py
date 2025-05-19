import sys
import argparse
import pandas as pd
import KafkaConsumer  # Assumes KafkaConsumer.py is in the same directory

if __name__ == '__main__':
    print("📄 Start getting variables")
    pd.set_option('display.max_colwidth', None)

    parser = argparse.ArgumentParser(description="Run Kafka Consumer")
    parser.add_argument(
        '-topics', '--topics', nargs='+', required=True, dest='topics',
        metavar='TOPICS', help='Topics assigned to the consumer'
    )
    parser.add_argument(
        '-topictitle', '--topictitle', type=str, required=True, dest='topictitle',
        metavar='TOPICTITLE', help='Topic title for this project'
    )
    parser.add_argument(
        '-consindex', '--consindex', type=int, default=0, dest='consindex',
        metavar='CONSINDEX', help='Index of the consumer in its pod'
    )
    parser.add_argument(
        '-hpath', '--h5pypath', type=str, default='./consumer-app-data', dest='h5pypath',
        metavar='H5PYPATH', help='Path to save HDF5/CSR matrix files'
    )
    parser.add_argument(
        '-colrange', '--colrange', type=int, default=500000, dest='colrange',
        metavar='COLRANGE', help='Range of columns for hashing'
    )
    parser.add_argument(
        '-pi', '--podindex', type=int, default=0, dest='podindex',
        metavar='PODINDEX', help='Pod ordinal index'
    )
    parser.add_argument(
        '-uris', '--server_uris', nargs='+', default=['127.0.0.1:9092'], dest='server_uris',
        metavar='KAFKA_URIS', help='Kafka broker URIs'
    )

    args = parser.parse_args(sys.argv[1:])

    # Extract arguments
    topics = args.topics
    topic_title = args.topictitle
    pod_index = args.podindex
    col_range = args.colrange
    file_path = args.h5pypath
    cons_index = args.consindex

    print(f"▶ Assigned topics: {topics}")

    # Create and run the Kafka consumer
    try:
        consumer = KafkaConsumer.Consumer(args.server_uris, pod_index)
    except Exception as ex:
        print(f"❌ Failed to create consumer: {str(ex)}")
        sys.exit(1)

    consumer.consume_stream(
        topicTitle=topic_title,
        topics=topics,
        file_path=file_path,
        col_range=col_range,
        pod_index=pod_index,
        cindex=cons_index
    )

