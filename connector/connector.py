import requests
import json
import logging
import os
import sys

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from kafka import KafkaProducer
import multiprocessing as mp


def setup_custom_logger(filename):
    """Set configuration for logging"""

    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)

    # set file output handler and formatter for that
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # set console output handler and formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s -  %(message)s'))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def worker_func(vacancy_id):
    """Thread function for doing requests to API"""
    url = REQUESTS_URL_PATTERN % vacancy_id
    try:
        # do request and decode body
        response = requests.get(url)
        content = response.content.decode('utf-8')

        return {'status_code': response.status_code, 'id': vacancy_id, 'content': content}

    except:
        # if there is an error, return 0 as a status_code and id of vacancy
        LOGGER.error('An error occurred on id {}'.format(vacancy_id))
        return {'status_code': 0, 'id': vacancy_id}


def handle_error_data(error_data, queue):
    """Handle data that was returned with an error"""
    for item in error_data:
        queue.put(int(item['id']))


def send_to_kafka(correct_data):
    """Handle correct data from futures and send it to Kafka"""

    # init kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,
                                   value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    sent = 0

    for elem in correct_data:
        try:
            # try to send data to kafka
            message = elem['content']
            future = kafka_producer.send(KAFKA_TOPIC, message)
            future.get(timeout=5)
            sent += 1
        except:
            LOGGER.error('An error occurred on id {}'.format(elem['id']))

    # finally flush data
    kafka_producer.flush()
    LOGGER.info(
        ' {}/{} messages have been sent to Kafka, Kafka topic: {}'.format(sent, len(correct_data),
                                                                          KAFKA_TOPIC))


def handle_not_done_requests(not_done_futures):
    """Handle futures that failed"""
    pass


def sort_done_requests(done_requests):
    """Handle futures that done"""

    correct_data, incorrect_data, error_data = [], [], []

    for future in done_requests:
        future_body = future.result()

        # if requests was successful
        if future_body['status_code'] is 200:
            correct_data.append(future_body)

        # if requests was failed with a connection error
        elif future_body['status_code'] is 0:
            error_data.append(future_body)

        # if requests failed with errors like 404, 403 and etc..
        else:
            incorrect_data.append(future_body)

    return correct_data, incorrect_data, error_data


def handler_func(done, not_done, queue):
    """Thread handler function"""

    LOGGER.info('Done requests: {}, Not done requests {}'.format(len(done), len(not_done)))

    # handle not done requests
    handle_not_done_requests(not_done)

    # sort requests by their status
    correct_data, incorrect_data, error_data = sort_done_requests(done)

    LOGGER.info(
        'Correct messages: {},Incorrect messages: {}, Error messages {}'.format(len(correct_data), len(incorrect_data),
                                                                                len(error_data)))
    # send correct data to kafka cluster
    send_to_kafka(correct_data)

    handle_error_data(error_data, queue)


def start_jobs(ids, worker_func, workers_number=6):
    """Start workers with specified range of indexes"""
    with ThreadPoolExecutor(max_workers=workers_number) as executor:
        # do requests asynchronously
        futures = [executor.submit(worker_func, i) for i in ids]

        # wait for all threads to finish executing with specified timeout
        done, not_done = wait(futures, timeout=REQUESTS_TIMEOUT, return_when=ALL_COMPLETED)

    return done, not_done



if __name__ == "__main__":

    START_ID = int(sys.argv[1])

    FINISH_ID = int(sys.argv[2])

    STEP = int(sys.argv[3])

    REQUESTS_TIMEOUT = int(os.environ.get('REQUESTS_TIMEOUT'))

    REQUESTS_URL_PATTERN = os.environ.get('REQUESTS_URL_PATTERN')

    CONNECTOR_LOGS = os.environ.get('CONNECTOR_LOGS')



    KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
    KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS').split(' ')
    print(KAFKA_BROKERS)

    queue = mp.Queue()

    current = START_ID

    LOGGER = setup_custom_logger(CONNECTOR_LOGS)

    while current < FINISH_ID:
        # check queue size and if it is bigger or equal to a step, use values from queue
        LOGGER.info('{} elements are waiting to be downloaded'.format(queue.qsize()))
        if queue.qsize() > STEP:
            LOGGER.info('Downloading elements from queue...')

            # take N elements from queue
            ids = [queue.get() for _ in range(STEP)]

        else:
            LOGGER.info('Downloading elements from range {} to {}...'.format(current, current + STEP))

            ids = range(current, current + STEP)
            current += STEP

        # start jobs
        done_futures, not_done_futures = start_jobs(ids, worker_func=worker_func, workers_number=10)

        # start handler for workers results
        futures_handler = mp.Process(target=handler_func, args=(done_futures, not_done_futures, queue))
        futures_handler.start()


