from pgn_parser import PGNParser
from multiprocessing import JoinableQueue, Process


def print_from_queue(processing_queue: JoinableQueue):
    while True:
        task = processing_queue.get()
        if task is None:  # Use None as a signal to stop
            processing_queue.task_done()
            break
        print(f"Processed {task}")
        processing_queue.task_done()


def main():
    processing_queue = JoinableQueue()
    parser = PGNParser(file_path="data/lichess_db_standard_rated_2024-01.pgn")
    process_1 = Process(
        target=parser.parse_pgn,
        kwargs=dict(processing_queue=processing_queue),
    )
    process_2 = Process(target=print_from_queue, args=(processing_queue,))

    # Start processes
    process_1.start()
    process_2.start()

    # Wait for the parser to finish
    process_1.join()

    # Signal the print process to stop by adding None to the queue
    processing_queue.put(None)

    # Wait for the print process to finish
    process_2.join()


if __name__ == "__main__":
    main()
