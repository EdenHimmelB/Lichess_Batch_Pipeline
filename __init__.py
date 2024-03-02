from multiprocessing import JoinableQueue, Process
from engine import PGNParser, CSVWriter


class Runner:
    @staticmethod
    def work(input_file_path: str, target_file_path: str):
        processing_queue = JoinableQueue()
        parser = PGNParser(input_file_path)
        csv_writer = CSVWriter(target_file_path)

        process_1 = Process(
            target=parser.parse_pgn,
            kwargs=dict(processing_queue=processing_queue),
        )
        process_2 = Process(
            target=csv_writer.write_csv,
            kwargs=dict(processing_queue=processing_queue),
        )

        # Start processes
        process_1.start()
        process_2.start()

        # Wait for the parser to finish
        process_1.join()

        # Signal the CSVWrite process to stop by adding None to the queue
        processing_queue.put(None)

        # Wait for the print process to finish
        process_2.join()
