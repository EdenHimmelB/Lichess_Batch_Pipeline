import csv


class CSVWriter:
    def __init__(self, csv_file_path: str) -> None:
        self.csv_file_path = csv_file_path

    def write_csv(self, processing_queue):
        with open(self.csv_file_path, "w", newline="") as writer:
            csv_writer = csv.writer(self.csv_file_path)
            match_record = processing_queue.get()
            if match_record:
                csv_writer.writerow(
                    []
                )
