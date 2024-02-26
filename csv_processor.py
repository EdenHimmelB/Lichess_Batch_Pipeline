import csv


class CSVWriter:
    def __init__(self, csv_file_path: str) -> None:
        self.csv_file_path = csv_file_path

    def write_csv(self, processing_queue):
        with open(self.csv_file_path, "w", newline="") as writer:
            csv_writer = csv.writer(self.csv_file_path)
            csv_writer.writerow(
                [
                    "GameID",
                    "Event",
                    "Site",
                    "Date",
                    "Round",
                    "White",
                    "Black",
                    "Result",
                    "UTCDate",
                    "UTCTime",
                    "WhiteElo",
                    "BlackElo",
                    "WhiteRatingDiff",
                    "BlackRatingDiff",
                    "WhiteTitle",
                    "BlackTitle",
                    "ECO",
                    "Opening",
                    "TimeControl",
                    "Termination",
                    "GameMoves",
                ]
            )
            match_record = processing_queue.get()
            if match_record:
                csv_writer.writerow([

                ])
            