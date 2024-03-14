import re
import csv
import subprocess

from .match import Match
from multiprocessing import JoinableQueue, Process


TAG_REGEX = re.compile(r'\[(\w+)\s+"([^"]+)"\]')
MOVES_REGEX = re.compile(
    r"(\S+)\s*\{\s*(?:\[%eval\s+(-?\d+\.{1}\d+?|#\d+)\]\s*)?(?:\[%clk\s+(\d+:\d+:\d+)\]\s*)\}"
)


class PGNParser:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self._consecutive_non_tag_lines = 0

    def parse_pgn(self, processing_queue: JoinableQueue) -> None:
        
        match_record = Match()

        with subprocess.Popen(
            ["pzstd", "-dc", self.file_path], stdout=subprocess.PIPE
        ) as proc:
            for line in proc.stdout:
                decoded_line = line.decode()
                tag_match = TAG_REGEX.match(decoded_line)

                # If self._consecutive_non_tag_lines > 2, it means that 3 lines have been parsed
                # (1 blank line, moves line/ result line, another blank line)
                # which indicates an entirely different game has been reached.
                if self._consecutive_non_tag_lines > 2:
                    processing_queue.put(match_record)
                    self._consecutive_non_tag_lines = 0
                    previous_match_record = match_record
                    match_record = Match()

                # This block indicates a tag line has been parsed
                if tag_match:
                    self._consecutive_non_tag_lines = 0
                    tag_name, tag_value = tag_match.groups()
                    match_record.set_attribute(name=tag_name.lower(), value=tag_value)

                # This block indicates a moves line has been parsed
                elif move_match := MOVES_REGEX.findall(decoded_line):
                    self._consecutive_non_tag_lines += 1
                    moves = [{"move": move[0], "eval": move[1], "time": move[2]} for move in move_match]
                    match_record.set_attribute(name="gamemoves", value=moves)

                # This block indicates a blank line has been parsed
                else:
                    self._consecutive_non_tag_lines += 1

        # If the last match record is an incomplete record due to file partitioned,
        # it would not be pushed.
        # This block will push that last record to the processing queue.
        if previous_match_record != match_record:
            processing_queue.put(match_record)

        processing_queue.join()


class CSVWriter:
    def __init__(self, csv_file_path: str) -> None:
        self.csv_file_path = csv_file_path

    def write_csv(self, processing_queue: JoinableQueue):
        with open(self.csv_file_path, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
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

            while True:
                match_record: Match = processing_queue.get()
                if match_record is None:
                    processing_queue.task_done()
                    break
                csv_writer.writerow(
                    [
                        match_record.game_id,
                        match_record.event,
                        match_record.site,
                        match_record.date,
                        match_record.round,
                        match_record.white,
                        match_record.black,
                        match_record.result,
                        match_record.utcdate,
                        match_record.utctime,
                        match_record.whiteelo,
                        match_record.blackelo,
                        match_record.whiteratingdiff,
                        match_record.blackratingdiff,
                        match_record.whitetitle,
                        match_record.blacktitle,
                        match_record.eco,
                        match_record.opening,
                        match_record.timecontrol,
                        match_record.termination,
                        match_record.gamemoves,
                    ]
                )
                processing_queue.task_done()

class Converter:
    @staticmethod
    def run(input_file_path: str, target_file_path: str):
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