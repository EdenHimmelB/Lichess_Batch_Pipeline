import re
import csv
import subprocess
from threading import Thread

from .match import Match
from multiprocessing import JoinableQueue, Process

from google.cloud import storage

TAG_REGEX = re.compile(r'\[(\w+)\s+"([^"]+)"\]')
COMPLEX_MOVES_REGEX = re.compile(
    r"""
    (\S+)\s*\{\s*(?:\[%eval\s+(-?\d+\.{1}\d+?|\#\d+)\]\s*)?(?:\[%clk\s+(\d+:\d+:\d+)\]\s*)\}
    """,
    re.VERBOSE,
)
BASIC_MOVES_REGEX = re.compile(
    r"""
    [NBKRQ]?[a-h]?[1-8]?[\-x]?[a-h][1-8](?:=?[nbrqkNBRQK])?|[PNBRQK]?@[a-h][1-8]|--|Z0|0000|@@@@|O-O(?:-O)?|0-0(?:-0)?
    """,
    re.VERBOSE,
)


class PGNParser:
    def __init__(self, file_path: str, blob) -> None:
        self.file_path = file_path
        self._consecutive_non_tag_lines = 0
        self._blob = blob

    def write_to_proc(self, proc, blob_stream):
        for chunk in blob_stream:
            proc.stdin.write(chunk)
        proc.stdin.close()

    def read_from_proc(self, proc, processing_queue: JoinableQueue) -> None:
        match_record = Match()
        previous_match_record = None
        record = 0
        while True:
            output_chunk = proc.stdout.readline()
            if not output_chunk:
                break
            decoded_line = output_chunk.decode()

            # If self._consecutive_non_tag_lines > 2, it means that 3 lines have been parsed
            # (1 blank line, moves line/ result line, another blank line)
            # which indicates an entirely different game has been reached.
            if self._consecutive_non_tag_lines > 2:
                processing_queue.put(match_record)
                record += 1
                print(f"process {record}")
                self._consecutive_non_tag_lines = 0
                previous_match_record = match_record
                match_record = Match()

            # This block indicates a tag line has been parsed
            if tag_match := TAG_REGEX.match(decoded_line):
                self._consecutive_non_tag_lines = 0
                tag_name, tag_value = tag_match.groups()
                match_record.set_attribute(name=tag_name.lower(), value=tag_value)

            # If not tag line, will next check if it's moves line with or without comments
            elif move_match := COMPLEX_MOVES_REGEX.findall(decoded_line):
                self._consecutive_non_tag_lines += 1
                moves = [
                    {"move": move[0], "eval": move[1], "time": move[2]}
                    for move in move_match
                ]
                match_record.set_attribute(name="gamemoves", value=moves)

            elif move_match := BASIC_MOVES_REGEX.findall(decoded_line):
                self._consecutive_non_tag_lines += 1
                moves = [{"move": move} for move in move_match]
                match_record.set_attribute(name="gamemoves", value=moves)

            # Empty line or else will be ignored.
            else:
                self._consecutive_non_tag_lines += 1

        # Don't forget to add the last match record processing here
        if previous_match_record != match_record:
            processing_queue.put(match_record)
            print(f"process {match_record}")

    def parse_pgn(self, processing_queue: JoinableQueue) -> None:
        with subprocess.Popen(
            ["pzstd", "-dc"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=-1
        ) as proc, self._blob.open("rb") as blob_stream:
            
            # Start a thread for writing to the subprocess
            writer_thread = Thread(target=self.write_to_proc, args=(proc, blob_stream))
            writer_thread.start()
            
            # Read from the subprocess in the main thread
            self.read_from_proc(proc, processing_queue)
            
            # Wait for the writer thread to complete
            writer_thread.join()
            processing_queue.join()


class CSVWriter:
    def __init__(self, file_path: str, blob) -> None:
        self.file_path = file_path
        self._blob = blob

    def write_csv(self, processing_queue: JoinableQueue):
        with self._blob.open("w", newline="", encoding="utf-8") as csv_file:
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
    def run(input_file_path: str, output_file_path: str):
        processing_queue = JoinableQueue(maxsize=100000)

        bucket_name = input_file_path.split("/")[2]
        input_blob_name = "/".join(input_file_path.split("/")[3:])
        output_blob_name = "/".join(output_file_path.split("/")[3:])

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        input_blob = bucket.blob(input_blob_name)
        output_blob = bucket.blob(output_blob_name)

        parser = PGNParser(input_file_path, input_blob)
        csv_writer = CSVWriter(output_file_path, output_blob)

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