import re
import csv
from unit import Match
from multiprocessing import JoinableQueue


TAG_REGEX = re.compile(r'\[(\w+)\s+"([^"]+)"\]')
MOVES_REGEX = re.compile(
    r"(\S+)\s*\{\s*(?:\[%eval\s+(-?\d+\.{1}\d+?|#\d+)\]\s*)?(?:\[%clk\s+(\d+:\d+:\d+)\]\s*)\}"
)


class PGNParser:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def parse_pgn(self, processing_queue: JoinableQueue) -> None:
        consecutive_non_tag_lines = 0
        match_record = Match()
        with open(self.file_path, "r") as pgn_reader:
            for line in pgn_reader:
                tag_match = TAG_REGEX.match(line)
                if consecutive_non_tag_lines > 2:
                    processing_queue.put(match_record)
                    consecutive_non_tag_lines = 0
                    match_record = Match()
                if tag_match:
                    consecutive_non_tag_lines = 0
                    tag_name, tag_value = tag_match.groups()
                    match_record.set_attribute(name=tag_name.lower(), value=tag_value)
                elif len(move_match := MOVES_REGEX.findall(line)) > 0:
                    consecutive_non_tag_lines += 1
                    match_record.set_attribute(name="gamemoves", value=move_match)
                else:
                    consecutive_non_tag_lines += 1
        processing_queue.join()


class CSVWriter:
    def __init__(self, csv_file_path: str) -> None:
        self.csv_file_path = csv_file_path

    def write_csv(self, processing_queue: JoinableQueue):
        with open(self.csv_file_path, "w", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
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
                print(f"Processed {match_record.game_id}")
                processing_queue.task_done()
