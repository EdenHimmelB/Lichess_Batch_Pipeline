from unit import Match
import re

TAG_REGEX = re.compile(r'\[(\w+)\s+"([^"]+)"\]')
MOVES_REGEX = re.compile(
    r"((?:\d+\.\s*)?(?:\d+\.\.\.\s*)?\S+)\s*\{\s*\[%clk\s+([0-9:]+)\]\s*\}"
)


class PGNParser:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    @staticmethod
    def parse_pgn(self, processing_queue) -> None:
        consecutive_non_tag_lines = 0
        match_record = Match()
        with open(self.file_path, "r") as pgn_reader:
            for line in pgn_reader:
                tag_match = TAG_REGEX.match(line)
                if consecutive_non_tag_lines > 2:
                    print("create a new game\n")
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
                    processing_queue.put(match_record)
