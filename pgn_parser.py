from unit import Match
import re

TAG_REGEX = re.compile(r'\[(\w+)\s+"([^"]+)"\]')
MOVES_REGEX = re.compile(
    r"((?:\d+\.\s*)?(?:\d+\.\.\.\s*)?\S+)\s*\{\s*\[%clk\s+([0-9:]+)\]\s*\}"
)


class PGNParser:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def parse_pgn(self):
        consecutive_non_tag_lines = 0
        new_match = Match()
        with open(self.file_path, "r") as pgn_file:
            for line in pgn_file:
                tag_match = TAG_REGEX.match(line)
                if consecutive_non_tag_lines > 2:
                    print("create a new game\n")
                    consecutive_non_tag_lines = 0
                    new_match = Match()
                if tag_match:
                    consecutive_non_tag_lines = 0
                    tag_name, tag_value = tag_match.groups()
                    new_match.set_attribute(name=tag_name.lower(), value=tag_value)
                elif len(move_match := MOVES_REGEX.findall(line)) > 0:
                    consecutive_non_tag_lines += 1
                    new_match.set_attribute(name="gamemoves", value=move_match)
                else:
                    consecutive_non_tag_lines += 1
