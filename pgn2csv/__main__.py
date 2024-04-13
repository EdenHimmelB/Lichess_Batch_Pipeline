import argparse
from .engine import Converter


def main():
    parser = argparse.ArgumentParser(description="Convert pgn.zst file to csv.")
    parser.add_argument(
        "input", type=str, help="name of the pgn.zst file which needs be converted"
    )
    parser.add_argument("output", type=str, help="name of the converted, csv file")
    args = parser.parse_args()

    Converter.run(args.input, args.output)


if __name__ == "__main__":
    main()
