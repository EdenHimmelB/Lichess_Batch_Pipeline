import argparse
from .engine import Converter

def main():
    parser = argparse.ArgumentParser(description="Convert pgn.zst file to csv.")
    parser.add_argument(
        "input", type=str, help="name of the pgn.zst file which needs be converted"
    )
    args = parser.parse_args()
    converted_file_name = args.input.split(".")[0] + ".csv"
    
    # print(args.input, converted_file_name)
    Converter.run(args.input, converted_file_name)

if __name__ == '__main__':
    main()