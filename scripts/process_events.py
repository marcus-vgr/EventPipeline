from argparse import ArgumentParser

from EventProcessor.processor import DataProcessor

parser = ArgumentParser(prog="process_events.py", description="Script to process events")
parser.add_argument("-i", "--input", type=str, required=True, help="<path/to/input.parquet>")
parser.add_argument("-o", "--output", type=str, required=False, help="<path/to/output.parquet> ; If not is given, it will use .../input_filtered.parquet")
parser.add_argument("-m", "--method", type=str, required=False, default="py", choices=["py", "cpp"], help="Which backend to use.")

args = parser.parse_args()

test = DataProcessor(args.input, args.output, args.method)
test.run()