"""
Here we will generate prototype data to be used in the project.
We want to generate, for each UNIQUE event_id, the following variables:
- timestamp: uniform(0,1000)
- x, y: normal(0,100)
- energy: exponential(scale=15)
- detector_id: uniform integer [1,...,64]
"""

import numpy as np
import pyarrow as pa
from pyarrow import parquet as pq
import sys

class DataGenerator:

    def __init__(self, nEvents: int, output: str):
        self.nEvents = nEvents
        self.output = output
        if not self.output.endswith(".parquet"):
            self.output += ".parquet"

        self.CHUNKSIZE = 5000

    def generate_chunck(self, start_id: int, size: int):
        return {
            "event_id": np.arange(start_id, start_id+size, dtype=np.int64),
            "timestamp": np.random.uniform(0,1000,size=size).astype(np.float32),
            "x": np.random.normal(0,100,size=size).astype(np.float32),
            "y": np.random.normal(0,100,size=size).astype(np.float32),
            "energy": np.random.exponential(scale=15,size=size).astype(np.float32),
            "detector_id": np.random.randint(1,64,size=size).astype(np.int32), 
        } 


    def generate_data(self):
                
        print(f"Generating {self.nEvents} events and saving in {self.output}.")

        writer = None

        producedEvents = 0
        while producedEvents < self.nEvents:
            size = self.CHUNKSIZE if producedEvents + self.CHUNKSIZE <= self.nEvents else self.nEvents - producedEvents

            data = self.generate_chunck(producedEvents, size)
            table = pa.table(data)

            if writer is None:
                writer = pq.ParquetWriter(self.output, table.schema)

            writer.write_table(table)

            producedEvents += size
            print(producedEvents, "events produced")

        writer.close()
        print("Done!")


def main():
    
    if len(sys.argv) != 3:
        print("Please run python generate_data.py <nEvents> <ofile>")
        return
    
    nEvents = int(sys.argv[1])
    output = sys.argv[2]
    gen = DataGenerator(nEvents, output)
    gen.generate_data()


if __name__ == "__main__":
    main()
    