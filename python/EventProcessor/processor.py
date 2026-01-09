import time
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np


class DataProcessor:

    def __init__(self, ifile: str, ofile: str, method: str, momentum_min: float=0.2, momentum_max: float=1.0, radius_max: float=100):
        self.ifile = ifile
        self.ofile = ofile
        if self.ofile is None:
            self.ofile = self.ifile.replace(".parquet", "_filtered.parquet")
        self.method = method
        print("Input: ", self.ifile)
        print("Output: ", self.ofile)
        print("Method: ", self.method)

        self.momentum_min = momentum_min
        self.momentum_max = momentum_max
        self.radius_max = radius_max

        self.EPSILON = 1e-6
        self.BATCHSIZE = 5000


    def run(self):
        if self.method == "py":
            self._py_run()

    def _py_filter_dataframe(self, dataframe: pd.DataFrame):
        dataframe["radius"] = np.sqrt(dataframe["x"]**2 + dataframe["y"]**2)
        dataframe["momentum_proxy"] = dataframe["energy"] / (dataframe["radius"] + self.EPSILON)
        dataframe["time_residual"] = dataframe["timestamp"] - dataframe.groupby("detector_id")["timestamp"].transform("mean")
        dataframe["passFilter"] = (dataframe["momentum_proxy"] >= self.momentum_min) & (dataframe["momentum_proxy"] <= self.momentum_max) & (dataframe["radius"] <= self.radius_max)
        
        dataframe.query("passFilter", inplace=True)

    def _py_run(self):

        tInit = time.time()

        nEventsTotal = 0
        nEventsFilter = 0
        
        writer = None
        reader = pq.ParquetFile(self.ifile)
        
        for df in reader.iter_batches(batch_size=self.BATCHSIZE):
        
            df = df.to_pandas()
            nEventsTotal += len(df)
            self._py_filter_dataframe(df)
            nEventsFilter += len(df)
        
            table = pa.Table.from_pandas(df)
            if writer is None:
                writer = pq.ParquetWriter(self.ofile, table.schema)
            writer.write_table(table)
        
        writer.close()
        
        print("NEventsInit: ", nEventsTotal, "| NEventsFiltered: ", nEventsFilter, f"|| Efficiency: {nEventsFilter/nEventsTotal * 100:.2f}%")
        timeRunning = time.time() - tInit
        print(f"Script run in {timeRunning:.2f}s.")