import pyarrow.parquet as pq

def areFilesIdentical(ifile1: str, ifile2: str, batchsize: int = 100, tolerance: float = 1e-3) -> bool:

    f1 = pq.ParquetFile(ifile1)
    f2 = pq.ParquetFile(ifile2)

    if (f1.metadata.num_rows != f2.metadata.num_rows) or (f1.metadata.num_columns != f2.metadata.num_columns):
        return False

    for df1, df2 in zip(f1.iter_batches(batch_size=batchsize), f2.iter_batches(batch_size=batchsize)):

        ratio = df1.to_pandas() / df2.to_pandas()
        ratio.fillna(1.0, inplace=True)

        for col in ratio.columns:
            if (ratio[col].max() > (1 + tolerance)) or (ratio[col].min() < (1-tolerance)):
                return False 
        return True



