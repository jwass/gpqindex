import itertools
import statistics
import sys
import time

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs


index_path = "omf-internal-usw2/testing/jwasserman/gpqindex/_index.parquet"
dataset_path = "overturemaps-us-west-2/release/2024-08-20.0/theme=buildings/type=building"
bbox = [-71.068, 42.353, -71.058, 42.363]


def _filter(bbox):
    xmin, ymin, xmax, ymax = bbox
    return (
        (pc.field("bbox", "xmin") < xmax)
        & (pc.field("bbox", "xmax") > xmin)
        & (pc.field("bbox", "ymin") < ymax)
        & (pc.field("bbox", "ymax") > ymin)
    )


def _from_pyarrow(path_or_paths, column, filesystem=None):
    # Fetch a single column from the path or paths and apply the bbox filter
    dataset = ds.dataset(path_or_paths, filesystem=filesystem)
    batches = dataset.to_batches(filter=_filter(bbox), columns=[column])
    return list(itertools.chain(*(b.column(0).to_pylist() for b in batches)))


def pyarrow_index():
    # The index is currently behind a non-public location
    filenames = _from_pyarrow(index_path, "filename", filesystem=fs.S3FileSystem(region="us-west-2"))
    ids = _from_pyarrow(
        filenames, "id", filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
    )
    return len(ids)


def pyarrow_no_index():
    ids = _from_pyarrow(
        dataset_path, "id", filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
    )
    return len(ids)


def _from_duckdb(path_or_paths, column):
    q = f"""
    SET s3_region='us-west-2';
    SELECT {column}
    FROM read_parquet({path_or_paths})
    WHERE
        bbox.xmin < {bbox[2]}
        AND bbox.xmax > {bbox[0]}
        AND bbox.ymin < {bbox[3]}
        AND bbox.ymax > {bbox[1]};
    """
    rows = duckdb.sql(q)
    col = [r[0] for r in rows.fetchall()]
    return col


def duckdb_index():
    filenames = _from_duckdb(f"'s3://{index_path}'", "filename")

    duckdb_list = "[{}]".format(",".join([f"'s3://{f}'" for f in filenames]))
    ids = _from_duckdb(duckdb_list, "id")
    return len(ids)


def duckdb_no_index():
    ids = _from_duckdb(f"'s3://{dataset_path}/*.parquet'", "id")
    return len(ids)


def time_it(func, n):
    times = []

    for i in range(n):
        start = time.time()
        func()
        end = time.time()

        dt = end - start
        print(f"Run {i}: {dt} s")
        times.append(end - start)


    median = statistics.median(times)
    return median



if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "--pyarrow-index":
        func = pyarrow_index
    elif cmd == "--pyarrow-no-index":
        func = pyarrow_no_index
    elif cmd == "--duckdb-no-index":
        func = duckdb_no_index
    elif cmd == "--duckdb-index":
        func = duckdb_index
    else:
        raise ValueError("Invalid option")

    t = time_it(func, 5)
    print(f"Median time is {t}")
