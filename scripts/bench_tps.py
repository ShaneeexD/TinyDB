import os
import tempfile
import time

from tinydb_engine import TinyDB


def main() -> None:
    db_path = os.path.join(tempfile.gettempdir(), "tinydb_tps_bench.db")
    if os.path.exists(db_path):
        os.remove(db_path)

    db = TinyDB(db_path)
    try:
        db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, v TEXT)")
        n = 5000

        start = time.perf_counter()
        for i in range(1, n + 1):
            db.execute(f"INSERT INTO bench VALUES ({i}, 'x')")
        insert_seconds = time.perf_counter() - start

        start = time.perf_counter()
        for i in range(1, n + 1):
            db.execute(f"SELECT id FROM bench WHERE id = {i}")
        select_seconds = time.perf_counter() - start

        print(f"Rows per phase: {n}")
        print(f"INSERT TPS: {n / insert_seconds:.2f}")
        print(f"SELECT TPS: {n / select_seconds:.2f}")
        print(f"TOTAL TPS: {2 * n / (insert_seconds + select_seconds):.2f}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
