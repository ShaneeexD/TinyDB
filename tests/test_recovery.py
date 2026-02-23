import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine.storage.pager import PAGE_SIZE, Pager
from tinydb_engine.wal.wal import WAL


def test_recovery_replays_only_committed_writes(tmp_path):
    db_path = tmp_path / "recovery.db"

    wal = WAL(str(db_path))
    pager = Pager(str(db_path), wal=wal)
    page_id = pager.allocate_page()
    pager.commit()
    pager.close()

    committed_image = bytes([7]) * PAGE_SIZE
    wal = WAL(str(db_path))
    wal.begin()
    wal.log_page_write(page_id, committed_image)
    wal.commit()

    uncommitted_image = bytes([9]) * PAGE_SIZE
    wal.begin()
    wal.log_page_write(page_id, uncommitted_image)
    wal.abort()

    recovered = Pager(str(db_path), wal=WAL(str(db_path)))
    try:
        assert recovered.read_page(page_id) == committed_image
    finally:
        recovered.close()
