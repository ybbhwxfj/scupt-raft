import sqlite3
import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description='split a large sqlite database file.')
    parser.add_argument('--rows', type=int,
                        help='an integer for the maximum rows of the split file')
    parser.add_argument('--path',
                        help='the path of the input database')

    args = parser.parse_args()
    db_path = args.path
    max_rows = args.rows
    con = sqlite3.connect(db_path)

    cur = con.cursor()

    db_path = Path(db_path)
    if not db_path.exists():
        print("no such path ?".format(db_path))
        return -1

    stem = db_path.resolve().stem
    base_name = db_path.name
    postfix = base_name.replace(stem, '')
    parent_path = db_path.parent

    n = 1
    offset = 1

    res = cur.execute('''select id, trace_json from trace;''')
    while True:
        c = res.fetchmany(max_rows)
        if len(c) == 0:
            break

        print("write {} rows".format(offset))
        output_db = parent_path.joinpath("{}_{}{}".format(stem, n,  postfix))
        con_new = sqlite3.connect(output_db)
        cur_new = con_new.cursor()
        cur_new.execute(
            '''create table trace (
                id text not null primary key,
                trace_json text not null
            )'''
        )
        for row in c:
            cur_new.execute("insert into trace values(?, ?);", row)
        con_new.commit()

        n += 1
        offset += max_rows
    return 0


if __name__ == '__main__':
    sys.exit(main())
