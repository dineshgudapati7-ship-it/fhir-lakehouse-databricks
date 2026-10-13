"""Write API responses to the raw UC Volume exactly as received."""
import os


def raw_file_path(volume_path, resource, fmt, load_date, run_id, page_number):
    """/Volumes/<catalog>/raw/landing/<Resource>/<fmt>/load_date=YYYY-MM-DD/run_id=<id>/page_0001.<fmt>"""
    return (f"{volume_path}/{resource}/{fmt}/load_date={load_date}/run_id={run_id}/"
            f"page_{page_number:04d}.{fmt}")


def write_raw(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path
