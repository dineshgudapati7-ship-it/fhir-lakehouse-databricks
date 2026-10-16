import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "lib"))

import gold  # noqa: E402

OBJECTS = gold.gold_objects("S.", "G.", "O.")


def test_names_unique_and_kinds_valid():
    names = [name for name, _, _ in OBJECTS]
    assert len(names) == len(set(names))
    assert {kind for _, kind, _ in OBJECTS} <= {"table", "view"}


def test_objects_only_reference_earlier_gold_objects():
    seen = set()
    for name, _, sql in OBJECTS:
        refs = set(re.findall(r"\bG\.(\w+)", sql))
        assert refs <= seen, f"{name} references {refs - seen} before they are built"
        seen.add(name)


def test_facts_read_current_silver_rows_only():
    for name, _, sql in OBJECTS:
        if name.startswith(("dim_", "fact_")):
            assert "is_current" in sql, name
