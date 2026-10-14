import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks", "lib"))

import gold  # noqa: E402

OBJECTS = gold.gold_objects("S.", "G.", "O.")


def test_names_unique_and_kinds_valid():
    names = [o[0] for o in OBJECTS]
    assert len(names) == len(set(names))
    assert {o[1] for o in OBJECTS} <= {"table", "view"}


def test_objects_only_reference_earlier_gold_objects():
    seen = set()
    for name, _, _, sql in OBJECTS:
        refs = set(re.findall(r"\bG\.(\w+)", sql))
        assert refs <= seen, f"{name} references {refs - seen} before they are built"
        seen.add(name)


def test_cluster_columns_are_selected():
    for name, kind, cluster_by, sql in OBJECTS:
        for col in cluster_by or []:
            assert f"AS {col}" in sql, f"{name}: cluster column {col} not produced"
