#!/usr/bin/env python3
"""Enforces the project's test conventions (see CLAUDE.md):

* Go tests are named  Test_should<Expected>When<Condition>
* every Go test body contains GIVEN, WHEN and THEN comments
  (combined forms like "// WHEN / THEN:" are allowed)

Usage: lint-tests.py <dir> [<dir> ...]   exit code 1 on violations
"""
import pathlib
import re
import sys

NAME = re.compile(r"^func (Test\w*)\(t \*testing\.T\)")
GOOD = re.compile(r"^Test_should[A-Z]\w*When[A-Z]\w*$")
FUNC = re.compile(r"^func ")
SKIP_DIRS = {"node_modules", ".tools", ".claude", "vendor"}


def check_file(path: pathlib.Path) -> list[str]:
    problems = []
    lines = path.read_text().splitlines()
    i = 0
    while i < len(lines):
        m = NAME.match(lines[i])
        if not m:
            i += 1
            continue
        name, start = m.group(1), i
        i += 1
        while i < len(lines) and not FUNC.match(lines[i]):
            i += 1
        body = "\n".join(lines[start:i])
        where = f"{path}:{start + 1}"
        if name == "TestMain":
            continue
        if not GOOD.match(name):
            problems.append(f"{where}: {name} does not match Test_should<Expected>When<Condition>")
        comments = " ".join(re.findall(r"//\s*([^\n]*)", body))
        for marker in ("GIVEN", "WHEN", "THEN"):
            if not re.search(rf"\b{marker}\b", comments):
                problems.append(f"{where}: {name} has no '// {marker}:' comment")
    return problems


def main(dirs: list[str]) -> int:
    problems = []
    count = 0
    for d in dirs:
        for path in sorted(pathlib.Path(d).rglob("*_test.go")):
            if SKIP_DIRS.intersection(path.relative_to(d).parts):
                continue
            count += 1
            problems += check_file(path)
    for p in problems:
        print(p)
    print(f"lint-tests: {count} test files, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["."]))
