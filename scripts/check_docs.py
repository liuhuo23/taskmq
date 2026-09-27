#!/usr/bin/env python3
"""文档与代码体检：守住两类「看起来对、跑起来是坏的」问题。

1. **围栏配平**：每个 markdown 文件的 ``` 数量必须是偶数（否则后面全被吞进代码块）；
2. **装饰器写法**：行首写成反引号 + `app.task(`（应为 `@app.task(`）——markdown 里示例跑不了，
   Python 里直接是语法错误。这类错在 .md/.py/.toml 里都查。

用法：`python scripts/check_docs.py [路径…]`，默认检查 docs/、skills/、scripts/、tests/、taskmq/
与根目录的 README.md。CI 的 docs 工作流在 mkdocs 之前跑它，`make check` 也会跑。
"""
from __future__ import annotations

import pathlib
import re
import sys

BT = chr(96)
FENCE = BT * 3
DECORATORS = ("app.", "dataclasses.", "wf.")


SUFFIXES = (".md", ".py", ".toml")


def source_files(roots: list[str]) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for root in roots:
        path = pathlib.Path(root)
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            for suffix in SUFFIXES:
                files.extend(sorted(path.rglob(f"*{suffix}")))
    return files


def check(path: pathlib.Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    problems: list[str] = []
    if path.suffix == ".md":
        fences = len(re.findall(rf"(?m)^{FENCE}", text))
        if fences % 2:
            problems.append(f"代码围栏数 {fences}（应为偶数，否则后面整段被吞）")
    for index, line in enumerate(text.splitlines(), 1):
        stripped = line.lstrip()
        for name in DECORATORS:
            if stripped.startswith(BT + name):
                problems.append(f"{index}: 行首反引号装饰器（应写 @）：{line.strip()[:60]}")
                break
        if path.suffix != ".md":
            continue
        run = len(stripped) - len(stripped.lstrip(BT))
        if run > 3 and stripped.startswith(BT * run):
            problems.append(f"{index}: 围栏用了 {run} 个反引号（统一 3 个）：{line.strip()[:60]}")
    return problems


def main(argv: list[str]) -> int:
    roots = argv[1:] or ["docs", "skills", "scripts", "tests", "taskmq", "README.md"]
    failed = False
    for path in source_files(roots):
        problems = check(path)
        if problems:
            failed = True
            print(f"{path}:")
            for problem in problems:
                print(f"  - {problem}")
    if failed:
        print("\n文档体检不通过（见上）。")
        return 1
    print("文档体检通过。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
