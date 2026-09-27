#!/usr/bin/env python3
"""文档体检：守住两类「看起来对、渲染出来是坏的」问题。

1. **围栏配平**：每个 markdown 文件的 ``` 数量必须是偶数（否则后面全被吞进代码块）；
2. **装饰器写法**：行首 `@app.task(` 这种（把 `` 写成反引号）会让示例直接不能跑。

用法：`python scripts/check_docs.py [路径…]`，默认检查 docs/ 与 skills/ 下的全部 .md 和 README.md。
CI 的 docs 工作流会在 mkdocs 之前跑它。
"""
from __future__ import annotations

import pathlib
import re
import sys

BT = chr(96)
FENCE = BT * 3
DECORATORS = ("app.", "dataclasses.", "wf.")


def md_files(roots: list[str]) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for root in roots:
        path = pathlib.Path(root)
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(sorted(path.rglob("*.md")))
    return files


def check(path: pathlib.Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    problems: list[str] = []
    fences = len(re.findall(rf"(?m)^{FENCE}", text))
    if fences % 2:
        problems.append(f"代码围栏数 {fences}（应为偶数，否则后面整段被吞）")
    for index, line in enumerate(text.splitlines(), 1):
        stripped = line.lstrip()
        for name in DECORATORS:
            if stripped.startswith(BT + name):
                problems.append(f"{index}: 行首反引号装饰器（应写 @）：{line.strip()[:60]}")
                break
        run = len(stripped) - len(stripped.lstrip(BT))
        if run > 3 and stripped.startswith(BT * run):
            problems.append(f"{index}: 围栏用了 {run} 个反引号（统一 3 个）：{line.strip()[:60]}")
    return problems


def main(argv: list[str]) -> int:
    roots = argv[1:] or ["docs", "skills", "README.md"]
    failed = False
    for path in md_files(roots):
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
