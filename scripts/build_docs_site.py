#!/usr/bin/env python3
from __future__ import annotations

"""Build and optionally serve a zero-dependency local docs website."""

import argparse
import html
import http.server
import os
import re
import socketserver
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = REPO_ROOT / "build" / "docs-site"


@dataclass(frozen=True)
class DocPage:
    source: Path
    output: Path
    title: str
    kind: str


def _slug(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9\u4e00-\u9fff]+", "-", text.strip()).strip("-")
    return slug or "page"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _source_pages() -> List[DocPage]:
    pages: List[DocPage] = []

    root_markdowns = [REPO_ROOT / "README.md"] + sorted(
        p for p in REPO_ROOT.glob("*.md") if p.name != "README.md"
    )
    docs_markdowns = sorted((REPO_ROOT / "docs").glob("*.md"))
    example_sources = sorted((REPO_ROOT / "examples").glob("*.py"))

    for path in root_markdowns:
        if not path.exists():
            continue
        output = OUTPUT_ROOT / ("index.html" if path.name == "README.md" else f"specs/{path.stem}.html")
        title = path.stem.replace("_", " ").title() if path.name != "README.md" else "README"
        pages.append(DocPage(source=path, output=output, title=title, kind="markdown"))

    for path in docs_markdowns:
        output = OUTPUT_ROOT / f"docs/{path.stem}.html"
        title = path.stem.replace("_", " ").title()
        pages.append(DocPage(source=path, output=output, title=title, kind="markdown"))

    for path in example_sources:
        output = OUTPUT_ROOT / f"examples/{path.stem}.html"
        title = path.stem.replace("_", " ").title()
        pages.append(DocPage(source=path, output=output, title=title, kind="code"))

    return pages


def _page_map(pages: Iterable[DocPage]) -> Dict[Path, Path]:
    return {page.source.resolve(): page.output.resolve() for page in pages}


def _relative_output_href(from_output: Path, to_output: Path) -> str:
    return os.path.relpath(to_output, start=from_output.parent).replace(os.sep, "/")


def _rewrite_link_target(
    *,
    source_path: Path,
    target: str,
    source_to_output: Dict[Path, Path],
) -> str:
    raw = str(target or "").strip()
    if not raw or raw.startswith(("http://", "https://", "mailto:", "#")):
        return raw
    path_part, hash_part = raw, ""
    if "#" in raw:
        path_part, hash_part = raw.split("#", 1)
    if "?" in path_part:
        path_part, query_part = path_part.split("?", 1)
        query_suffix = "?" + query_part
    else:
        query_suffix = ""
    candidate = (source_path.parent / path_part).resolve()
    mapped = source_to_output.get(candidate)
    if mapped is None:
        return raw
    href = _relative_output_href(source_to_output[source_path.resolve()], mapped)
    if query_suffix:
        href += query_suffix
    if hash_part:
        href += "#" + hash_part
    return href


def _inline_markdown(text: str, source_path: Path, source_to_output: Dict[Path, Path]) -> str:
    escaped = html.escape(text, quote=False)

    def _replace_code(match: re.Match[str]) -> str:
        return f"<code>{match.group(1)}</code>"

    def _replace_link(match: re.Match[str]) -> str:
        label = match.group(1)
        target = match.group(2)
        href = _rewrite_link_target(source_path=source_path, target=target, source_to_output=source_to_output)
        return f'<a href="{html.escape(href, quote=True)}">{label}</a>'

    escaped = re.sub(r"`([^`]+)`", _replace_code, escaped)
    escaped = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _replace_link, escaped)
    return escaped


def _render_markdown(content: str, *, source_path: Path, source_to_output: Dict[Path, Path]) -> Tuple[str, List[Tuple[int, str]]]:
    lines = content.splitlines()
    out: List[str] = []
    toc: List[Tuple[int, str]] = []
    para: List[str] = []
    code: List[str] = []
    in_code = False
    code_lang = ""
    list_mode: str | None = None
    quote_lines: List[str] = []

    def flush_para() -> None:
        nonlocal para
        if para:
            out.append(f"<p>{_inline_markdown(' '.join(para).strip(), source_path, source_to_output)}</p>")
            para = []

    def close_lists() -> None:
        nonlocal list_mode
        if list_mode == "ul":
            out.append("</ul>")
        elif list_mode == "ol":
            out.append("</ol>")
        list_mode = None

    def flush_quote() -> None:
        nonlocal quote_lines
        if quote_lines:
            out.append(f"<blockquote><p>{_inline_markdown(' '.join(quote_lines).strip(), source_path, source_to_output)}</p></blockquote>")
            quote_lines = []

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if in_code:
            if stripped.startswith("```"):
                out.append(f"<pre><code class=\"language-{html.escape(code_lang, quote=True)}\">{html.escape('\\n'.join(code))}</code></pre>")
                in_code = False
                code = []
                code_lang = ""
            else:
                code.append(line)
            continue

        if stripped.startswith("```"):
            flush_para()
            flush_quote()
            close_lists()
            in_code = True
            code_lang = stripped[3:].strip()
            code = []
            continue

        if not stripped:
            flush_para()
            flush_quote()
            close_lists()
            continue

        if stripped.startswith(">"):
            flush_para()
            close_lists()
            quote_lines.append(stripped.lstrip("> ").rstrip())
            continue

        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:
            flush_para()
            flush_quote()
            close_lists()
            level = len(heading.group(1))
            text = heading.group(2).strip()
            anchor = _slug(text)
            toc.append((level, text))
            out.append(f'<h{level} id="{anchor}">{_inline_markdown(text, source_path, source_to_output)}</h{level}>')
            continue

        unordered = re.match(r"^[-*+]\s+(.*)$", stripped)
        ordered = re.match(r"^\d+\.\s+(.*)$", stripped)
        if unordered or ordered:
            flush_para()
            flush_quote()
            item = unordered.group(1) if unordered else ordered.group(1)
            desired = "ul" if unordered else "ol"
            if list_mode and list_mode != desired:
                close_lists()
            if list_mode is None:
                out.append(f"<{desired}>")
                list_mode = desired
            out.append(f"<li>{_inline_markdown(item, source_path, source_to_output)}</li>")
            continue

        flush_quote()
        if list_mode is not None:
            close_lists()
        para.append(stripped)

    flush_para()
    flush_quote()
    close_lists()
    if in_code:
        out.append(f"<pre><code class=\"language-{html.escape(code_lang, quote=True)}\">{html.escape('\\n'.join(code))}</code></pre>")

    return "\n".join(out), toc


def _render_code_source(content: str) -> str:
    return f"<pre><code>{html.escape(content)}</code></pre>"


def _nav_html(pages: List[DocPage], current: Path) -> str:
    root = OUTPUT_ROOT.resolve()
    sections: Dict[str, List[Tuple[str, str]]] = {"Home": [], "Docs": [], "Examples": [], "Specs": []}
    current = current.resolve()
    for page in pages:
        href = _relative_output_href(current, page.output.resolve())
        item = (page.title, href)
        if page.output.name == "index.html":
            sections["Home"].append(item)
        elif page.output.parts[-2] == "docs":
            sections["Docs"].append(item)
        elif page.output.parts[-2] == "examples":
            sections["Examples"].append(item)
        else:
            sections["Specs"].append(item)

    parts = ['<nav class="sidebar">', '<div class="brand"><a href="{}">pycloud-parallel docs</a></div>'.format(_relative_output_href(current, root / "index.html"))]
    for title, items in sections.items():
        if not items:
            continue
        parts.append(f"<section><h2>{html.escape(title)}</h2><ul>")
        for label, href in items:
            parts.append(f'<li><a href="{html.escape(href, quote=True)}">{html.escape(label)}</a></li>')
        parts.append("</ul></section>")
    parts.append("</nav>")
    return "\n".join(parts)


def _page_shell(*, title: str, nav: str, body: str, toc: List[Tuple[int, str]], current: Path) -> str:
    toc_html = ""
    if toc:
        toc_html = "<aside class=\"toc\"><h2>On This Page</h2><ul>" + "".join(
            f'<li class="level-{level}"><a href="#{_slug(text)}">{html.escape(text)}</a></li>' for level, text in toc
        ) + "</ul></aside>"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #0b1020;
      --panel: #121935;
      --panel-2: #171f42;
      --text: #e9eefc;
      --muted: #9aa7ce;
      --link: #8eb5ff;
      --border: #24305e;
      --code: #0d1326;
      --accent: #7cf0c6;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #0b1020 0%, #0f1630 100%);
      color: var(--text);
    }}
    a {{ color: var(--link); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .layout {{
      display: grid;
      grid-template-columns: 300px minmax(0, 1fr) 240px;
      min-height: 100vh;
    }}
    .sidebar {{
      background: var(--panel);
      border-right: 1px solid var(--border);
      padding: 24px 18px;
      position: sticky;
      top: 0;
      height: 100vh;
      overflow: auto;
    }}
    .sidebar h2, .toc h2 {{ margin: 18px 0 8px; font-size: 14px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em; }}
    .brand {{ font-weight: 700; font-size: 18px; margin-bottom: 18px; }}
    .sidebar ul, .toc ul {{ list-style: none; padding-left: 0; margin: 0; }}
    .sidebar li, .toc li {{ margin: 6px 0; }}
    .sidebar section + section {{ margin-top: 12px; }}
    .content {{
      max-width: 1000px;
      padding: 36px 40px 80px;
      margin: 0 auto;
    }}
    .page {{
      background: rgba(255,255,255,0.03);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 32px 36px;
      box-shadow: 0 20px 80px rgba(0,0,0,0.25);
    }}
    .page h1, .page h2, .page h3, .page h4, .page h5, .page h6 {{ scroll-margin-top: 24px; }}
    .page h1 {{ font-size: 2.1rem; margin-top: 0; }}
    .page h2 {{ margin-top: 2rem; border-bottom: 1px solid var(--border); padding-bottom: 0.35rem; }}
    .page h3 {{ margin-top: 1.6rem; }}
    .page p, .page li {{ line-height: 1.7; color: #dbe4ff; }}
    .page code {{ background: rgba(124,240,198,0.1); color: #d9fff4; padding: 0.12rem 0.3rem; border-radius: 6px; }}
    .page pre {{
      background: var(--code);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px 20px;
      overflow: auto;
    }}
    .page pre code {{ background: transparent; padding: 0; color: #e6eeff; }}
    .page blockquote {{
      margin: 16px 0;
      padding: 4px 16px;
      border-left: 4px solid var(--accent);
      background: rgba(124,240,198,0.05);
    }}
    .toc {{
      background: var(--panel-2);
      border-left: 1px solid var(--border);
      padding: 24px 18px;
      position: sticky;
      top: 0;
      height: 100vh;
      overflow: auto;
    }}
    .toc li.level-2 {{ margin-left: 0; }}
    .toc li.level-3 {{ margin-left: 12px; }}
    .toc li.level-4 {{ margin-left: 24px; }}
    @media (max-width: 1100px) {{
      .layout {{ grid-template-columns: 260px minmax(0, 1fr); }}
      .toc {{ display: none; }}
    }}
    @media (max-width: 820px) {{
      .layout {{ grid-template-columns: 1fr; }}
      .sidebar {{ position: relative; height: auto; }}
      .content {{ padding: 18px; }}
    }}
  </style>
</head>
<body>
  <div class=\"layout\">
    {nav}
    <main class=\"content\">
      <article class=\"page\">
        {body}
      </article>
    </main>
    {toc_html}
  </div>
</body>
</html>"""


def build_site() -> Path:
    pages = _source_pages()
    source_to_output = _page_map(pages)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    for page in pages:
        raw = _read_text(page.source)
        nav = _nav_html(pages, page.output)
        if page.kind == "markdown":
            body, toc = _render_markdown(raw, source_path=page.source, source_to_output=source_to_output)
        else:
            body = _render_code_source(raw)
            toc = []
        html_text = _page_shell(title=page.title, nav=nav, body=body, toc=toc, current=page.output)
        page.output.parent.mkdir(parents=True, exist_ok=True)
        page.output.write_text(html_text, encoding="utf-8")

    # build a tiny redirect-like index for docs folder root
    docs_index = OUTPUT_ROOT / "docs/index.html"
    if not docs_index.exists():
        docs_index.write_text(
            '<!doctype html><meta http-equiv="refresh" content="0; url=../index.html">',
            encoding="utf-8",
        )

    return OUTPUT_ROOT


def serve(root: Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(root), **kwargs)

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return None

    with socketserver.ThreadingTCPServer((host, port), Handler) as httpd:
        print(f"Docs site available at http://{host}:{port}/")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and serve local docs site")
    parser.add_argument("--serve", action="store_true", help="start a local HTTP server after building")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--port", type=int, default=8765, help="HTTP bind port")
    args = parser.parse_args()

    root = build_site()
    print(f"Built docs site under {root}")
    if args.serve:
        serve(root, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
