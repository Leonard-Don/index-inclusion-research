"""Generate the LaTeX manuscript skeleton (``paper/manuscript.tex``).

``index-inclusion-tex-export`` converts the Markdown paper skeleton
(``paper/skeleton.md``) + the methodology summary card
(``paper/methodology_summary.md``) into an Overleaf-compatible LaTeX
source plus a companion ``references.bib`` BibTeX file.

The Markdown skeleton is what the project natively produces; LaTeX is
what academic journals and thesis defense templates require. This
module closes that gap so the author can either compile the manuscript
locally with ``pdflatex``/``xelatex`` or upload to Overleaf and write
prose directly inside the academic submission format.

Conversion rules (markdown → LaTeX):

- ``## 1. <title>`` → ``\\section{<title>}``
- ``### 1.1 <title>`` → ``\\subsection{<title>}``
- ``#### 4.4.1 <title>`` → ``\\subsubsection{<title>}``
- ``| col | col |`` markdown tables → LaTeX ``tabular`` + booktabs rules
- ``> ...`` blockquotes → ``\\begin{quote}...\\end{quote}``
- ``[TODO: prose]`` → ``\\TODO{prose}`` (red text, removable via flag)
- ``![alt](path)`` figure references →
  ``\\begin{figure}[h]\\includegraphics{path}\\caption{alt}\\end{figure}``
- References section → ``\\bibliographystyle{plain}`` +
  ``\\bibliography{references}`` (companion ``.bib`` lists 16 entries)
- Bold / italic / code spans are mapped to ``\\textbf``/``\\emph``/``\\texttt``.

Chinese text is handled via either ``ctex`` (default, simplest for
Overleaf with the XeLaTeX compiler) or ``xeCJK`` (explicit CJK font
configuration). Both produce valid CJK-typesetting source.

Outputs:

- ``paper/manuscript.tex`` — full LaTeX source
- ``paper/references.bib`` — BibTeX with one ``@article`` entry per
  paper in ``literature_catalog.PAPER_LIBRARY`` (16 entries)
"""

from __future__ import annotations

import argparse
import logging
import re
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from index_inclusion_research import paths

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


def _default_skeleton_md() -> Path:
    return paths.project_root() / "paper" / "skeleton.md"


def _default_methodology_md() -> Path:
    return paths.project_root() / "paper" / "methodology_summary.md"


def _default_manuscript_tex() -> Path:
    return paths.project_root() / "paper" / "manuscript.tex"


def _default_references_bib() -> Path:
    return paths.project_root() / "paper" / "references.bib"


# Sanity bounds on the rendered manuscript (bytes). Reflects the
# expected ``preamble + §1..§7 + appendix + verbatim limitations
# embedding``. Mostly here to catch a wholly empty render.
MANUSCRIPT_MIN_BYTES = 5 * 1024
MANUSCRIPT_MAX_BYTES = 64 * 1024


# ---------------------------------------------------------------------------
# Markdown → LaTeX text escaping
# ---------------------------------------------------------------------------


# LaTeX special characters that must be escaped inside plain text. Order
# matters: ``\`` first so we don't double-escape the substitutions we add
# in subsequent passes.
_LATEX_ESCAPES: tuple[tuple[str, str], ...] = (
    ("\\", r"\textbackslash{}"),
    ("&", r"\&"),
    ("%", r"\%"),
    ("$", r"\$"),
    ("#", r"\#"),
    ("_", r"\_"),
    ("{", r"\{"),
    ("}", r"\}"),
    ("~", r"\textasciitilde{}"),
    ("^", r"\textasciicircum{}"),
)


def _escape_latex(text: str) -> str:
    """Escape LaTeX special characters inside plain text.

    We only escape what would otherwise break compilation; CJK / ASCII
    letters / digits / punctuation are left as-is. This function is
    called on chunks identified by ``_render_inline`` as plain text;
    inline markup (bold / italic / code) is rendered separately.
    """
    for raw, escaped in _LATEX_ESCAPES:
        text = text.replace(raw, escaped)
    return text


# Inline markup is order-sensitive: ``**`` must match before ``*`` and
# inline code must match before either so ``*`` inside ``` `*foo*` ```
# isn't mistaken for italic. We capture spans in a single pass with a
# combined regex and route each match through the per-type renderer.
_INLINE_PATTERN = re.compile(
    r"(`[^`]+`)"  # inline code
    r"|(\*\*[^*]+\*\*)"  # bold
    r"|(\*[^*]+\*)",  # italic
)


def _render_inline(text: str) -> str:
    """Apply inline markdown markup → LaTeX commands.

    Plain-text chunks between matches are escaped via ``_escape_latex``.
    The matched spans (`` `code` ``, ``**bold**``, ``*italic*``) are
    rewritten to ``\\texttt{}``/``\\textbf{}``/``\\emph{}`` respectively
    and their *inner* text is also escaped.
    """
    parts: list[str] = []
    last_end = 0
    for match in _INLINE_PATTERN.finditer(text):
        plain_before = text[last_end : match.start()]
        if plain_before:
            parts.append(_escape_latex(plain_before))
        code, bold, italic = match.group(1), match.group(2), match.group(3)
        if code is not None:
            inner = code[1:-1]
            parts.append(r"\texttt{" + _escape_latex(inner) + "}")
        elif bold is not None:
            inner = bold[2:-2]
            parts.append(r"\textbf{" + _escape_latex(inner) + "}")
        elif italic is not None:
            inner = italic[1:-1]
            parts.append(r"\emph{" + _escape_latex(inner) + "}")
        last_end = match.end()
    if last_end < len(text):
        parts.append(_escape_latex(text[last_end:]))
    return "".join(parts)


# ---------------------------------------------------------------------------
# Section header conversion
# ---------------------------------------------------------------------------


_HEADER_PATTERN = re.compile(
    r"^(#{1,4})\s+(.+?)\s*$"
)


def _convert_header(line: str) -> str | None:
    """Return the LaTeX command for a Markdown header line, else None."""
    match = _HEADER_PATTERN.match(line)
    if not match:
        return None
    hashes = match.group(1)
    title = match.group(2).strip()
    # Skip the document title (``# ...``) — caller handles ``\title``.
    if len(hashes) == 1:
        return None
    rendered_title = _render_inline(title)
    if len(hashes) == 2:
        return r"\section{" + rendered_title + "}"
    if len(hashes) == 3:
        return r"\subsection{" + rendered_title + "}"
    return r"\subsubsection{" + rendered_title + "}"


# ---------------------------------------------------------------------------
# Figure conversion
# ---------------------------------------------------------------------------


_FIGURE_PATTERN = re.compile(
    r"!\[([^\]]+)\]\(([^)]+)\)"
)


def _convert_figure(match: re.Match[str]) -> str:
    """Render ``![alt](path)`` as a LaTeX figure environment.

    Uses ``\\textwidth`` so the figure scales to the column; the alt
    text doubles as the caption. We strip the leading ``../`` from the
    skeleton's relative path because the manuscript lives in
    ``paper/`` next to the renamed figures.
    """
    alt = match.group(1).strip()
    raw_path = match.group(2).strip()
    # Normalize ``../results/figures/foo.png`` → ``../results/figures/foo``;
    # ``\includegraphics`` automatically picks the extension, but leaving
    # ``.png`` is also valid and unambiguous.
    caption = _render_inline(alt)
    # Escape underscores in the file path (TeX file path is literal but
    # we still need to handle ``_`` for the caption ID).
    return (
        "\\begin{figure}[ht]\n"
        "  \\centering\n"
        "  \\includegraphics[width=\\textwidth]{" + raw_path + "}\n"
        "  \\caption{" + caption + "}\n"
        "\\end{figure}"
    )


# ---------------------------------------------------------------------------
# Blockquote conversion
# ---------------------------------------------------------------------------


def _convert_blockquote(lines: list[str]) -> list[str]:
    """Wrap consecutive ``> ...`` lines in a LaTeX ``quote`` environment.

    Operates on a pre-collected list of blockquote bodies (without the
    leading ``> ``). Each body line is inline-rendered, joined with
    blank lines preserved as paragraph breaks.
    """
    body = "\n".join(_render_inline(line) for line in lines)
    return [r"\begin{quote}", body, r"\end{quote}"]


# ---------------------------------------------------------------------------
# TODO marker conversion
# ---------------------------------------------------------------------------


_TODO_PATTERN = re.compile(r"\[TODO:\s*([^\]]+)\]")


def _convert_todo(text: str, include_todos: bool) -> str:
    """Rewrite ``[TODO: prose]`` markers based on the ``--include-todos`` flag.

    When ``include_todos=True`` we emit ``\\TODO{...}`` (rendered red
    via the preamble macro) so the author can grep for ``\\TODO`` in
    Overleaf. When ``False`` we strip the marker entirely so the
    manuscript is review-ready.
    """
    if include_todos:
        return _TODO_PATTERN.sub(
            lambda m: r"\TODO{" + _escape_latex(m.group(1).strip()) + "}",
            text,
        )
    return _TODO_PATTERN.sub("", text)


def _render_inline_with_todos(text: str, *, include_todos: bool) -> str:
    return _convert_todo(_render_inline(text), include_todos=include_todos)


# ---------------------------------------------------------------------------
# Table conversion
# ---------------------------------------------------------------------------


def _is_table_separator(line: str) -> bool:
    """Detect ``| --- | --- |`` style table separators (with optional alignment)."""
    stripped = line.strip()
    if not stripped.startswith("|"):
        return False
    cells = [c.strip() for c in stripped.strip("|").split("|")]
    if not cells:
        return False
    for cell in cells:
        if not cell:
            return False
        # Allow ``---``, ``:---``, ``---:``, ``:---:``
        normalized = cell.replace(":", "")
        if not all(ch == "-" for ch in normalized):
            return False
        if len(normalized) < 3:
            return False
    return True


def _table_alignment(separator_line: str) -> list[str]:
    """Read the alignment row and return per-column ``l``/``c``/``r`` codes."""
    cells = [c.strip() for c in separator_line.strip().strip("|").split("|")]
    codes: list[str] = []
    for cell in cells:
        left = cell.startswith(":")
        right = cell.endswith(":")
        if left and right:
            codes.append("c")
        elif right:
            codes.append("r")
        else:
            codes.append("l")
    return codes


def _split_table_row(line: str) -> list[str]:
    """Split a ``| a | b | c |`` row into stripped cell strings."""
    stripped = line.strip()
    inner = stripped.strip("|")
    return [cell.strip() for cell in inner.split("|")]


def _convert_table(rows: list[str]) -> list[str]:
    """Render a Markdown pipe table as a LaTeX ``tabular`` block with booktabs."""
    if len(rows) < 2:
        return []
    header_cells = _split_table_row(rows[0])
    alignment = _table_alignment(rows[1])
    if len(alignment) < len(header_cells):
        alignment = alignment + ["l"] * (len(header_cells) - len(alignment))
    body_rows = rows[2:]

    rendered: list[str] = []
    rendered.append(r"\begin{table}[ht]")
    rendered.append(r"  \centering")
    rendered.append(r"  \small")
    rendered.append(r"  \begin{tabular}{" + "".join(alignment) + "}")
    rendered.append(r"    \toprule")
    rendered.append(
        "    "
        + " & ".join(_render_inline(c) for c in header_cells)
        + r" \\"
    )
    rendered.append(r"    \midrule")
    for row in body_rows:
        cells = _split_table_row(row)
        # Pad or truncate to header width.
        if len(cells) < len(header_cells):
            cells = cells + [""] * (len(header_cells) - len(cells))
        elif len(cells) > len(header_cells):
            cells = cells[: len(header_cells)]
        rendered.append(
            "    "
            + " & ".join(_render_inline(c) for c in cells)
            + r" \\"
        )
    rendered.append(r"    \bottomrule")
    rendered.append(r"  \end{tabular}")
    rendered.append(r"\end{table}")
    return rendered


# ---------------------------------------------------------------------------
# References / bibliography
# ---------------------------------------------------------------------------


def _bibtex_escape(text: str) -> str:
    """Escape BibTeX-sensitive characters inside a field value.

    BibTeX honors the same TeX escape rules as the manuscript body, plus
    the ``@`` symbol that opens an entry. We escape the same special
    characters as ``_escape_latex`` to keep the bib file compile-clean.
    """
    return _escape_latex(text)


def _coerce_year(year_label: str) -> str:
    """Pull a 4-digit year out of ``year_label`` or fall back to TODO."""
    digits = "".join(ch for ch in str(year_label) if ch.isdigit())
    if len(digits) >= 4:
        return digits[:4]
    return "[TODO: year]"


def build_bibtex_from_catalog() -> str:
    """Return a BibTeX string with one ``@article`` entry per literature paper.

    Falls back to placeholders when the catalog isn't importable (a
    bare test env without ``akshare``/``flask`` would still need the
    other helpers in this module to work). Each entry uses the project
    ``paper_id`` as its citation key so the manuscript's ``\\cite{}``
    calls can reference catalogue rows directly.
    """
    try:
        from index_inclusion_research.literature_catalog import (
            list_literature_papers,
        )
    except ImportError as exc:
        logger.warning("literature_catalog import failed: %s", exc)
        return "% [TODO: literature catalog unavailable]\n"

    entries: list[str] = []
    for paper in list_literature_papers():
        authors = _bibtex_escape(paper.authors.replace(";", " and"))
        year = _coerce_year(paper.year_label)
        title = _bibtex_escape(paper.title)
        market = _bibtex_escape(paper.market_focus)
        method = _bibtex_escape(paper.method_focus)
        camp = _bibtex_escape(paper.camp)
        # We don't fabricate journal names — when the catalog has none
        # we surface a [TODO: journal] placeholder so the author can
        # grep in Overleaf to fill in publication venues before
        # submission.
        entry = (
            "@article{" + paper.paper_id + ",\n"
            "  author    = {" + authors + "},\n"
            "  title     = {" + title + "},\n"
            "  year      = {" + year + "},\n"
            "  journal   = {[TODO: journal]},\n"
            "  note      = {" + market + "; " + method + "; camp=" + camp + "},\n"
            "}"
        )
        entries.append(entry)
    return "\n\n".join(entries) + "\n"


# ---------------------------------------------------------------------------
# Markdown body conversion (main loop)
# ---------------------------------------------------------------------------


def _convert_body(markdown_text: str, *, include_todos: bool) -> str:
    """Convert the body of the markdown skeleton (everything except
    title metadata) into LaTeX.

    Operates line-by-line so we can detect multi-line constructs
    (tables, blockquotes) and consume their bodies together. References
    and figure macros are handled inline.

    Parameters
    ----------
    markdown_text:
        Raw skeleton.md content (excluding the ``# 标题`` line).
    include_todos:
        When False, ``[TODO: ...]`` markers are stripped entirely.
    """
    out: list[str] = []
    lines = markdown_text.splitlines()
    i = 0
    n = len(lines)
    in_references = False
    references_emitted = False

    while i < n:
        line = lines[i]
        stripped = line.strip()

        # Detect the §References section — we replace its enumerated
        # markdown list with a single ``\bibliography`` directive so the
        # author can edit ``references.bib`` instead of the manuscript.
        if stripped == "## 参考文献":
            out.append(r"\section{参考文献}")
            in_references = True
            i += 1
            continue

        if in_references and stripped.startswith("## "):
            # We've moved past the references section. Emit the
            # bibliography directive now (if we haven't already) before
            # rendering the new section header.
            if not references_emitted:
                out.append(r"\bibliographystyle{plain}")
                out.append(r"\bibliography{references}")
                references_emitted = True
            in_references = False
            # Fall through to normal header handling.

        # Table detection: pipe row followed by ``| --- |`` separator.
        if (
            stripped.startswith("|")
            and i + 1 < n
            and _is_table_separator(lines[i + 1])
        ):
            table_rows: list[str] = [line, lines[i + 1]]
            j = i + 2
            while j < n and lines[j].strip().startswith("|"):
                table_rows.append(lines[j])
                j += 1
            if not in_references:
                out.extend(_convert_table(table_rows))
            i = j
            continue

        # Blockquote: collect consecutive ``> ...`` lines.
        if stripped.startswith("> "):
            quote_body: list[str] = []
            while i < n and lines[i].strip().startswith("> "):
                quote_body.append(lines[i].strip()[2:])
                i += 1
            if not in_references:
                out.extend(_convert_blockquote(quote_body))
            continue

        # Figure include.
        if _FIGURE_PATTERN.search(line):
            converted_line = _FIGURE_PATTERN.sub(_convert_figure, line)
            if not in_references:
                out.append(converted_line)
            i += 1
            continue

        # Headers.
        header = _convert_header(line)
        if header is not None:
            out.append(header)
            i += 1
            continue

        # Inside §References: skip enumerated list markdown — the BibTeX
        # file owns that information now.
        if in_references:
            i += 1
            continue

        # Plain text (paragraph or list item). Apply inline markdown
        # then TODO substitution.
        if stripped:
            rendered = _render_inline_with_todos(
                line, include_todos=include_todos
            )
            # Markdown list bullets ``- foo`` are rendered as itemized
            # paragraphs only at the simplest possible level; the inline
            # markdown render leaves the leading ``- `` in place which
            # LaTeX would otherwise treat as a literal dash. Translate
            # leading list bullets into ``\item`` directives wrapped in
            # an ``itemize`` block (single-line itemize is fine).
            if stripped.startswith("- "):
                # Collect consecutive list lines.
                bullets: list[str] = []
                while i < n and lines[i].strip().startswith("- "):
                    text = lines[i].strip()[2:]
                    text = _render_inline_with_todos(
                        text, include_todos=include_todos
                    )
                    bullets.append(text)
                    i += 1
                out.append(r"\begin{itemize}")
                for bullet in bullets:
                    out.append("  \\item " + bullet)
                out.append(r"\end{itemize}")
                continue
            # Horizontal rule
            if stripped == "---":
                out.append(r"\hrulefill")
                i += 1
                continue
            out.append(rendered)
        else:
            out.append("")
        i += 1

    # If the document ended while still inside the references section,
    # flush the bibliography directive now.
    if in_references and not references_emitted:
        out.append(r"\bibliographystyle{plain}")
        out.append(r"\bibliography{references}")

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Title block + preamble assembly
# ---------------------------------------------------------------------------


def _extract_title_and_metadata(markdown_text: str) -> tuple[str, str, str]:
    """Pull the document title + author + date out of the skeleton header.

    Returns ``(title, author, date)``. Skeleton lays them out as:

        # 指数纳入效应跨市场不对称研究：基于美中市场的实证分析

        **作者**: [TODO: 作者]
        **日期**: 2026-05-17
    """
    title_match = re.search(r"^#\s+(.+?)\s*$", markdown_text, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else "[TODO: title]"
    author_match = re.search(r"\*\*作者\*\*:\s*(.+?)\s*$", markdown_text, re.MULTILINE)
    author = author_match.group(1).strip() if author_match else "[TODO: 作者]"
    date_match = re.search(r"\*\*日期\*\*:\s*(.+?)\s*$", markdown_text, re.MULTILINE)
    date = date_match.group(1).strip() if date_match else datetime.now(tz=UTC).strftime("%Y-%m-%d")
    return title, author, date


def _build_preamble(cjk_engine: str, *, include_todos: bool) -> str:
    """Return the LaTeX preamble used by ``manuscript.tex``.

    Two CJK modes are supported. ``ctex`` is the default because it
    works out-of-the-box on Overleaf with the XeLaTeX or LuaLaTeX
    compiler — most journals will accept the result without further
    configuration. ``xeCJK`` is the explicit alternative for users who
    need to control the CJK fonts manually.
    """
    if cjk_engine == "xeCJK":
        cjk_block = (
            r"\usepackage{xeCJK}" + "\n"
            r"\setCJKmainfont{Noto Serif CJK SC}" + "\n"
            r"\setCJKsansfont{Noto Sans CJK SC}" + "\n"
            r"\setCJKmonofont{Noto Sans Mono CJK SC}" + "\n"
        )
    else:
        cjk_block = r"\usepackage[UTF8]{ctex}" + "\n"

    todo_macro = (
        "% Inline TODO marker — red text the author can grep for.\n"
        + r"\newcommand{\TODO}[1]{\textcolor{red}{[TODO: #1]}}" + "\n"
        if include_todos
        else "% Inline TODO markers disabled by --include-todos false.\n"
        + r"\newcommand{\TODO}[1]{}" + "\n"
    )

    preamble = (
        r"\documentclass[11pt,a4paper]{article}" + "\n"
        + cjk_block
        + r"\usepackage[margin=1in]{geometry}" + "\n"
        + r"\usepackage{setspace}" + "\n"
        + r"\usepackage{booktabs}" + "\n"
        + r"\usepackage{graphicx}" + "\n"
        + r"\usepackage{hyperref}" + "\n"
        + r"\usepackage{cite}" + "\n"
        + r"\usepackage{xcolor}" + "\n"
        + r"\usepackage{enumitem}" + "\n"
        + r"\hypersetup{colorlinks=true,linkcolor=blue,citecolor=blue,urlcolor=blue}" + "\n"
        + todo_macro
        + r"\onehalfspacing" + "\n"
    )
    return preamble


def _build_title_block(
    title: str, author: str, date: str, *, include_todos: bool
) -> str:
    """Return the LaTeX title block (``\\title``/``\\author``/``\\date``/``\\maketitle``)."""
    return (
        r"\title{"
        + _render_inline_with_todos(title, include_todos=include_todos)
        + "}\n"
        + r"\author{"
        + _render_inline_with_todos(author, include_todos=include_todos)
        + "}\n"
        + r"\date{"
        + _render_inline_with_todos(date, include_todos=include_todos)
        + "}\n"
        + r"\maketitle"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_tex_manuscript(
    skeleton_md: str,
    methodology_md: str,
    root: Path,
    *,
    include_todos: bool = True,
    cjk_engine: str = "ctex",
    generated_at: datetime | None = None,
) -> str:
    """Render the LaTeX manuscript from the skeleton + methodology summary.

    Parameters
    ----------
    skeleton_md:
        Raw ``paper/skeleton.md`` content. The body of the manuscript
        comes from this Markdown skeleton.
    methodology_md:
        Raw ``paper/methodology_summary.md`` content. Currently appended
        as a one-page appendix block so the answer to "what did you
        actually do?" lives next to the manuscript without round-trips
        to a sibling file.
    root:
        Project root. Reserved for future use (e.g. resolving relative
        figure paths against a non-default ``paths.project_root()``);
        accepted as a parameter to match the task contract.
    include_todos:
        Whether to emit ``\\TODO{...}`` markers for prose-pending
        sections. False strips them entirely.
    cjk_engine:
        ``ctex`` (default, Overleaf-friendly) or ``xeCJK`` (explicit
        font control).
    generated_at:
        Optional timestamp for the auto-generated metadata comment.
        Tests pass a fixed value so the rendered manuscript is byte-stable.

    Returns
    -------
    str
        Full LaTeX source ready to write to ``paper/manuscript.tex``.
    """
    del root  # Reserved parameter; figure paths are relative inside skeleton.

    title, author, date = _extract_title_and_metadata(skeleton_md)
    # Strip the H1 title line — we re-emit it via ``\title``.
    body_md = re.sub(r"^#\s+.+?\n", "", skeleton_md, count=1)
    # Drop the ``**作者**``/``**日期**``/``**摘要 (TODO)**`` metadata block —
    # the title page covers author/date, and the abstract becomes its
    # own LaTeX ``abstract`` environment below.
    abstract_match = re.search(
        r"\*\*摘要[^*]*\*\*:?\s*(.+?)(?=\n##\s|\Z)",
        body_md,
        re.DOTALL,
    )
    abstract_text = ""
    if abstract_match:
        abstract_text = abstract_match.group(1).strip()
        body_md = body_md.replace(abstract_match.group(0), "", 1)
    # Drop the leading metadata lines (作者/日期/摘要).
    body_md = re.sub(r"^\*\*作者\*\*:[^\n]*\n", "", body_md)
    body_md = re.sub(r"^\*\*日期\*\*:[^\n]*\n", "", body_md)

    body_tex = _convert_body(body_md, include_todos=include_todos)

    # Methodology summary appendix — small section appended at the end
    # so reviewers can see the auto-derived methods alongside the
    # prose. We render only the body (skip the doc-level H1).
    methodology_appendix = ""
    if methodology_md.strip():
        method_body = re.sub(r"^#\s+.+?\n", "", methodology_md, count=1)
        method_body_tex = _convert_body(method_body, include_todos=include_todos)
        methodology_appendix = (
            "\n\n"
            r"\appendix" + "\n"
            r"\section{方法论摘要（自动派生）}" + "\n"
            + method_body_tex
        )

    abstract_block = ""
    if abstract_text:
        abstract_rendered = _render_inline_with_todos(
            abstract_text,
            include_todos=include_todos,
        )
        abstract_block = (
            r"\begin{abstract}" + "\n"
            + abstract_rendered + "\n"
            + r"\end{abstract}"
        )

    generated_stamp = (generated_at or datetime.now(tz=UTC)).astimezone(UTC).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    parts = [
        "% Auto-generated by index-inclusion-tex-export.",
        "% Companion bibliography: references.bib (16 entries).",
        f"% Generated: {generated_stamp}.",
        "",
        _build_preamble(cjk_engine, include_todos=include_todos),
        "",
        r"\begin{document}",
        "",
        _build_title_block(title, author, date, include_todos=include_todos),
        "",
        abstract_block,
        "",
        body_tex,
        methodology_appendix,
        "",
        r"\end{document}",
        "",
    ]
    return "\n".join(parts)


def write_manuscript(
    *,
    skeleton_md: Path | None = None,
    methodology_md: Path | None = None,
    manuscript_tex: Path | None = None,
    references_bib: Path | None = None,
    include_todos: bool = True,
    cjk_engine: str = "ctex",
    generated_at: datetime | None = None,
    force: bool = False,
) -> tuple[Path, Path]:
    """Render both ``manuscript.tex`` and ``references.bib`` to disk.

    Returns the resolved output paths so CLI logging and tests can
    inspect them. Refuses to overwrite existing files unless
    ``force=True`` so a user who hand-edited the manuscript won't lose
    work by re-running the CLI.
    """
    skeleton_md = skeleton_md or _default_skeleton_md()
    methodology_md = methodology_md or _default_methodology_md()
    manuscript_tex = manuscript_tex or _default_manuscript_tex()
    references_bib = references_bib or _default_references_bib()

    if manuscript_tex.exists() and not force:
        raise FileExistsError(
            f"{manuscript_tex} already exists; pass --force to overwrite."
        )
    if references_bib.exists() and not force:
        raise FileExistsError(
            f"{references_bib} already exists; pass --force to overwrite."
        )

    skeleton_text = skeleton_md.read_text(encoding="utf-8") if skeleton_md.exists() else ""
    methodology_text = (
        methodology_md.read_text(encoding="utf-8")
        if methodology_md.exists()
        else ""
    )

    manuscript = build_tex_manuscript(
        skeleton_text,
        methodology_text,
        paths.project_root(),
        include_todos=include_todos,
        cjk_engine=cjk_engine,
        generated_at=generated_at,
    )
    bib = build_bibtex_from_catalog()

    manuscript_tex.parent.mkdir(parents=True, exist_ok=True)
    references_bib.parent.mkdir(parents=True, exist_ok=True)
    manuscript_tex.write_text(manuscript, encoding="utf-8")
    references_bib.write_text(bib, encoding="utf-8")
    return manuscript_tex, references_bib


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _str2bool(value: str) -> bool:
    if value.lower() in {"true", "t", "yes", "y", "1"}:
        return True
    if value.lower() in {"false", "f", "no", "n", "0"}:
        return False
    raise argparse.ArgumentTypeError(f"Invalid boolean: {value!r}")


def _parse_generated_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid ISO timestamp for --generated-at: {value!r}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "--generated-at must include a timezone offset, e.g. "
            "2026-05-17T12:34:56Z."
        )
    return parsed


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index-inclusion-tex-export",
        description=(
            "Convert paper/skeleton.md + paper/methodology_summary.md into a "
            "LaTeX manuscript (paper/manuscript.tex) and a companion "
            "BibTeX file (paper/references.bib). The output is Overleaf-"
            "compatible and includes a ctex (default) or xeCJK preamble "
            "for Chinese text."
        ),
    )
    parser.add_argument(
        "--skeleton-md",
        type=Path,
        default=None,
        help="Source Markdown skeleton (default: paper/skeleton.md).",
    )
    parser.add_argument(
        "--methodology-md",
        type=Path,
        default=None,
        help="Methodology summary card (default: paper/methodology_summary.md).",
    )
    parser.add_argument(
        "--manuscript-out",
        type=Path,
        default=None,
        help="Destination LaTeX file (default: paper/manuscript.tex).",
    )
    parser.add_argument(
        "--references-out",
        type=Path,
        default=None,
        help="Destination BibTeX file (default: paper/references.bib).",
    )
    parser.add_argument(
        "--include-todos",
        type=_str2bool,
        default=True,
        help=(
            "Whether to emit \\TODO{...} markers (default true). "
            "Set to false for a review-ready manuscript with markers stripped."
        ),
    )
    parser.add_argument(
        "--cjk-engine",
        choices=["ctex", "xeCJK"],
        default="ctex",
        help=(
            "CJK preamble engine (default ctex; more portable for Overleaf). "
            "Use xeCJK if you need explicit CJK font configuration."
        ),
    )
    parser.add_argument(
        "--generated-at",
        type=_parse_generated_at,
        default=None,
        help=(
            "ISO timestamp for the manuscript Generated comment. "
            "Must include a timezone offset, e.g. 2026-05-17T12:34:56Z."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing manuscript.tex / references.bib if present.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    args = _build_arg_parser().parse_args(argv)

    try:
        manuscript_path, references_path = write_manuscript(
            skeleton_md=args.skeleton_md,
            methodology_md=args.methodology_md,
            manuscript_tex=args.manuscript_out,
            references_bib=args.references_out,
            include_todos=args.include_todos,
            cjk_engine=args.cjk_engine,
            generated_at=args.generated_at,
            force=args.force,
        )
    except FileExistsError as exc:
        logger.error(str(exc))
        return 1

    manuscript_size = manuscript_path.stat().st_size
    references_size = references_path.stat().st_size
    logger.info(
        "Wrote manuscript to %s (%d bytes)", manuscript_path, manuscript_size
    )
    logger.info(
        "Wrote bibliography to %s (%d bytes)", references_path, references_size
    )
    if not (MANUSCRIPT_MIN_BYTES <= manuscript_size <= MANUSCRIPT_MAX_BYTES):
        logger.warning(
            "Manuscript size %d bytes is outside the sanity band "
            "[%d, %d] — inspect inputs / template.",
            manuscript_size,
            MANUSCRIPT_MIN_BYTES,
            MANUSCRIPT_MAX_BYTES,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
