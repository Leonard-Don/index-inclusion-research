from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from index_inclusion_research import paths
from index_inclusion_research.analysis.rdd_candidates import (
    build_candidate_batch_audit,
    summarize_candidate_audit,
    validate_candidate_frame,
)
from index_inclusion_research.loaders import save_dataframe

ROOT = paths.project_root()

CSINDEX_HOME = "https://www.csindex.com.cn/csindex-home"
CSINDEX_SITE = "https://www.csindex.com.cn"
DEFAULT_OUTPUT_DIR = ROOT / "results" / "literature" / "hs300_rdd_l3_collection"
DEFAULT_DRAFT_OUTPUT = DEFAULT_OUTPUT_DIR / "official_candidate_draft.csv"
DEFAULT_AUDIT_OUTPUT = DEFAULT_OUTPUT_DIR / "online_source_audit.csv"
DEFAULT_REPORT_OUTPUT = DEFAULT_OUTPUT_DIR / "online_collection_report.md"
DEFAULT_ATTACHMENT_DIR = DEFAULT_OUTPUT_DIR / "official_attachments"
DEFAULT_FORMAL_OUTPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
DEFAULT_NOTICE_ROWS = 80
DEFAULT_TIMEOUT = 30
CSI300_CUTOFF = 300.0

SEARCH_TERMS = (
    "沪深300、中证500、中证1000",
    "沪深300等指数样本",
    "关于沪深300、中证500、中证1000等指数定期调整结果的公告",
    "关于沪深300、中证500、中证1000、中证A500等指数定期调整结果的公告",
)


@dataclass(frozen=True)
class AttachmentLink:
    notice_id: int
    notice_title: str
    publish_date: str
    effective_date: str
    file_name: str
    file_url: str
    detail_url: str


@dataclass(frozen=True)
class ParsedHs300Attachment:
    deletions: list[dict[str, object]]
    additions: list[dict[str, object]]
    reserves: list[dict[str, object]]

    @property
    def usable_for_l3(self) -> bool:
        return bool(self.additions and self.reserves)


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _request_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
        ),
        "Referer": "https://www.csindex.com.cn/zh-CN/about/newsCenter",
    }


def _clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return _clean_text(soup.get_text(" "))


def _extract_effective_date(content_html: str) -> str:
    text = _html_to_text(content_html)
    compact = re.sub(r"\s+", "", text)
    match = re.search(r"于(\d{4})年(\d{1,2})月(\d{1,2})日[^。；;]*生效", compact)
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def _announcement_payload(search_input: str, *, rows: int) -> dict[str, object]:
    return {
        "lang": "cn",
        "searchInput": search_input,
        "page": {"key": "", "order": None, "page": 1, "rows": rows, "sortBy": ""},
        "classList": [],
        "indexList": [],
        "relatedTopics": [],
        "typeList": [],
    }


def query_rebalance_announcements(
    session: requests.Session,
    *,
    search_terms: tuple[str, ...] = SEARCH_TERMS,
    rows: int = DEFAULT_NOTICE_ROWS,
) -> list[dict[str, object]]:
    notices: dict[int, dict[str, object]] = {}
    for term in search_terms:
        response = session.post(
            f"{CSINDEX_HOME}/announcement/queryAnnouncementByVo",
            json=_announcement_payload(term, rows=rows),
            headers=_request_headers(),
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "200":
            continue
        for row in payload.get("data") or []:
            title = _clean_text(row.get("title"))
            theme = _clean_text(row.get("theme"))
            if "沪深300" not in title or "定期调整结果" not in title:
                continue
            if theme and theme != "指数调样":
                continue
            notice_id = int(row["id"])
            notices[notice_id] = {
                "id": notice_id,
                "title": title,
                "theme": theme,
                "publish_date": _clean_text(row.get("publishDate")),
                "search_term": term,
                "detail_url": f"{CSINDEX_SITE}/zh-CN/about/newsDetail?id={notice_id}",
            }
    return sorted(notices.values(), key=lambda item: (str(item["publish_date"]), int(item["id"])))


def fetch_notice_detail(session: requests.Session, notice_id: int) -> dict[str, object]:
    response = session.get(
        f"{CSINDEX_HOME}/announcement/queryAnnouncementById",
        params={"id": notice_id},
        headers=_request_headers(),
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if str(payload.get("code")) != "200" or not payload.get("data"):
        raise ValueError(f"CSIndex notice detail unavailable for id={notice_id}")
    return payload["data"]


def _attachment_links_from_detail(detail: dict[str, object]) -> list[AttachmentLink]:
    notice_id = int(detail["id"])
    title = _clean_text(detail.get("title"))
    publish_date = _clean_text(detail.get("publishDate"))
    content = str(detail.get("content") or "")
    effective_date = _extract_effective_date(content)
    detail_url = f"{CSINDEX_SITE}/zh-CN/about/newsDetail?id={notice_id}"
    links: list[AttachmentLink] = []
    seen: set[str] = set()

    for item in detail.get("enclosureList") or []:
        file_url = _clean_text(item.get("fileUrl"))
        if not file_url or file_url in seen:
            continue
        seen.add(file_url)
        links.append(
            AttachmentLink(
                notice_id=notice_id,
                notice_title=title,
                publish_date=publish_date,
                effective_date=effective_date,
                file_name=_clean_text(item.get("fileName")) or Path(file_url).name,
                file_url=file_url,
                detail_url=detail_url,
            )
        )

    for href in re.findall(r'href="([^"]+)"', content):
        if href in seen:
            continue
        seen.add(href)
        links.append(
            AttachmentLink(
                notice_id=notice_id,
                notice_title=title,
                publish_date=publish_date,
                effective_date=effective_date,
                file_name=Path(href).name,
                file_url=href,
                detail_url=detail_url,
            )
        )
    return links


def _download_attachment(session: requests.Session, link: AttachmentLink, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(link.file_url).suffix.lower() or ".bin"
    filename = f"{link.publish_date}_{link.notice_id}_{quote(link.file_name, safe='')[:80]}{suffix}"
    output_path = output_dir / filename
    response = session.get(link.file_url, headers=_request_headers(), timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    output_path.write_bytes(response.content)
    return output_path


def _pdf_to_text(pdf_path: Path) -> str:
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _section(text: str, start_pattern: str, end_pattern: str) -> str:
    start = re.search(start_pattern, text)
    if not start:
        return ""
    section_start = start.end()
    end = re.search(end_pattern, text[section_start:])
    if not end:
        return text[section_start:]
    return text[section_start : section_start + end.start()]


def _parse_adjustment_pairs(section_text: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    deletions: list[dict[str, object]] = []
    additions: list[dict[str, object]] = []
    for line in section_text.splitlines():
        if "证券代码" in line or "调出" in line or "调入" in line:
            continue
        tokens = line.split()
        code_positions = [idx for idx, token in enumerate(tokens) if re.fullmatch(r"\d{6}", token)]
        if len(code_positions) < 2:
            continue
        delete_idx, add_idx = code_positions[:2]
        delete_name = _clean_text(" ".join(tokens[delete_idx + 1 : add_idx]))
        add_name = _clean_text(" ".join(tokens[add_idx + 1 :]))
        if not delete_name or not add_name or add_name == "-":
            continue
        order = len(additions) + 1
        deletions.append(
            {
                "order": order,
                "ticker": tokens[delete_idx],
                "security_name": delete_name,
                "role": "deletion",
            }
        )
        additions.append(
            {
                "order": order,
                "ticker": tokens[add_idx],
                "security_name": add_name,
                "role": "addition",
            }
        )
    return deletions, additions


def _parse_ranked_rows(section_text: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in section_text.splitlines():
        if "排序" in line or "证券代码" in line:
            continue
        tokens = line.split()
        idx = 0
        while idx < len(tokens) - 1:
            if tokens[idx].isdigit() and re.fullmatch(r"\d{6}", tokens[idx + 1]):
                name_end = idx + 2
                while name_end < len(tokens):
                    next_pair = (
                        name_end < len(tokens) - 1
                        and tokens[name_end].isdigit()
                        and re.fullmatch(r"\d{6}", tokens[name_end + 1])
                    )
                    if next_pair:
                        break
                    name_end += 1
                security_name = _clean_text(" ".join(tokens[idx + 2 : name_end]))
                if security_name and security_name != "-":
                    rows.append(
                        {
                            "rank": int(tokens[idx]),
                            "ticker": tokens[idx + 1],
                            "security_name": security_name,
                            "role": "reserve",
                        }
                    )
                idx = name_end
            else:
                idx += 1
    return sorted(rows, key=lambda item: int(item["rank"]))


def parse_hs300_attachment_text(text: str) -> ParsedHs300Attachment:
    adjustment_section = _section(
        text,
        r"沪深\s*300\s*指数样本调整名单[:：]",
        r"中证\s*500\s*指数样本调整名单[:：]",
    )
    reserve_section = _section(
        text,
        r"沪深\s*300\s*指数备选名单[:：]",
        r"中证\s*500\s*指数备选名单[:：]",
    )
    deletions, additions = _parse_adjustment_pairs(adjustment_section)
    reserves = _parse_ranked_rows(reserve_section)
    return ParsedHs300Attachment(deletions=deletions, additions=additions, reserves=reserves)


def _batch_id(publish_date: str) -> str:
    timestamp = pd.Timestamp(publish_date)
    return f"csi300-{timestamp.year:04d}-{timestamp.month:02d}"


def build_candidate_rows(link: AttachmentLink, parsed: ParsedHs300Attachment) -> list[dict[str, object]]:
    if not link.effective_date:
        raise ValueError(f"Missing effective date for CSIndex notice {link.notice_id}")
    batch_id = _batch_id(link.publish_date)
    rows: list[dict[str, object]] = []
    additions = sorted(parsed.additions, key=lambda item: int(item["order"]))
    n_additions = len(additions)
    for item in additions:
        order = int(item["order"])
        rows.append(
            {
                "batch_id": batch_id,
                "market": "CN",
                "index_name": "CSI300",
                "ticker": str(item["ticker"]).zfill(6),
                "security_name": item["security_name"],
                "announce_date": link.publish_date,
                "effective_date": link.effective_date,
                "running_variable": CSI300_CUTOFF + (n_additions - order + 1) / 100,
                "cutoff": CSI300_CUTOFF,
                "inclusion": 1,
                "event_type": "official_adjustment_addition",
                "source": "CSIndex official adjustment and reserve attachment",
                "source_url": link.file_url,
                "note": "Official adjustment-list order mapped to a boundary ordinal running variable.",
                "sector": "",
            }
        )
    for item in sorted(parsed.reserves, key=lambda row: int(row["rank"])):
        rank = int(item["rank"])
        rows.append(
            {
                "batch_id": batch_id,
                "market": "CN",
                "index_name": "CSI300",
                "ticker": str(item["ticker"]).zfill(6),
                "security_name": item["security_name"],
                "announce_date": link.publish_date,
                "effective_date": link.effective_date,
                "running_variable": CSI300_CUTOFF - rank / 100,
                "cutoff": CSI300_CUTOFF,
                "inclusion": 0,
                "event_type": "official_reserve_control",
                "source": "CSIndex official adjustment and reserve attachment",
                "source_url": link.file_url,
                "note": f"Official reserve-list rank {rank} mapped below the CSI300 cutoff.",
                "sector": "",
            }
        )
    return rows


def _source_audit_row(
    *,
    link: AttachmentLink,
    status: str,
    local_path: Path | None,
    parsed: ParsedHs300Attachment | None,
    reason: str,
) -> dict[str, object]:
    addition_rows = len(parsed.additions) if parsed else 0
    control_rows = len(parsed.reserves) if parsed else 0
    usable = bool(parsed and parsed.usable_for_l3)
    return {
        "source_kind": "official_adjustment_backup_attachment",
        "announcement_id": link.notice_id,
        "publish_date": link.publish_date,
        "effective_date": link.effective_date,
        "title": link.notice_title,
        "detail_url": link.detail_url,
        "attachment_name": link.file_name,
        "attachment_url": link.file_url,
        "local_path": _display_path(local_path) if local_path else "",
        "status": status,
        "usable_for_l3": usable,
        "addition_rows": addition_rows,
        "control_rows": control_rows,
        "candidate_rows": addition_rows + control_rows,
        "reason": reason,
    }


def _notice_audit_row(notice: dict[str, object], *, has_detail: bool) -> dict[str, object]:
    return {
        "source_kind": "official_rebalance_result_notice",
        "announcement_id": notice["id"],
        "publish_date": notice["publish_date"],
        "effective_date": "",
        "title": notice["title"],
        "detail_url": notice["detail_url"],
        "attachment_name": "",
        "attachment_url": "",
        "local_path": "",
        "status": "detail_fetched" if has_detail else "found",
        "usable_for_l3": False,
        "addition_rows": 0,
        "control_rows": 0,
        "candidate_rows": 0,
        "reason": "Notice metadata is provenance; the attachment must contain adjustment and reserve lists.",
    }


def _build_report(
    *,
    draft_output: Path,
    formal_output: Path | None,
    audit_output: Path,
    candidate_frame: pd.DataFrame,
    audit_frame: pd.DataFrame,
    candidate_audit: pd.DataFrame,
) -> str:
    summary = summarize_candidate_audit(candidate_audit)
    usable_sources = int(audit_frame["usable_for_l3"].fillna(False).sum()) if not audit_frame.empty else 0
    formal_line = (
        f"- 正式文件：`{_display_path(formal_output)}`"
        if formal_output is not None
        else "- 正式文件：未写入；先保留草稿等待人工确认"
    )
    lines = [
        "# HS300 RDD 官方线上来源采集报告",
        "",
        "- 来源域名：`csindex.com.cn` 与 `oss-ch.csindex.com.cn`",
        f"- 来源审计：`{_display_path(audit_output)}`",
        f"- 候选草稿：`{_display_path(draft_output)}`",
        formal_line,
        f"- 可用官方附件数：`{usable_sources}`",
        f"- 候选行数：`{len(candidate_frame)}`",
        f"- 批次数：`{summary.get('candidate_batches')}`",
        f"- 调入样本数：`{summary.get('treated_rows')}`",
        f"- 备选对照数：`{summary.get('control_rows')}`",
        f"- 覆盖 cutoff 两侧批次数：`{summary.get('crossing_batches')}`",
        "",
        "口径说明：",
        "- 本采集只接受中证指数官网公告详情页及其官方附件。",
        "- 正式调入样本来自“沪深300指数样本调整名单”的调入列。",
        "- 对照样本来自“沪深300指数备选名单”的官方排序。",
        "- `running_variable` 是基于官方调入顺序与备选排序映射出的边界序数变量；它不是单独发布的市值分数。",
        "- 该文件可以替代公开重建 L2 样本进入正式 L3 文件路径，但论文正文应披露上述序数变量口径。",
        "",
        "验收命令：",
        f"- `index-inclusion-prepare-hs300-rdd --input {_display_path(draft_output)} --check-only`",
        f"- `index-inclusion-prepare-hs300-rdd --input {_display_path(draft_output)} --output data/raw/hs300_rdd_candidates.csv --force`",
        "- `index-inclusion-hs300-rdd && index-inclusion-rebuild-all`",
    ]
    return "\n".join(lines) + "\n"


def collect_official_hs300_sources(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    draft_output: Path = DEFAULT_DRAFT_OUTPUT,
    audit_output: Path = DEFAULT_AUDIT_OUTPUT,
    report_output: Path = DEFAULT_REPORT_OUTPUT,
    attachment_dir: Path = DEFAULT_ATTACHMENT_DIR,
    formal_output: Path | None = None,
    force: bool = False,
    max_notices: int | None = None,
) -> dict[str, object]:
    for path in [draft_output, audit_output, report_output]:
        if path.exists() and not force:
            raise FileExistsError(f"Refusing to overwrite existing file without --force: {path}")
    if formal_output is not None and formal_output.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite existing file without --force: {formal_output}")

    output_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    notices = query_rebalance_announcements(session)
    if max_notices is not None:
        notices = notices[-max_notices:]

    source_rows: list[dict[str, object]] = []
    candidate_rows: list[dict[str, object]] = []
    for notice in notices:
        detail = fetch_notice_detail(session, int(notice["id"]))
        source_rows.append(_notice_audit_row(notice, has_detail=True))
        for link in _attachment_links_from_detail(detail):
            local_path: Path | None = None
            parsed: ParsedHs300Attachment | None = None
            status = "skipped"
            reason = "Attachment is not a PDF adjustment/result list."
            if link.file_url.lower().endswith(".pdf"):
                try:
                    local_path = _download_attachment(session, link, attachment_dir)
                    parsed = parse_hs300_attachment_text(_pdf_to_text(local_path))
                    if parsed.usable_for_l3:
                        candidate_rows.extend(build_candidate_rows(link, parsed))
                        status = "parsed"
                        reason = "Official adjustment and reserve lists parsed for HS300."
                    else:
                        status = "parsed_without_l3_controls"
                        reason = "Attachment did not expose both HS300 additions and reserve controls."
                except Exception as exc:  # pragma: no cover - exercised by live collection, not unit fixtures
                    status = "error"
                    reason = str(exc)
            source_rows.append(
                _source_audit_row(
                    link=link,
                    status=status,
                    local_path=local_path,
                    parsed=parsed,
                    reason=reason,
                )
            )

    if not candidate_rows:
        raise ValueError("No official HS300 adjustment + reserve candidate rows were collected.")

    candidate_frame = validate_candidate_frame(pd.DataFrame(candidate_rows)).sort_values(
        ["announce_date", "inclusion", "running_variable", "ticker"],
        ascending=[True, False, False, True],
    )
    source_audit = pd.DataFrame(source_rows)
    candidate_audit = build_candidate_batch_audit(candidate_frame)

    save_dataframe(candidate_frame, draft_output)
    save_dataframe(source_audit, audit_output)
    if formal_output is not None:
        save_dataframe(candidate_frame, formal_output)
    report_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.write_text(
        _build_report(
            draft_output=draft_output,
            formal_output=formal_output,
            audit_output=audit_output,
            candidate_frame=candidate_frame,
            audit_frame=source_audit,
            candidate_audit=candidate_audit,
        ),
        encoding="utf-8",
    )
    return {
        "draft_output": draft_output,
        "audit_output": audit_output,
        "report_output": report_output,
        "formal_output": formal_output,
        "candidate_rows": len(candidate_frame),
        "source_rows": len(source_audit),
        "candidate_batches": summarize_candidate_audit(candidate_audit).get("candidate_batches"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect official online CSIndex sources for HS300 RDD L3 candidates.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--draft-output", type=Path, default=DEFAULT_DRAFT_OUTPUT)
    parser.add_argument("--audit-output", type=Path, default=DEFAULT_AUDIT_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    parser.add_argument("--attachment-dir", type=Path, default=DEFAULT_ATTACHMENT_DIR)
    parser.add_argument("--max-notices", type=int, default=None)
    parser.add_argument("--write-formal", action="store_true", help="Write data/raw/hs300_rdd_candidates.csv.")
    parser.add_argument("--formal-output", type=Path, default=DEFAULT_FORMAL_OUTPUT)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    formal_output = args.formal_output if args.write_formal else None
    outputs = collect_official_hs300_sources(
        output_dir=args.output_dir,
        draft_output=args.draft_output,
        audit_output=args.audit_output,
        report_output=args.report_output,
        attachment_dir=args.attachment_dir,
        formal_output=formal_output,
        force=args.force,
        max_notices=args.max_notices,
    )
    print("HS300 official online collection completed.")
    print(f"Draft candidates: {_display_path(outputs['draft_output'])}")
    print(f"Source audit: {_display_path(outputs['audit_output'])}")
    print(f"Report: {_display_path(outputs['report_output'])}")
    if outputs["formal_output"] is not None:
        print(f"Formal candidates: {_display_path(outputs['formal_output'])}")
    print(f"Candidate rows: {outputs['candidate_rows']}")
    print(f"Source rows: {outputs['source_rows']}")
    print(f"Candidate batches: {outputs['candidate_batches']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
