from __future__ import annotations

import argparse
import datetime
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict
from urllib.parse import quote

import pandas as pd
import requests
from bs4 import BeautifulSoup

from index_inclusion_research import paths
from index_inclusion_research.analysis.rdd_candidates import (
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
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
DEFAULT_SEARCH_DIAGNOSTICS_OUTPUT = DEFAULT_OUTPUT_DIR / "online_search_diagnostics.csv"
DEFAULT_YEAR_COVERAGE_OUTPUT = DEFAULT_OUTPUT_DIR / "online_year_coverage.csv"
DEFAULT_MANUAL_GAP_WORKLIST_OUTPUT = DEFAULT_OUTPUT_DIR / "online_manual_gap_worklist.csv"
DEFAULT_GAP_SOURCE_HINTS_OUTPUT = DEFAULT_OUTPUT_DIR / "online_gap_source_hints.csv"
DEFAULT_REPORT_OUTPUT = DEFAULT_OUTPUT_DIR / "online_collection_report.md"
DEFAULT_ATTACHMENT_DIR = DEFAULT_OUTPUT_DIR / "official_attachments"
DEFAULT_FORMAL_OUTPUT = ROOT / "data" / "raw" / "hs300_rdd_candidates.csv"
DEFAULT_NOTICE_ROWS = 80
DEFAULT_TIMEOUT = 30
CSI300_CUTOFF = 300.0
SOURCE_AUDIT_COLUMNS = [
    "source_kind",
    "announcement_id",
    "publish_date",
    "effective_date",
    "title",
    "detail_url",
    "attachment_name",
    "attachment_url",
    "local_path",
    "status",
    "usable_for_l3",
    "addition_rows",
    "control_rows",
    "candidate_rows",
    "reason",
]
SEARCH_DIAGNOSTIC_COLUMNS = [
    "search_term",
    "requested_rows",
    "api_code",
    "status",
    "raw_rows",
    "hs300_title_rows",
    "title_matched_rows",
    "theme_matched_rows",
    "matched_rows",
    "matched_notice_ids",
    "matched_publish_dates",
    "date_filtered_matched_rows",
    "date_filtered_notice_ids",
    "sample_titles",
    "reason",
]
YEAR_COVERAGE_COLUMNS = [
    "year",
    "notice_rows",
    "attachment_rows",
    "usable_attachment_rows",
    "parsed_addition_rows",
    "parsed_control_rows",
    "candidate_rows",
    "candidate_batches",
    "status",
]
MANUAL_GAP_WORKLIST_COLUMNS = [
    "year",
    "priority",
    "gap_type",
    "announcement_id",
    "publish_date",
    "title",
    "detail_url",
    "attachment_name",
    "attachment_url",
    "local_path",
    "addition_rows",
    "control_rows",
    "missing_evidence",
    "suggested_next_step",
]
GAP_SOURCE_HINT_COLUMNS = [
    "year",
    "priority",
    "gap_type",
    "announcement_id",
    "source_kind",
    "source_label",
    "source_url",
    "query",
    "expected_evidence",
    "notes",
]

SEARCH_TERMS = (
    "沪深300、中证500、中证1000",
    "沪深300等指数样本",
    "关于沪深300、中证500、中证1000等指数定期调整结果的公告",
    "关于沪深300、中证500、中证1000、中证A500等指数定期调整结果的公告",
    "沪深300 指数样本 调整",
    "沪深300 指数样本股 调整",
    "调整沪深300指数样本",
    "调整沪深300指数样本股",
    "沪深300 定期调整",
)


class OfficialHs300SourcesOutputs(TypedDict):
    draft_output: Path
    audit_output: Path
    search_diagnostics_output: Path
    year_coverage_output: Path
    manual_gap_worklist_output: Path
    gap_source_hints_output: Path
    report_output: Path
    formal_output: Path | None
    candidate_rows: int
    source_rows: int
    search_rows: int
    year_rows: int
    gap_rows: int
    hint_rows: int
    candidate_batches: int | None
    status: str


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


def _clean_cell_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return _clean_text(value)


def _safe_int(value: object, default: int = 0) -> int:
    if value is None or pd.isna(value):
        return default
    try:
        return int(float(str(value)))
    except ValueError:
        return default


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


def _is_hs300_rebalance_title(title: str) -> bool:
    compact = re.sub(r"\s+", "", title)
    if "沪深300" not in compact:
        return False
    return bool(
        re.search(
            r"(定期调整(?:结果)?|指数样本调整名单|样本调整名单|样本股调整|调整.*指数样本|指数样本.*调整)",
            compact,
        )
    )


def _infer_csi300_effective_date(publish_date: str, title: str) -> str:
    """Fallback: CSI300 semi-annual rebalances take effect on the 2nd Friday of the
    calendar month following announce_date. Stable convention 2010-2025+ across
    every batch in data/raw/hs300_rdd_candidates.csv. Only applied when the notice
    title looks like an HS300 rebalance announcement.
    """
    if not publish_date or not _is_hs300_rebalance_title(title):
        return ""
    try:
        announce = pd.Timestamp(publish_date)
    except (TypeError, ValueError):
        return ""
    next_month_first = (announce + pd.offsets.MonthBegin(1)).date()
    days_to_friday = (4 - next_month_first.weekday()) % 7
    second_friday = next_month_first + datetime.timedelta(days=days_to_friday + 7)
    return second_friday.strftime("%Y-%m-%d")


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


def _join_values(values: Sequence[object]) -> str:
    return " | ".join(str(value) for value in values if str(value).strip())


def query_rebalance_announcements(
    session: requests.Session,
    *,
    search_terms: tuple[str, ...] = SEARCH_TERMS,
    rows: int = DEFAULT_NOTICE_ROWS,
    search_diagnostics: list[dict[str, object]] | None = None,
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
        raw_data = payload.get("data")
        raw_rows = raw_data if isinstance(raw_data, list) else []
        diagnostic = {
            "search_term": term,
            "requested_rows": rows,
            "api_code": str(payload.get("code", "")),
            "status": "ok" if str(payload.get("code")) == "200" else "api_code_not_200",
            "raw_rows": len(raw_rows),
            "hs300_title_rows": 0,
            "title_matched_rows": 0,
            "theme_matched_rows": 0,
            "matched_rows": 0,
            "matched_notice_ids": "",
            "matched_publish_dates": "",
            "date_filtered_matched_rows": 0,
            "date_filtered_notice_ids": "",
            "sample_titles": "",
            "reason": "",
        }
        if str(payload.get("code")) != "200":
            if search_diagnostics is not None:
                diagnostic["reason"] = "CSIndex API returned a non-200 code."
                search_diagnostics.append(diagnostic)
            continue
        matched_ids: list[int] = []
        matched_dates: list[str] = []
        matched_titles: list[str] = []
        for raw_row in raw_rows:
            if not isinstance(raw_row, Mapping):
                continue
            row = raw_row
            title = _clean_text(row.get("title"))
            theme = _clean_text(row.get("theme"))
            if "沪深300" in title:
                diagnostic["hs300_title_rows"] = _safe_int(diagnostic["hs300_title_rows"]) + 1
            if not _is_hs300_rebalance_title(title):
                continue
            diagnostic["title_matched_rows"] = _safe_int(diagnostic["title_matched_rows"]) + 1
            if theme and theme != "指数调样":
                continue
            diagnostic["theme_matched_rows"] = _safe_int(diagnostic["theme_matched_rows"]) + 1
            notice_id = _safe_int(row.get("id"))
            if not notice_id:
                continue
            matched_ids.append(notice_id)
            matched_dates.append(_clean_text(row.get("publishDate")))
            matched_titles.append(title)
            notices[notice_id] = {
                "id": notice_id,
                "title": title,
                "theme": theme,
                "publish_date": _clean_text(row.get("publishDate")),
                "search_term": term,
                "detail_url": f"{CSINDEX_SITE}/zh-CN/about/newsDetail?id={notice_id}",
            }
        if search_diagnostics is not None:
            diagnostic["matched_rows"] = len(matched_ids)
            diagnostic["matched_notice_ids"] = _join_values(matched_ids)
            diagnostic["matched_publish_dates"] = _join_values(matched_dates)
            diagnostic["sample_titles"] = _join_values(matched_titles[:3])
            if not matched_ids:
                diagnostic["reason"] = "No rows matched the HS300 rebalance title/theme filters."
            search_diagnostics.append(diagnostic)
    return sorted(notices.values(), key=lambda item: (str(item["publish_date"]), _safe_int(item.get("id"))))


def _parse_date_bound(value: str | None, *, name: str) -> pd.Timestamp | None:
    if not value:
        return None
    try:
        return pd.Timestamp(value).normalize()
    except ValueError as exc:
        raise ValueError(f"{name} must be a parseable date, got {value!r}") from exc


def _filter_notices_by_publish_date(
    notices: list[dict[str, object]],
    *,
    since: str | None = None,
    until: str | None = None,
) -> list[dict[str, object]]:
    since_ts = _parse_date_bound(since, name="since")
    until_ts = _parse_date_bound(until, name="until")
    if since_ts is not None and until_ts is not None and since_ts > until_ts:
        raise ValueError(f"since must be <= until, got {since!r} > {until!r}")
    if since_ts is None and until_ts is None:
        return list(notices)

    filtered: list[dict[str, object]] = []
    for notice in notices:
        raw_date = str(notice.get("publish_date", "") or "").strip()
        if not raw_date:
            continue
        try:
            publish_ts = pd.Timestamp(raw_date).normalize()
        except ValueError:
            continue
        if since_ts is not None and publish_ts < since_ts:
            continue
        if until_ts is not None and publish_ts > until_ts:
            continue
        filtered.append(notice)
    return filtered


def _annotate_search_diagnostics(
    diagnostics: list[dict[str, object]],
    filtered_notices: list[dict[str, object]],
) -> list[dict[str, object]]:
    filtered_ids = {str(notice.get("id")) for notice in filtered_notices}
    annotated: list[dict[str, object]] = []
    for row in diagnostics:
        current = dict(row)
        matched_ids = [
            item.strip()
            for item in str(current.get("matched_notice_ids", "") or "").split("|")
            if item.strip()
        ]
        kept_ids = [notice_id for notice_id in matched_ids if notice_id in filtered_ids]
        current["date_filtered_matched_rows"] = len(kept_ids)
        current["date_filtered_notice_ids"] = _join_values(kept_ids)
        if matched_ids and not kept_ids and not current.get("reason"):
            current["reason"] = "Matched notices exist but fall outside the requested date window."
        annotated.append(current)
    return annotated


def fetch_notice_detail(session: requests.Session, notice_id: int) -> dict[str, object]:
    response = session.get(
        f"{CSINDEX_HOME}/announcement/queryAnnouncementById",
        params={"id": notice_id},
        headers=_request_headers(),
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data")
    if str(payload.get("code")) != "200" or not isinstance(data, Mapping):
        raise ValueError(f"CSIndex notice detail unavailable for id={notice_id}")
    return dict(data)


def _attachment_links_from_detail(detail: dict[str, object]) -> list[AttachmentLink]:
    notice_id = _safe_int(detail.get("id"))
    title = _clean_text(detail.get("title"))
    publish_date = _clean_text(detail.get("publishDate"))
    content = str(detail.get("content") or "")
    effective_date = _extract_effective_date(content)
    if not effective_date:
        effective_date = _infer_csi300_effective_date(publish_date, title)
    detail_url = f"{CSINDEX_SITE}/zh-CN/about/newsDetail?id={notice_id}"
    links: list[AttachmentLink] = []
    seen: set[str] = set()

    enclosure_list = detail.get("enclosureList")
    enclosures = enclosure_list if isinstance(enclosure_list, list) else []
    for raw_item in enclosures:
        if not isinstance(raw_item, Mapping):
            continue
        item = raw_item
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


def _ticker_text(value: object) -> str:
    text = _clean_text(value)
    if re.fullmatch(r"\d+(?:\.0)?", text):
        text = text.split(".", maxsplit=1)[0]
    return text.zfill(6) if text.isdigit() else text


def _is_missing_marker(value: object) -> bool:
    return _clean_text(value) in {"", "-", "—", "nan", "NaN", "None"}


def _is_hs300_excel_row(row: pd.Series) -> bool:
    index_code = _ticker_text(row.iloc[0] if len(row) > 0 else "")
    index_name = _clean_text(row.iloc[1] if len(row) > 1 else "")
    return index_code == "000300" or index_name == "沪深300"


def _excel_adjustment_rows(frame: pd.DataFrame) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    deletions: list[dict[str, object]] = []
    additions: list[dict[str, object]] = []
    if frame.empty or frame.shape[1] < 6:
        return deletions, additions
    for _, row in frame.iterrows():
        if not _is_hs300_excel_row(row):
            continue
        delete_code = _ticker_text(row.iloc[2])
        delete_name = _clean_text(row.iloc[3])
        add_code = _ticker_text(row.iloc[4])
        add_name = _clean_text(row.iloc[5])
        order = len(additions) + 1
        if re.fullmatch(r"\d{6}", delete_code) and not _is_missing_marker(delete_name):
            deletions.append(
                {
                    "order": order,
                    "ticker": delete_code,
                    "security_name": delete_name,
                    "role": "deletion",
                }
            )
        if re.fullmatch(r"\d{6}", add_code) and not _is_missing_marker(add_name):
            additions.append(
                {
                    "order": order,
                    "ticker": add_code,
                    "security_name": add_name,
                    "role": "addition",
                }
            )
    return deletions, additions


def _excel_single_role_rows(frame: pd.DataFrame, *, role: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if frame.empty or frame.shape[1] < 4:
        return rows
    for _, row in frame.iterrows():
        if not _is_hs300_excel_row(row):
            continue
        ticker = _ticker_text(row.iloc[2])
        name = _clean_text(row.iloc[3])
        if not re.fullmatch(r"\d{6}", ticker) or _is_missing_marker(name):
            continue
        rows.append({"ticker": ticker, "security_name": name, "role": role})
    return rows


def _excel_reserve_rows(frame: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if frame.empty or frame.shape[1] < 5:
        return rows
    for _, row in frame.iterrows():
        if not _is_hs300_excel_row(row):
            continue
        rank_text = _clean_text(row.iloc[2])
        ticker = _ticker_text(row.iloc[3])
        name = _clean_text(row.iloc[4])
        if not rank_text.isdigit() or not re.fullmatch(r"\d{6}", ticker) or _is_missing_marker(name):
            continue
        rows.append(
            {
                "rank": int(rank_text),
                "ticker": ticker,
                "security_name": name,
                "role": "reserve",
            }
        )
    return sorted(rows, key=lambda item: _safe_int(item.get("rank")))


def _normalized_sheet_name(name: object) -> str:
    return _clean_text(name)


def parse_hs300_excel_attachment(excel_path: Path) -> ParsedHs300Attachment:
    deletions: list[dict[str, object]] = []
    additions: list[dict[str, object]] = []
    reserves: list[dict[str, object]] = []
    workbook = pd.ExcelFile(excel_path)
    for sheet_name in workbook.sheet_names:
        frame = pd.read_excel(workbook, sheet_name=sheet_name, header=None, dtype=object)
        if frame.empty:
            continue
        normalized = _normalized_sheet_name(sheet_name)
        if normalized == "调入":
            for row in _excel_single_role_rows(frame, role="addition"):
                row["order"] = len(additions) + 1
                additions.append(row)
        elif normalized == "调出":
            for row in _excel_single_role_rows(frame, role="deletion"):
                row["order"] = len(deletions) + 1
                deletions.append(row)
        elif normalized == "备选名单":
            reserves.extend(_excel_reserve_rows(frame))
        else:
            sheet_deletions, sheet_additions = _excel_adjustment_rows(frame)
            for row in sheet_deletions:
                row["order"] = len(deletions) + 1
                deletions.append(row)
            for row in sheet_additions:
                row["order"] = len(additions) + 1
                additions.append(row)
    return ParsedHs300Attachment(deletions=deletions, additions=additions, reserves=reserves)


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
    return sorted(rows, key=lambda item: _safe_int(item.get("rank")))


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
    additions = sorted(parsed.additions, key=lambda item: _safe_int(item.get("order")))
    n_additions = len(additions)
    for item in additions:
        order = _safe_int(item.get("order"))
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
    for item in sorted(parsed.reserves, key=lambda row: _safe_int(row.get("rank"))):
        rank = _safe_int(item.get("rank"))
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


def _year_from_date(value: object) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(pd.Timestamp(raw).year)
    except ValueError:
        return None


def _int_cell(value: object) -> int:
    try:
        return int(float(str(value or "0")))
    except ValueError:
        return 0


def _requested_years(
    *,
    since: str | None,
    until: str | None,
    source_audit: pd.DataFrame,
    candidate_frame: pd.DataFrame,
) -> list[int]:
    since_ts = _parse_date_bound(since, name="since")
    until_ts = _parse_date_bound(until, name="until")
    if since_ts is not None and until_ts is not None:
        return list(range(int(since_ts.year), int(until_ts.year) + 1))

    years: set[int] = set()
    if not source_audit.empty and "publish_date" in source_audit:
        years.update(source_audit["publish_date"].map(_year_from_date).dropna().astype(int))
    if not candidate_frame.empty and "announce_date" in candidate_frame:
        years.update(candidate_frame["announce_date"].map(_year_from_date).dropna().astype(int))
    if since_ts is not None:
        years = {year for year in years if year >= int(since_ts.year)}
        years.add(int(since_ts.year))
    if until_ts is not None:
        years = {year for year in years if year <= int(until_ts.year)}
        years.add(int(until_ts.year))
    return sorted(years)


def _build_year_coverage_frame(
    *,
    source_audit: pd.DataFrame,
    candidate_frame: pd.DataFrame,
    since: str | None,
    until: str | None,
) -> pd.DataFrame:
    years = _requested_years(since=since, until=until, source_audit=source_audit, candidate_frame=candidate_frame)
    rows: list[dict[str, object]] = []
    for year in years:
        if source_audit.empty:
            source_year = pd.DataFrame(columns=SOURCE_AUDIT_COLUMNS)
        else:
            source_year = source_audit.loc[source_audit["publish_date"].map(_year_from_date) == year]
        notice_rows = int((source_year["source_kind"] == "official_rebalance_result_notice").sum()) if not source_year.empty else 0
        attachment_rows = int((source_year["source_kind"] == "official_adjustment_backup_attachment").sum()) if not source_year.empty else 0
        usable_attachment_rows = int(source_year["usable_for_l3"].fillna(False).sum()) if not source_year.empty else 0
        parsed_addition_rows = (
            int(pd.to_numeric(source_year["addition_rows"], errors="coerce").fillna(0).sum())
            if not source_year.empty
            else 0
        )
        parsed_control_rows = (
            int(pd.to_numeric(source_year["control_rows"], errors="coerce").fillna(0).sum())
            if not source_year.empty
            else 0
        )
        if candidate_frame.empty:
            candidate_year = pd.DataFrame(columns=candidate_frame.columns)
        else:
            candidate_year = candidate_frame.loc[candidate_frame["announce_date"].map(_year_from_date) == year]
        candidate_rows = int(len(candidate_year))
        candidate_batches = int(candidate_year["batch_id"].nunique()) if not candidate_year.empty else 0
        if candidate_rows:
            status = "candidate_found"
        elif notice_rows or attachment_rows:
            status = "notice_only"
        else:
            status = "no_notice"
        rows.append(
            {
                "year": year,
                "notice_rows": notice_rows,
                "attachment_rows": attachment_rows,
                "usable_attachment_rows": usable_attachment_rows,
                "parsed_addition_rows": parsed_addition_rows,
                "parsed_control_rows": parsed_control_rows,
                "candidate_rows": candidate_rows,
                "candidate_batches": candidate_batches,
                "status": status,
            }
        )
    return pd.DataFrame(rows, columns=YEAR_COVERAGE_COLUMNS)


def _gap_row(
    *,
    year: int | None,
    priority: str,
    gap_type: str,
    source_row: dict[str, object] | None = None,
    missing_evidence: str,
    suggested_next_step: str,
) -> dict[str, object]:
    source_row = source_row or {}
    return {
        "year": year or "",
        "priority": priority,
        "gap_type": gap_type,
        "announcement_id": source_row.get("announcement_id", ""),
        "publish_date": source_row.get("publish_date", ""),
        "title": source_row.get("title", ""),
        "detail_url": source_row.get("detail_url", ""),
        "attachment_name": source_row.get("attachment_name", ""),
        "attachment_url": source_row.get("attachment_url", ""),
        "local_path": source_row.get("local_path", ""),
        "addition_rows": _int_cell(source_row.get("addition_rows", 0)),
        "control_rows": _int_cell(source_row.get("control_rows", 0)),
        "missing_evidence": missing_evidence,
        "suggested_next_step": suggested_next_step,
    }


def _build_manual_gap_worklist_frame(
    *,
    source_audit: pd.DataFrame,
    year_coverage: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    attachment_ids: set[str] = set()
    year_statuses: dict[int, str] = {}
    if not year_coverage.empty and {"year", "status"}.issubset(year_coverage.columns):
        for _, item in year_coverage.iterrows():
            year = _int_cell(item.get("year", 0))
            if year:
                year_statuses[year] = str(item.get("status", ""))
    if not source_audit.empty:
        attachment_rows = source_audit.loc[
            source_audit["source_kind"].astype(str) == "official_adjustment_backup_attachment"
        ]
        attachment_ids = {str(value) for value in attachment_rows["announcement_id"].dropna()}
        for _, item in attachment_rows.iterrows():
            source_row = item.to_dict()
            status = str(source_row.get("status", ""))
            publish_year = _year_from_date(source_row.get("publish_date"))
            year_status = year_statuses.get(publish_year or 0, "")
            if status == "parsed_without_l3_controls":
                additions = _int_cell(source_row.get("addition_rows", 0))
                priority = "P1" if additions and year_status != "candidate_found" else "P2"
                rows.append(
                    _gap_row(
                        year=publish_year,
                        priority=priority,
                        gap_type="parsed_additions_missing_controls",
                        source_row=source_row,
                        missing_evidence="official reserve/control list or boundary ranking",
                        suggested_next_step=(
                            "Locate the official reserve list / boundary ranking for this announcement; "
                            "do not import the addition-only attachment into formal L3 until controls are available."
                        ),
                    )
                )
            elif status in {"skipped", "error"}:
                if year_status == "candidate_found":
                    continue
                rows.append(
                    _gap_row(
                        year=publish_year,
                        priority="P2",
                        gap_type="unparsed_attachment",
                        source_row=source_row,
                        missing_evidence="parseable official adjustment and reserve attachment",
                        suggested_next_step=(
                            "Inspect the attachment format or archive copy; add a parser only if it exposes "
                            "both HS300 additions and reserve controls."
                        ),
                    )
                )
        notice_rows = source_audit.loc[source_audit["source_kind"].astype(str) == "official_rebalance_result_notice"]
        for _, item in notice_rows.iterrows():
            source_row = item.to_dict()
            publish_year = _year_from_date(source_row.get("publish_date"))
            if year_statuses.get(publish_year or 0, "") == "candidate_found":
                continue
            announcement_id = str(source_row.get("announcement_id", ""))
            if announcement_id in attachment_ids:
                continue
            rows.append(
                _gap_row(
                    year=publish_year,
                    priority="P2",
                    gap_type="notice_without_attachment",
                    source_row=source_row,
                    missing_evidence="official adjustment/reserve attachment URL",
                    suggested_next_step=(
                        "Find the missing official attachment through CSIndex history, CNInfo, or archive snapshots, "
                        "then rerun the collector."
                    ),
                )
            )
    if not year_coverage.empty:
        for _, item in year_coverage.iterrows():
            status = str(item.get("status", ""))
            if status != "no_notice":
                continue
            missing_year = _int_cell(item.get("year", 0)) or None
            rows.append(
                _gap_row(
                    year=missing_year,
                    priority="P3",
                    gap_type="year_without_notice",
                    missing_evidence="CSIndex rebalance announcement and official attachments",
                    suggested_next_step=(
                        "Add targeted historical search terms for this year, or look for archived CSIndex/CNInfo records."
                    ),
                )
            )
    frame = pd.DataFrame(rows, columns=MANUAL_GAP_WORKLIST_COLUMNS)
    if frame.empty:
        return frame
    priority_order = {"P1": 1, "P2": 2, "P3": 3}
    frame["_priority_order"] = frame["priority"].map(priority_order).fillna(9)
    frame["_year_order"] = pd.to_numeric(frame["year"], errors="coerce").fillna(9999)
    frame = frame.sort_values(["_priority_order", "_year_order", "gap_type", "announcement_id"]).drop(
        columns=["_priority_order", "_year_order"]
    )
    return frame.loc[:, MANUAL_GAP_WORKLIST_COLUMNS].reset_index(drop=True)


def _wayback_url(url: str) -> str:
    return f"https://web.archive.org/web/*/{quote(url, safe=':/?&=%')}"


def _search_url(query: str) -> str:
    return f"https://www.bing.com/search?q={quote(query)}"


def _cninfo_search_url(query: str) -> str:
    return f"https://www.cninfo.com.cn/new/fulltextSearch?notautosubmit=&keyWord={quote(query)}"


def _gap_base_query(row: dict[str, object]) -> str:
    parts = [
        _clean_cell_text(row.get("year")),
        _clean_cell_text(row.get("title")),
        _clean_cell_text(row.get("attachment_name")),
        "沪深300 备选名单 reserve control",
    ]
    return " ".join(part for part in parts if part)


def _build_gap_source_hints_frame(gap_worklist: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    seen: set[tuple[object, str, str]] = set()

    def add_hint(
        gap: dict[str, object],
        *,
        source_kind: str,
        source_label: str,
        source_url: str,
        query: str = "",
    ) -> None:
        key = (gap.get("announcement_id", ""), source_kind, source_url or query)
        if key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "year": gap.get("year", ""),
                "priority": gap.get("priority", ""),
                "gap_type": gap.get("gap_type", ""),
                "announcement_id": gap.get("announcement_id", ""),
                "source_kind": source_kind,
                "source_label": source_label,
                "source_url": source_url,
                "query": query,
                "expected_evidence": _clean_cell_text(gap.get("missing_evidence")),
                "notes": _clean_cell_text(gap.get("suggested_next_step")),
            }
        )

    if gap_worklist.empty:
        return pd.DataFrame(columns=GAP_SOURCE_HINT_COLUMNS)

    for _, item in gap_worklist.iterrows():
        gap = item.to_dict()
        detail_url = _clean_cell_text(gap.get("detail_url"))
        attachment_url = _clean_cell_text(gap.get("attachment_url"))
        query = _gap_base_query(gap)
        site_query = f"site:csindex.com.cn {query}".strip()
        cninfo_query = f"{query} 中证指数 公告".strip()

        if detail_url:
            add_hint(
                gap,
                source_kind="csindex_detail",
                source_label="中证公告详情页",
                source_url=detail_url,
            )
            add_hint(
                gap,
                source_kind="wayback_detail",
                source_label="Wayback 公告详情归档",
                source_url=_wayback_url(detail_url),
            )
        if attachment_url:
            add_hint(
                gap,
                source_kind="official_attachment",
                source_label="中证官方附件",
                source_url=attachment_url,
            )
            add_hint(
                gap,
                source_kind="wayback_attachment",
                source_label="Wayback 附件归档",
                source_url=_wayback_url(attachment_url),
            )
        add_hint(
            gap,
            source_kind="web_search_csindex",
            source_label="网页搜索：中证站内",
            source_url=_search_url(site_query),
            query=site_query,
        )
        add_hint(
            gap,
            source_kind="web_search_general",
            source_label="网页搜索：全网",
            source_url=_search_url(query),
            query=query,
        )
        add_hint(
            gap,
            source_kind="cninfo_fulltext_search",
            source_label="巨潮全文搜索",
            source_url=_cninfo_search_url(cninfo_query),
            query=cninfo_query,
        )

    frame = pd.DataFrame(rows, columns=GAP_SOURCE_HINT_COLUMNS)
    if frame.empty:
        return frame
    priority_order = {"P1": 1, "P2": 2, "P3": 3}
    source_order = {
        "csindex_detail": 1,
        "official_attachment": 2,
        "wayback_detail": 3,
        "wayback_attachment": 4,
        "web_search_csindex": 5,
        "web_search_general": 6,
        "cninfo_fulltext_search": 7,
    }
    frame["_priority_order"] = frame["priority"].map(priority_order).fillna(9)
    frame["_year_order"] = pd.to_numeric(frame["year"], errors="coerce").fillna(9999)
    frame["_source_order"] = frame["source_kind"].map(source_order).fillna(99)
    frame = frame.sort_values(["_priority_order", "_year_order", "announcement_id", "_source_order"]).drop(
        columns=["_priority_order", "_year_order", "_source_order"]
    )
    return frame.loc[:, GAP_SOURCE_HINT_COLUMNS].reset_index(drop=True)


def _build_report(
    *,
    draft_output: Path,
    formal_output: Path | None,
    audit_output: Path,
    search_diagnostics_output: Path,
    year_coverage_output: Path,
    manual_gap_worklist_output: Path,
    gap_source_hints_output: Path,
    candidate_frame: pd.DataFrame,
    audit_frame: pd.DataFrame,
    search_diagnostics_frame: pd.DataFrame,
    year_coverage_frame: pd.DataFrame,
    manual_gap_worklist_frame: pd.DataFrame,
    gap_source_hints_frame: pd.DataFrame,
    candidate_audit: pd.DataFrame,
) -> str:
    summary = summarize_candidate_audit(candidate_audit)
    usable_sources = int(audit_frame["usable_for_l3"].fillna(False).sum()) if not audit_frame.empty else 0
    partial_sources = int((audit_frame["status"].astype(str) == "parsed_without_l3_controls").sum()) if not audit_frame.empty else 0
    partial_addition_rows = (
        int(
            pd.to_numeric(
                audit_frame.loc[
                    audit_frame["status"].astype(str) == "parsed_without_l3_controls",
                    "addition_rows",
                ],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )
        if not audit_frame.empty
        else 0
    )
    search_raw_rows = int(search_diagnostics_frame["raw_rows"].sum()) if not search_diagnostics_frame.empty else 0
    search_matched_rows = int(search_diagnostics_frame["matched_rows"].sum()) if not search_diagnostics_frame.empty else 0
    search_date_rows = int(search_diagnostics_frame["date_filtered_matched_rows"].sum()) if not search_diagnostics_frame.empty else 0
    gap_rows = int(len(manual_gap_worklist_frame))
    hint_rows = int(len(gap_source_hints_frame))
    p1_gap_rows = (
        int((manual_gap_worklist_frame["priority"].astype(str) == "P1").sum())
        if not manual_gap_worklist_frame.empty
        else 0
    )
    formal_line = (
        f"- 正式文件：`{_display_path(formal_output)}`"
        if formal_output is not None
        else "- 正式文件：未写入；先保留草稿等待人工确认"
    )
    lines = [
        "# HS300 RDD 官方线上来源采集报告",
        "",
        "- 来源域名：`csindex.com.cn` 与 `oss-ch.csindex.com.cn`",
        f"- 搜索诊断：`{_display_path(search_diagnostics_output)}`",
        f"- 年份覆盖：`{_display_path(year_coverage_output)}`",
        f"- 人工补录清单：`{_display_path(manual_gap_worklist_output)}`",
        f"- 缺口来源查找入口：`{_display_path(gap_source_hints_output)}`",
        f"- 来源审计：`{_display_path(audit_output)}`",
        f"- 候选草稿：`{_display_path(draft_output)}`",
        formal_line,
        f"- 搜索返回原始行数：`{search_raw_rows}`",
        f"- 标题/主题匹配公告数：`{search_matched_rows}`",
        f"- 日期窗口内匹配公告数：`{search_date_rows}`",
        f"- 可用官方附件数：`{usable_sources}`",
        f"- 已解析但缺备选对照附件数：`{partial_sources}`（调入行 `{partial_addition_rows}`）",
        f"- 补录缺口行数：`{gap_rows}`（P1 `{p1_gap_rows}`）",
        f"- 缺口来源查找入口数：`{hint_rows}`",
        f"- 候选行数：`{len(candidate_frame)}`",
        f"- 批次数：`{summary.get('candidate_batches')}`",
        f"- 调入样本数：`{summary.get('treated_rows')}`",
        f"- 备选对照数：`{summary.get('control_rows')}`",
        f"- 覆盖 cutoff 两侧批次数：`{summary.get('crossing_batches')}`",
    ]
    if not year_coverage_frame.empty:
        no_notice_years = year_coverage_frame.loc[year_coverage_frame["status"] == "no_notice", "year"].astype(str).tolist()
        notice_only_years = year_coverage_frame.loc[year_coverage_frame["status"] == "notice_only", "year"].astype(str).tolist()
        candidate_years = year_coverage_frame.loc[year_coverage_frame["status"] == "candidate_found", "year"].astype(str).tolist()
        lines.extend(
            [
                f"- 已解析候选年份：`{_join_values(candidate_years) or '无'}`",
                f"- 仅命中公告/附件年份：`{_join_values(notice_only_years) or '无'}`",
                f"- 未命中公告年份：`{_join_values(no_notice_years) or '无'}`",
            ]
        )
    lines.extend(
        [
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
    )
    if candidate_frame.empty:
        lines.extend(
            [
                "",
                "采集状态：",
                "- 本次窗口没有解析出同时包含沪深300调入名单与备选名单的可用官方附件。",
            ]
        )
        if audit_frame.empty:
            if search_matched_rows and not search_date_rows:
                lines.append(
                    "- 搜索词命中过中证官网调样公告，但没有公告落在本次日期窗口；优先查看 "
                    "`online_search_diagnostics.csv` 的命中日期，再追加目标年份标题 `--search-term` "
                    "或转向 archive.org / CNInfo 等档案源。"
                )
            elif search_raw_rows and not search_matched_rows:
                lines.append(
                    "- 搜索接口有返回，但没有通过 HS300 调样标题/主题过滤；优先查看 "
                    "`online_search_diagnostics.csv` 的 sample title，再追加更贴近历史公告标题的 `--search-term`。"
                )
            else:
                lines.append("- 本次窗口也没有匹配到中证官网定期调整结果公告；优先扩大 `--notice-rows` 或调整日期窗口。")
        else:
            lines.append("- 请查看来源审计中的 `status` 与 `reason`，再决定是否扩大 `--notice-rows`、调整日期窗口或手工补录。")
    return "\n".join(lines) + "\n"


def _empty_candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS])


def collect_official_hs300_sources(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    draft_output: Path = DEFAULT_DRAFT_OUTPUT,
    audit_output: Path = DEFAULT_AUDIT_OUTPUT,
    search_diagnostics_output: Path = DEFAULT_SEARCH_DIAGNOSTICS_OUTPUT,
    year_coverage_output: Path = DEFAULT_YEAR_COVERAGE_OUTPUT,
    manual_gap_worklist_output: Path = DEFAULT_MANUAL_GAP_WORKLIST_OUTPUT,
    gap_source_hints_output: Path = DEFAULT_GAP_SOURCE_HINTS_OUTPUT,
    report_output: Path = DEFAULT_REPORT_OUTPUT,
    attachment_dir: Path = DEFAULT_ATTACHMENT_DIR,
    formal_output: Path | None = None,
    force: bool = False,
    max_notices: int | None = None,
    since: str | None = None,
    until: str | None = None,
    notice_rows: int = DEFAULT_NOTICE_ROWS,
    search_terms: tuple[str, ...] = SEARCH_TERMS,
) -> OfficialHs300SourcesOutputs:
    for path in [
        draft_output,
        audit_output,
        search_diagnostics_output,
        year_coverage_output,
        manual_gap_worklist_output,
        gap_source_hints_output,
        report_output,
    ]:
        if path.exists() and not force:
            raise FileExistsError(f"Refusing to overwrite existing file without --force: {path}")
    if formal_output is not None and formal_output.exists() and not force:
        raise FileExistsError(f"Refusing to overwrite existing file without --force: {formal_output}")

    output_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    search_rows: list[dict[str, object]] = []
    notices = query_rebalance_announcements(
        session,
        search_terms=search_terms,
        rows=notice_rows,
        search_diagnostics=search_rows,
    )
    notices = _filter_notices_by_publish_date(notices, since=since, until=until)
    search_rows = _annotate_search_diagnostics(search_rows, notices)
    if max_notices is not None:
        notices = notices[-max_notices:]

    source_rows: list[dict[str, object]] = []
    candidate_rows: list[dict[str, object]] = []
    for notice in notices:
        try:
            detail = fetch_notice_detail(session, _safe_int(notice.get("id")))
        except Exception as exc:  # pragma: no cover - live source availability varies
            row = _notice_audit_row(notice, has_detail=False)
            row["reason"] = f"Notice detail could not be fetched: {exc}"
            source_rows.append(row)
            continue
        source_rows.append(_notice_audit_row(notice, has_detail=True))
        for link in _attachment_links_from_detail(detail):
            local_path: Path | None = None
            parsed: ParsedHs300Attachment | None = None
            status = "skipped"
            reason = "Attachment is not a supported adjustment/result list."
            suffix = Path(link.file_url.split("?", maxsplit=1)[0]).suffix.lower()
            if suffix in {".pdf", ".xls", ".xlsx"}:
                try:
                    local_path = _download_attachment(session, link, attachment_dir)
                    if suffix == ".pdf":
                        parsed = parse_hs300_attachment_text(_pdf_to_text(local_path))
                    else:
                        parsed = parse_hs300_excel_attachment(local_path)
                    if parsed.usable_for_l3:
                        candidate_rows.extend(build_candidate_rows(link, parsed))
                        status = "parsed"
                        reason = "Official adjustment and reserve lists parsed for HS300."
                    else:
                        status = "parsed_without_l3_controls"
                        if parsed.additions:
                            reason = (
                                "Official HS300 additions parsed, but reserve controls are absent; "
                                "manual/archival reserve-list evidence is still required for L3 RDD."
                            )
                        else:
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

    if candidate_rows:
        candidate_frame = (
            pd.DataFrame(candidate_rows)
            .drop_duplicates(subset=["batch_id", "ticker", "inclusion"], keep="first")
            .reset_index(drop=True)
        )
        candidate_frame = validate_candidate_frame(candidate_frame).sort_values(
            ["announce_date", "inclusion", "running_variable", "ticker"],
            ascending=[True, False, False, True],
        )
    else:
        candidate_frame = _empty_candidate_frame()
    source_audit = pd.DataFrame(source_rows, columns=SOURCE_AUDIT_COLUMNS)
    search_diagnostics = pd.DataFrame(search_rows, columns=SEARCH_DIAGNOSTIC_COLUMNS)
    candidate_audit = build_candidate_batch_audit(candidate_frame)
    year_coverage = _build_year_coverage_frame(
        source_audit=source_audit,
        candidate_frame=candidate_frame,
        since=since,
        until=until,
    )
    manual_gap_worklist = _build_manual_gap_worklist_frame(
        source_audit=source_audit,
        year_coverage=year_coverage,
    )
    gap_source_hints = _build_gap_source_hints_frame(manual_gap_worklist)

    save_dataframe(candidate_frame, draft_output)
    save_dataframe(source_audit, audit_output)
    save_dataframe(search_diagnostics, search_diagnostics_output)
    save_dataframe(year_coverage, year_coverage_output)
    save_dataframe(manual_gap_worklist, manual_gap_worklist_output)
    save_dataframe(gap_source_hints, gap_source_hints_output)
    if formal_output is not None:
        save_dataframe(candidate_frame, formal_output)
    report_output.parent.mkdir(parents=True, exist_ok=True)
    report_output.write_text(
        _build_report(
            draft_output=draft_output,
            formal_output=formal_output,
            audit_output=audit_output,
            search_diagnostics_output=search_diagnostics_output,
            year_coverage_output=year_coverage_output,
            manual_gap_worklist_output=manual_gap_worklist_output,
            gap_source_hints_output=gap_source_hints_output,
            candidate_frame=candidate_frame,
            audit_frame=source_audit,
            search_diagnostics_frame=search_diagnostics,
            year_coverage_frame=year_coverage,
            manual_gap_worklist_frame=manual_gap_worklist,
            gap_source_hints_frame=gap_source_hints,
            candidate_audit=candidate_audit,
        ),
        encoding="utf-8",
    )
    return {
        "draft_output": draft_output,
        "audit_output": audit_output,
        "search_diagnostics_output": search_diagnostics_output,
        "year_coverage_output": year_coverage_output,
        "manual_gap_worklist_output": manual_gap_worklist_output,
        "gap_source_hints_output": gap_source_hints_output,
        "report_output": report_output,
        "formal_output": formal_output,
        "candidate_rows": len(candidate_frame),
        "source_rows": len(source_audit),
        "search_rows": len(search_diagnostics),
        "year_rows": len(year_coverage),
        "gap_rows": len(manual_gap_worklist),
        "hint_rows": len(gap_source_hints),
        "candidate_batches": summarize_candidate_audit(candidate_audit).get("candidate_batches"),
        "status": "parsed" if not candidate_frame.empty else "no_candidates",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect official online CSIndex sources for HS300 RDD L3 candidates.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--draft-output", type=Path, default=DEFAULT_DRAFT_OUTPUT)
    parser.add_argument("--audit-output", type=Path, default=DEFAULT_AUDIT_OUTPUT)
    parser.add_argument("--search-diagnostics-output", type=Path, default=DEFAULT_SEARCH_DIAGNOSTICS_OUTPUT)
    parser.add_argument("--year-coverage-output", type=Path, default=DEFAULT_YEAR_COVERAGE_OUTPUT)
    parser.add_argument("--manual-gap-worklist-output", type=Path, default=DEFAULT_MANUAL_GAP_WORKLIST_OUTPUT)
    parser.add_argument("--gap-source-hints-output", type=Path, default=DEFAULT_GAP_SOURCE_HINTS_OUTPUT)
    parser.add_argument("--report-output", type=Path, default=DEFAULT_REPORT_OUTPUT)
    parser.add_argument("--attachment-dir", type=Path, default=DEFAULT_ATTACHMENT_DIR)
    parser.add_argument("--max-notices", type=int, default=None)
    parser.add_argument("--since", default=None, help="Only collect notices published on/after this date, e.g. 2020-01-01.")
    parser.add_argument("--until", default=None, help="Only collect notices published on/before this date, e.g. 2022-12-31.")
    parser.add_argument("--notice-rows", type=int, default=DEFAULT_NOTICE_ROWS, help="Rows to request from each CSIndex search term.")
    parser.add_argument(
        "--search-term",
        action="append",
        default=[],
        help="Add an extra CSIndex search term; repeat the flag to add multiple terms.",
    )
    parser.add_argument("--write-formal", action="store_true", help="Write data/raw/hs300_rdd_candidates.csv.")
    parser.add_argument("--formal-output", type=Path, default=DEFAULT_FORMAL_OUTPUT)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    formal_output = args.formal_output if args.write_formal else None
    search_terms = tuple(dict.fromkeys([*SEARCH_TERMS, *args.search_term]))
    outputs = collect_official_hs300_sources(
        output_dir=args.output_dir,
        draft_output=args.draft_output,
        audit_output=args.audit_output,
        search_diagnostics_output=args.search_diagnostics_output,
        year_coverage_output=args.year_coverage_output,
        manual_gap_worklist_output=args.manual_gap_worklist_output,
        gap_source_hints_output=args.gap_source_hints_output,
        report_output=args.report_output,
        attachment_dir=args.attachment_dir,
        formal_output=formal_output,
        force=args.force,
        max_notices=args.max_notices,
        since=args.since,
        until=args.until,
        notice_rows=args.notice_rows,
        search_terms=search_terms,
    )
    print("HS300 official online collection completed.")
    print(f"Draft candidates: {_display_path(outputs['draft_output'])}")
    print(f"Source audit: {_display_path(outputs['audit_output'])}")
    print(f"Search diagnostics: {_display_path(outputs['search_diagnostics_output'])}")
    print(f"Year coverage: {_display_path(outputs['year_coverage_output'])}")
    print(f"Manual gap worklist: {_display_path(outputs['manual_gap_worklist_output'])}")
    print(f"Gap source hints: {_display_path(outputs['gap_source_hints_output'])}")
    print(f"Report: {_display_path(outputs['report_output'])}")
    if outputs["formal_output"] is not None:
        print(f"Formal candidates: {_display_path(outputs['formal_output'])}")
    if outputs.get("status") == "no_candidates":
        print("Status: no usable HS300 L3 candidate rows found; inspect the source audit, search diagnostics, year coverage, and report.")
    print(f"Candidate rows: {outputs['candidate_rows']}")
    print(f"Source rows: {outputs['source_rows']}")
    print(f"Search diagnostic rows: {outputs['search_rows']}")
    print(f"Year coverage rows: {outputs['year_rows']}")
    print(f"Manual gap rows: {outputs['gap_rows']}")
    print(f"Gap source hint rows: {outputs['hint_rows']}")
    print(f"Candidate batches: {outputs['candidate_batches']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
