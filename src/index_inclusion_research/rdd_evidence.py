from __future__ import annotations


def rdd_evidence_tier(mode: str) -> str:
    return {
        "real": "L3",
        "reconstructed": "L2",
        "demo": "L1",
        "missing": "L0",
    }.get(mode, "—")


def rdd_evidence_tier_from_status(status: str) -> str:
    return {
        "正式样本": "L3",
        "正式边界样本": "L3",
        "公开重建样本": "L2",
        "方法展示": "L1",
        "待补正式样本": "L0",
        "未生成": "—",
    }.get(status, "—")
