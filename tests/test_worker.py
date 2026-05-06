from __future__ import annotations

from app.worker import _audit_reason_from_exception


def test_audit_reason_from_exception_includes_message():
    assert _audit_reason_from_exception(RuntimeError("boom")) == "RuntimeError: boom"


def test_audit_reason_from_exception_avoids_duplicate_prefix():
    assert _audit_reason_from_exception(RuntimeError("RuntimeError: boom")) == "RuntimeError: boom"


def test_audit_reason_from_exception_falls_back_to_name():
    assert _audit_reason_from_exception(RuntimeError("")) == "RuntimeError"


def test_audit_reason_from_exception_extracts_http_code():
    assert (
        _audit_reason_from_exception(RuntimeError("[plex] audit manifest HTTP 504 for channel-123"))
        == "HTTP 504: [plex] audit manifest HTTP 504 for channel-123"
    )
