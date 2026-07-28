"""Local configuration migrations for moonraker-owl.

These migrations run from the installer/update path before the agent starts.
The package owns a small bootstrap subset of the local TOML (`cloud.*` target
settings), so plugin updates can reliably retarget every printer during rare
mandatory domain cutovers before the agent talks to Owl Cloud again.
"""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


MANAGED_BASE_URL = "https://staging.mewcon.net"
MANAGED_BROKER_HOST = "mqtt.staging.mewcon.net"
MANAGED_BROKER_PORT = 8883
MANAGED_BROKER_USE_TLS = True


class ConfigMigrationError(RuntimeError):
    """Raised when a local config migration cannot be completed."""


@dataclass(slots=True)
class ConfigMigrationResult:
    """Result of running local configuration migrations."""

    changed: bool
    changes: list[str] = field(default_factory=list)
    skipped_reason: Optional[str] = None
    backup_path: Optional[Path] = None


_SECTION_RE = re.compile(r"^\s*\[([^\]]+)]\s*(?:#.*)?$")


def migrate_config_file(path: Path) -> ConfigMigrationResult:
    """Apply safe local migrations to an existing TOML config file.

    The current migration force-aligns the package-owned bootstrap cloud target
    keys to the official Owl-managed environment for this plugin release.
    Per-device credentials and unrelated local settings stay untouched.
    """
    if not path.exists():
        return ConfigMigrationResult(changed=False, skipped_reason="config_missing")

    try:
        original = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigMigrationError(f"Could not read config file {path}: {exc}") from exc

    lines = original.splitlines(keepends=True)
    lines, start, end = _ensure_section(lines, "cloud")
    changes: list[str] = []

    base_url = _read_string_key(lines, start, end, "base_url")
    if _normalize_url(base_url or "") != MANAGED_BASE_URL:
        lines, start, end = _set_or_insert_string_key(
            lines,
            start,
            end,
            "base_url",
            MANAGED_BASE_URL,
            insert_after=None,
        )
        changes.append("cloud.base_url")

    broker_host = _read_string_key(lines, start, end, "broker_host")
    broker_host_name, broker_host_port = _split_host_port(broker_host or "")
    if broker_host_name != MANAGED_BROKER_HOST:
        lines, start, end = _set_or_insert_string_key(
            lines,
            start,
            end,
            "broker_host",
            MANAGED_BROKER_HOST,
            insert_after="base_url",
        )
        changes.append("cloud.broker_host")

    broker_port = _read_int_key(lines, start, end, "broker_port")
    if broker_port != MANAGED_BROKER_PORT or broker_host_port is not None:
        lines, start, end = _set_or_insert_int_key(
            lines,
            start,
            end,
            "broker_port",
            MANAGED_BROKER_PORT,
            insert_after="broker_host",
        )
        changes.append("cloud.broker_port")

    broker_use_tls = _read_bool_key(lines, start, end, "broker_use_tls")
    if broker_use_tls is not MANAGED_BROKER_USE_TLS:
        lines, start, end = _set_or_insert_bool_key(
            lines,
            start,
            end,
            "broker_use_tls",
            MANAGED_BROKER_USE_TLS,
            insert_after="broker_port",
        )
        changes.append("cloud.broker_use_tls")

    if not changes:
        return ConfigMigrationResult(changed=False, skipped_reason="already_current")

    backup_path = _write_backup(path, original)
    updated = "".join(lines)

    try:
        path.write_text(updated, encoding="utf-8")
        _preserve_owner_mode(path, backup_path)
    except OSError as exc:
        raise ConfigMigrationError(f"Could not write migrated config file {path}: {exc}") from exc

    return ConfigMigrationResult(changed=True, changes=changes, backup_path=backup_path)


def _find_section(lines: list[str], section: str) -> Optional[tuple[int, int]]:
    start: Optional[int] = None
    for index, line in enumerate(lines):
        match = _SECTION_RE.match(line)
        if match is None:
            continue

        if start is not None:
            return start, index

        if match.group(1).strip() == section:
            start = index + 1

    if start is None:
        return None

    return start, len(lines)


def _ensure_section(lines: list[str], section: str) -> tuple[list[str], int, int]:
    found = _find_section(lines, section)
    if found is not None:
        return lines, found[0], found[1]

    if lines:
        if not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        if lines[-1].strip():
            lines.append("\n")

    lines.append(f"[{section}]\n")
    start = len(lines)
    return lines, start, len(lines)


def _read_string_key(lines: list[str], start: int, end: int, key: str) -> Optional[str]:
    pattern = _key_pattern(key)
    for line in lines[start:end]:
        match = pattern.match(line)
        if match is None:
            continue
        if match.group("quote") is None:
            return None
        return match.group("value")
    return None


def _read_int_key(lines: list[str], start: int, end: int, key: str) -> Optional[int]:
    pattern = _key_pattern(key)
    for line in lines[start:end]:
        match = pattern.match(line)
        if match is None:
            continue
        raw = (match.group("value") or match.group("bare") or "").strip()
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _read_bool_key(lines: list[str], start: int, end: int, key: str) -> Optional[bool]:
    pattern = _key_pattern(key)
    for line in lines[start:end]:
        match = pattern.match(line)
        if match is None:
            continue
        raw = (match.group("value") or match.group("bare") or "").strip().lower()
        if raw == "true":
            return True
        if raw == "false":
            return False
        return None
    return None


def _set_string_key(lines: list[str], start: int, end: int, key: str, value: str) -> None:
    pattern = _key_pattern(key)
    for index in range(start, end):
        match = pattern.match(lines[index])
        if match is None:
            continue
        newline = "\n" if lines[index].endswith("\n") else ""
        suffix = match.group("suffix") or ""
        lines[index] = f'{match.group("prefix")}"{value}"{suffix}{newline}'
        return

    raise ConfigMigrationError(f"Could not find key {key!r} in [cloud] section")


def _set_or_insert_string_key(
    lines: list[str],
    start: int,
    end: int,
    key: str,
    value: str,
    *,
    insert_after: Optional[str],
) -> tuple[list[str], int, int]:
    existing_index = _find_key_index(lines, start, end, key)
    if existing_index is not None:
        _set_string_key(lines, start, end, key, value)
        return lines, start, end

    insert_index = start - 1 if insert_after is None else _find_key_index(lines, start, end, insert_after)
    if insert_index is None:
        insert_index = end - 1
    lines.insert(insert_index + 1, f'{key} = "{value}"\n')
    return lines, start, end + 1


def _set_or_insert_int_key(
    lines: list[str],
    start: int,
    end: int,
    key: str,
    value: int,
    *,
    insert_after: str,
) -> tuple[list[str], int, int]:
    pattern = _key_pattern(key)
    for index in range(start, end):
        match = pattern.match(lines[index])
        if match is None:
            continue
        newline = "\n" if lines[index].endswith("\n") else ""
        suffix = match.group("suffix") or ""
        lines[index] = f'{match.group("prefix")}{value}{suffix}{newline}'
        return lines, start, end

    insert_index = _find_key_index(lines, start, end, insert_after)
    if insert_index is None:
        insert_index = end - 1
    lines.insert(insert_index + 1, f"{key} = {value}\n")
    return lines, start, end + 1


def _set_or_insert_bool_key(
    lines: list[str],
    start: int,
    end: int,
    key: str,
    value: bool,
    *,
    insert_after: str,
) -> tuple[list[str], int, int]:
    pattern = _key_pattern(key)
    rendered = "true" if value else "false"
    for index in range(start, end):
        match = pattern.match(lines[index])
        if match is None:
            continue
        newline = "\n" if lines[index].endswith("\n") else ""
        suffix = match.group("suffix") or ""
        lines[index] = f'{match.group("prefix")}{rendered}{suffix}{newline}'
        return lines, start, end

    insert_index = _find_key_index(lines, start, end, insert_after)
    if insert_index is None:
        insert_index = end - 1
    lines.insert(insert_index + 1, f"{key} = {rendered}\n")
    return lines, start, end + 1


def _find_key_index(lines: list[str], start: int, end: int, key: str) -> Optional[int]:
    pattern = _key_pattern(key)
    for index in range(start, end):
        if pattern.match(lines[index]) is not None:
            return index
    return None


def _key_pattern(key: str) -> re.Pattern[str]:
    return re.compile(
        rf"^(?P<prefix>\s*{re.escape(key)}\s*=\s*)"
        r"(?:(?P<quote>[\"'])(?P<value>.*?)(?P=quote)|(?P<bare>[^#\r\n]*?))"
        r"(?P<suffix>\s*(?:#.*)?)\r?\n?$"
    )


def _normalize_url(value: str) -> str:
    return value.strip().rstrip("/")


def _split_host_port(value: str) -> tuple[str, Optional[int]]:
    host = value.strip()
    if ":" not in host:
        return host, None

    host_part, port_part = host.rsplit(":", 1)
    try:
        return host_part, int(port_part)
    except ValueError:
        return host, None


def _write_backup(path: Path, original: str) -> Path:
    backup_path = path.with_name(f"{path.name}.pre-mewcon-migration.bak")
    if not backup_path.exists():
        try:
            backup_path.write_text(original, encoding="utf-8")
            _preserve_owner_mode(backup_path, path)
        except OSError as exc:
            raise ConfigMigrationError(f"Could not write backup file {backup_path}: {exc}") from exc
    return backup_path


def _preserve_owner_mode(target: Path, source: Path) -> None:
    try:
        source_stat = source.stat()
    except OSError:
        return

    try:
        os.chmod(target, source_stat.st_mode & 0o777)
    except OSError:
        return

    try:
        shutil.copystat(source, target, follow_symlinks=True)
    except OSError:
        return