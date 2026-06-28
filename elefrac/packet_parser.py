"""
Spellbreak join packet inspection, player name rewriting, and UID extraction.

Spellbreak (UE4) encodes the connecting player's name in the join URL, e.g.:
    /Game/Maps/Lobby?Name=TOKEN16CHAR?game=solos?...

The proxy finds this field, validates the value as an auth token, and
rewrites it with the player's registered username before forwarding to
the game server — which then sees the real name in APlayerState.

The rewrite pads or truncates the replacement to the same byte length as
the original so no packet bytes shift (UDP checksums remain valid).

UID extraction
--------------
Spellbreak encodes its join URL with each ASCII byte multiplied by 2.
The hardware UID follows the URL segment in the same binary blob:
    [ComputerName]-[32-hex-char-ID]
Both the 2x-encoded (vanilla) and plaintext (modded) variants are tried.
"""

import logging
import re

log = logging.getLogger(__name__)

_NAME_MARKERS: list[bytes] = [b'?Name=', b'?name=']

# "/Game/Maps/" with each ASCII byte doubled — the header that precedes
# the join URL in a vanilla Spellbreak UDP connection packet.
_ENCODED_HEADER = bytes([94, 142, 194, 218, 202, 94, 154, 194, 224, 230, 94])

# 2×-encoded `?Name=` — present in vanilla (unmodded) join packets.
_ENCODED_NAME_MARKER = bytes([0x7E, 0x9C, 0xC2, 0xDA, 0xCA, 0x7A])
# 2×-encoded `?` — URL option delimiter in encoded packets.
_ENCODED_DELIM = 0x7E

_UID_RE = re.compile(r'^(.+)-([A-Fa-f0-9]{32})$')


# ── Name field helpers ────────────────────────────────────────────────────────

def extract_name(data: bytes) -> tuple[str | None, int, int, bool]:
    """
    Locate the Name field in a join packet.

    Tries the 2×-encoded form first (vanilla Spellbreak and auth_injector
    clients both use it), then falls back to plaintext `?Name=`.
    Returns (name, start, end, encoded) where encoded=True means the name
    bytes in the packet are 2×-encoded and must be re-encoded on rewrite.
    Returns (None, -1, -1, False) when not found.
    """
    # Encoded path — vanilla Spellbreak and auth_injector (primary)
    idx = data.find(_ENCODED_NAME_MARKER)
    if idx != -1:
        start = idx + len(_ENCODED_NAME_MARKER)
        end = start
        while end < len(data):
            b = data[end]
            if b == _ENCODED_DELIM:
                break
            decoded = b >> 1
            if decoded < 0x20 or decoded > 0x7E:
                break
            end += 1
        if end > start:
            name = ''.join(chr(data[i] >> 1) for i in range(start, end))
            return name, start, end, True

    # Plaintext path (legacy / other modded clients)
    for marker in _NAME_MARKERS:
        idx = data.find(marker)
        if idx == -1:
            continue
        start = idx + len(marker)
        end = start
        while end < len(data):
            b = data[end]
            # 0x3F = plain '?', 0x7E = 2×-encoded '?' — both terminate the field
            if b == 0x3F or b == 0x7E or b < 0x20 or b > 0x7E:
                break
            end += 1
        if end > start:
            try:
                return data[start:end].decode('ascii'), start, end, False
            except UnicodeDecodeError:
                continue

    return None, -1, -1, False


def is_join_packet(data: bytes) -> bool:
    return any(m in data for m in _NAME_MARKERS) or _ENCODED_HEADER in data


def decode_join_url(data: bytes) -> str | None:
    """Decode the full 2x-encoded join URL for debug inspection. Returns None if not found."""
    idx = data.find(_ENCODED_HEADER)
    if idx == -1:
        return None
    segment = data[idx + len(_ENCODED_HEADER):]
    chars = []
    for b in segment:
        if b == 0:
            break
        c = b >> 1
        if 0x20 <= c <= 0x7E:
            chars.append(chr(c))
        else:
            break
    url = ''.join(chars)
    return ('/Game/Maps/' + url) if url else None


def rewrite_name(
    data: bytes,
    name_start: int,
    name_end: int,
    new_name: str,
    encoded: bool = False,
) -> bytes:
    """
    Replace the name field at data[name_start:name_end] with new_name.
    If encoded=True, new_name is written as 2×-encoded bytes to match the
    surrounding packet format; otherwise it is written as plain ASCII.
    Truncates or pads to preserve the original slot length.
    """
    slot_len = name_end - name_start
    new_b = new_name.encode('ascii')
    if encoded:
        new_b = bytes(b << 1 for b in new_b)
        fill = 0x02  # 2 × 0x01 — decoded value terminates name parsing
    else:
        fill = 0x20  # space
    if len(new_b) < slot_len:
        new_b = new_b + bytes([fill] * (slot_len - len(new_b)))
    elif len(new_b) > slot_len:
        new_b = new_b[:slot_len]
    return data[:name_start] + new_b + data[name_end:]


# ── UID-field auth token extraction ──────────────────────────────────────────

# Set of 2×-encoded lowercase hex byte values (0-9, a-f each doubled).
_2X_HEX_BYTES: frozenset = frozenset(
    c * 2 for c in b'0123456789abcdef'
)

def extract_auth_uid_token(data: bytes) -> str | None:
    """
    Extract a 32-char auth token injected into the hardware UID field.

    auth_injector writes 32 2×-encoded lowercase hex chars into the UID
    segment (replacing the entire ComputerName-hexsuffix).  The result is a
    null-terminated segment of exactly 32 bytes where every byte is a
    2×-encoded hex digit.

    Returns the decoded 32-char hex string, or None if not present.
    """
    idx = data.find(_ENCODED_HEADER)
    if idx == -1:
        return None
    contents = data[idx + len(_ENCODED_HEADER):]
    for seg in contents.split(b'\x00'):
        if len(seg) != 32:
            continue
        if all(b in _2X_HEX_BYTES for b in seg):
            return ''.join(chr(b >> 1) for b in seg)
    return None


# ── UID extraction ─────────────────────────────────────────────────────────────

def _decode_spellbreak(segment: bytes) -> str:
    """
    Decode a segment of Spellbreak's 2x-encoded binary: divide each byte by 2
    to recover the original ASCII character.  Stops at a null byte.
    """
    chars = []
    for b in segment:
        if b == 0:
            break
        c = b >> 1  # integer divide by 2
        if 0x20 <= c <= 0x7E:
            chars.append(chr(c))
    return ''.join(chars)


def extract_uid(data: bytes) -> str | None:
    """
    Extract the hardware UID from a Spellbreak UDP join packet.

    Tries two formats:
      1. Vanilla (2x-encoded): packet contains _ENCODED_HEADER; all strings
         in the binary blob are decoded by dividing each byte by 2.
      2. Modded / plaintext: UID may appear as plain ASCII anywhere in the
         packet, typically embedded between null bytes.

    Returns the UID string (e.g. 'DESKTOP-XYZ-<32 hex>') or None.
    """
    # --- Encoded (vanilla Spellbreak) format ---------------------------------
    idx = data.find(_ENCODED_HEADER)
    if idx != -1:
        contents = data[idx + len(_ENCODED_HEADER):]
        for segment in contents.split(b'\x00'):
            if len(segment) < 35:  # minimum: 1 char + '-' + 32 hex
                continue
            decoded = _decode_spellbreak(segment)
            if len(decoded) >= 35:
                m = _UID_RE.match(decoded)
                if m:
                    uid = f'{m.group(1)}-{m.group(2).upper()}'
                    log.debug('Extracted encoded UID: %s', uid)
                    return uid

    # --- Plaintext (modded server) format ------------------------------------
    for segment in data.split(b'\x00'):
        if len(segment) < 35:
            continue
        try:
            text = segment.decode('ascii', errors='strict')
        except UnicodeDecodeError:
            continue
        m = _UID_RE.match(text)
        if m:
            uid = f'{m.group(1)}-{m.group(2).upper()}'
            log.debug('Extracted plaintext UID: %s', uid)
            return uid

    return None
