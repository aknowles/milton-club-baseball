"""
Push notifications via ntfy.sh, shared by the scraper and issue processors.

Standard library only, so the issue workflows need no ``pip install``.
"""

import base64
import re
import urllib.error
import urllib.request

NTFY_URL = "https://ntfy.sh"
_TOPIC_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def encode_header(value):
    """Make *value* safe to send as an HTTP header.

    Line breaks are collapsed so user-supplied text can't inject extra
    headers. HTTP headers are latin-1 only, so anything else is sent as an
    RFC 2047 encoded word, which ntfy decodes.
    """
    value = " ".join(str(value).split())
    if value.isascii():
        return value
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return f"=?UTF-8?B?{encoded}?="


def send_ntfy(topic, title, message):
    """Send a push notification. Returns True on success.

    Never raises: a failed push must not fail the run that triggered it.
    """
    if not topic or not _TOPIC_RE.match(topic):
        print(f"  ntfy skipped: invalid topic {topic!r}")
        return False
    req = urllib.request.Request(
        f"{NTFY_URL}/{topic}",
        data=message.encode("utf-8"),
        headers={
            "Title": encode_header(title),
            "Priority": "default",
            "Tags": "baseball",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except urllib.error.HTTPError as e:
        print(f"  ntfy error ({e.code}) for {topic}")
        return False
    except Exception as e:
        print(f"  ntfy send failed for {topic}: {e}")
        return False
    print(f"  ntfy sent to {topic}: {title}")
    return True
