import asyncio
import re
import smtplib
import ssl
from email.message import EmailMessage
from html import escape
from typing import Any


def _as_bool(value: Any, default: bool = False) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _is_valid_email(value: str) -> bool:
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", (value or "").strip(), re.I))


def _smtp_config(site: Any) -> dict[str, Any]:
    if not site:
        return {}
    return {
        "enabled": _as_bool(getattr(site, "smtp_enabled", False), False),
        "host": _clean(getattr(site, "smtp_host", "")),
        "port": int(getattr(site, "smtp_port", 587) or 587),
        "username": _clean(getattr(site, "smtp_username", "")),
        "password": _clean(getattr(site, "smtp_password", "")),
        "from_email": _clean(getattr(site, "smtp_from_email", "")),
        "from_name": _clean(getattr(site, "smtp_from_name", "")) or "MysticMovies",
        "use_tls": _as_bool(getattr(site, "smtp_use_tls", True), True),
        "use_ssl": _as_bool(getattr(site, "smtp_use_ssl", False), False),
    }


def build_email_html(
    *,
    subject: str,
    preheader: str,
    greeting: str,
    body_lines: list[str],
    cta_text: str = "",
    cta_url: str = "",
    footer_note: str = "",
    brand_name: str = "MysticMovies",
    accent: str = "#facc15",
) -> str:
    safe_subject = escape(subject)
    safe_preheader = escape(preheader)
    safe_greeting = escape(greeting)
    safe_brand = escape(brand_name)
    safe_accent = escape(accent or "#facc15")
    safe_footer = escape(footer_note or "This is an automated email from MysticMovies.")
    line_html = "".join(f"<p style='margin:0 0 12px 0;color:#d1d5db;font-size:15px;line-height:1.6'>{escape(line)}</p>" for line in (body_lines or []))
    cta_html = ""
    if cta_text and cta_url:
        cta_html = (
            "<div style='margin-top:18px'>"
            f"<a href='{escape(cta_url)}' style='display:inline-block;background:{safe_accent};color:#0b0f16;"
            "text-decoration:none;font-weight:700;font-size:14px;padding:12px 18px;border-radius:10px'>"
            f"{escape(cta_text)}</a></div>"
        )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{safe_subject}</title>
</head>
<body style="margin:0;padding:0;background:#070b12;font-family:Segoe UI,Roboto,Arial,sans-serif">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent">{safe_preheader}</div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#070b12;padding:24px 10px">
    <tr>
      <td align="center">
        <table role="presentation" width="640" cellpadding="0" cellspacing="0" style="max-width:640px;width:100%;border:1px solid #1f2937;border-radius:14px;overflow:hidden;background:#0b1220">
          <tr>
            <td style="background:linear-gradient(120deg,#111827,#0b1220);padding:18px 22px;border-bottom:1px solid #1f2937">
              <div style="font-size:12px;letter-spacing:.14em;color:{safe_accent};font-weight:700">MYSTICMOVIES</div>
              <div style="margin-top:8px;color:#fff;font-size:22px;font-weight:800">{safe_subject}</div>
            </td>
          </tr>
          <tr>
            <td style="padding:22px">
              <p style="margin:0 0 14px 0;color:#fff;font-size:16px;font-weight:700">{safe_greeting}</p>
              {line_html}
              {cta_html}
            </td>
          </tr>
          <tr>
            <td style="padding:14px 22px;border-top:1px solid #1f2937;color:#94a3b8;font-size:12px">
              {safe_footer}<br>
              Powered by {safe_brand}
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


async def send_email_via_site_settings(
    *,
    site: Any,
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str = "",
) -> tuple[bool, str]:
    cfg = _smtp_config(site)
    if not cfg or not cfg.get("enabled"):
        return False, "SMTP disabled"

    to_email = _clean(to_email).lower()
    if not _is_valid_email(to_email):
        return False, "Invalid recipient email"

    host = _clean(cfg.get("host"))
    port = int(cfg.get("port") or 0)
    from_email = _clean(cfg.get("from_email"))
    if not host or port <= 0 or not from_email or not _is_valid_email(from_email):
        return False, "SMTP config incomplete"

    username = _clean(cfg.get("username"))
    password = _clean(cfg.get("password"))
    use_tls = bool(cfg.get("use_tls"))
    use_ssl = bool(cfg.get("use_ssl"))
    from_name = _clean(cfg.get("from_name")) or "MysticMovies"

    message = EmailMessage()
    message["Subject"] = _clean(subject) or "MysticMovies Notification"
    message["From"] = f"{from_name} <{from_email}>"
    message["To"] = to_email
    fallback_text = _clean(text_body) or "Please view this email in HTML mode."
    message.set_content(fallback_text)
    message.add_alternative(html_body or f"<p>{escape(fallback_text)}</p>", subtype="html")

    def _send_sync() -> None:
        context = ssl.create_default_context()
        if use_ssl:
            with smtplib.SMTP_SSL(host=host, port=port, timeout=25, context=context) as smtp:
                smtp.ehlo()
                if username:
                    smtp.login(username, password)
                smtp.send_message(message)
            return
        with smtplib.SMTP(host=host, port=port, timeout=25) as smtp:
            smtp.ehlo()
            if use_tls:
                smtp.starttls(context=context)
                smtp.ehlo()
            if username:
                smtp.login(username, password)
            smtp.send_message(message)

    try:
        await asyncio.to_thread(_send_sync)
        return True, "sent"
    except Exception as exc:
        return False, str(exc)

