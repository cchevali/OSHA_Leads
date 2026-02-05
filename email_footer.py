from __future__ import annotations

from html import escape


def build_footer_text(
    brand_name: str,
    mailing_address: str,
    disclaimer: str,
    reply_to: str,
    unsub_url: str | None = None,
    include_separator: bool = True,
) -> str:
    lines = []
    if include_separator:
        lines.append("---")
    if brand_name:
        lines.append(brand_name)
    if mailing_address:
        lines.append(mailing_address)
    if disclaimer:
        lines.append(disclaimer)

    if unsub_url:
        lines.append('Opt out: reply with "unsubscribe" or click here to unsubscribe.')
        lines.append(unsub_url)
    else:
        lines.append(
            f'Opt out: reply with "unsubscribe" or email {reply_to} (subject: unsubscribe)'
        )

    return "\n".join(lines)


def build_footer_html(
    brand_name: str,
    mailing_address: str,
    disclaimer: str,
    reply_to: str,
    unsub_url: str | None = None,
) -> str:
    brand_html = escape(brand_name or "")
    address_html = escape(mailing_address or "")
    disclaimer_html = escape(disclaimer or "")
    reply_to_html = escape(reply_to or "")

    parts = [
        '<div style="margin-top: 24px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 12px; color: #666;">'
    ]
    if brand_html or address_html:
        parts.append(f"<p><strong>{brand_html}</strong><br>{address_html}</p>")
    if disclaimer_html:
        parts.append(f"<p>{disclaimer_html}</p>")

    if unsub_url:
        unsub_html = escape(unsub_url)
        parts.append(
            '  <p>Opt out: reply with "unsubscribe" or '
            f'<a href="{unsub_html}" style="color: #888;">click here to unsubscribe</a>.</p>'
        )
    else:
        parts.append(
            '  <p>Opt out: reply with "unsubscribe" or '
            f'<a href="mailto:{reply_to_html}?subject=unsubscribe" style="color: #888;">email {reply_to_html}</a>.</p>'
        )

    parts.append("</div>")
    return "\n".join(parts)
