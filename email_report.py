"""Alert emails: the alarms a user subscribed to, grouped by event, the most severe first.

Each email is sent as HTML, laid out with tables and inline styles because that is what email
clients render, and as plain text for clients that show no HTML.
"""

import html
import re
import time
from urllib.parse import urlparse

# the web frontend's colours, light and dark
PAGE, SURFACE, BORDER = '#f7f7f8', '#ffffff', '#e6e6eb'
TEXT, SECONDARY, MUTED = '#18181d', '#46464f', '#74747f'
ACCENT, ACCENT_SOFT, ACCENT_TEXT = '#5b5bd6', '#eeeefc', '#4040b0'
DANGER, DANGER_SOFT = '#c92a2a', '#fdeeee'
WARNING, WARNING_SOFT = '#9a5b00', '#fdf3e1'
OTHER, OTHER_SOFT = '#74747f', '#f0f0f3'
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

DARK_STYLE = """
@media (prefers-color-scheme: dark) {
  .page { background: #0e0e11 !important; }
  .card { background: #16161a !important; border-color: #2a2a31 !important; }
  .rule { border-color: #2a2a31 !important; }
  .text { color: #ececf1 !important; }
  .secondary { color: #c4c4cc !important; }
  .muted { color: #8f8f9c !important; }
  .chip { background: rgba(141, 141, 242, 0.14) !important; color: #c3c3ff !important; }
  .link { color: #c3c3ff !important; }
  .button { background: #8d8df2 !important; }
  .button a { color: #0e0e11 !important; }
  .level-danger { background: rgba(255, 123, 114, 0.12) !important; color: #ff7b72 !important; }
  .level-warning { background: rgba(227, 179, 65, 0.14) !important; color: #e3b341 !important; }
  .level-other { background: #212127 !important; color: #8f8f9c !important; }
}
@media (max-width: 600px) {
  .pad { padding-left: 18px !important; padding-right: 18px !important; }
  .title { font-size: 22px !important; }
}
"""

# severity levels, most severe first; unknown levels come after these, alarms without one last
LEVEL_ORDER = ['critical', 'major', 'high', 'error', 'minor', 'medium', 'warning', 'low', 'info']
SEVERE = {'critical', 'major', 'high', 'error'}
LEVEL_STYLES = {  # class, text, background
    **{level: ('level-danger', DANGER, DANGER_SOFT) for level in SEVERE},
    **{level: ('level-warning', WARNING, WARNING_SOFT) for level in ['minor', 'medium', 'warning']},
}

# Gmail cuts off emails longer than about 100 kB, so the HTML shows up to 10 alarms per event
# and about 50 in all (but at least 3 per event); the text part has all of them
MAX_ROWS_PER_EVENT = 10
MAX_ROWS = 50
MIN_ROWS_PER_EVENT = 3
MAX_TAGS = 4
# links in alarm templates are named after the site they lead to
LINK_NAMES = {
    'ps-dash.uc.ssl-hep.org': 'pS-Dash',
    'ps-triage.atlas-ml.org': 'perfSONAR triage',
    'atlas-kibana.mwt2.org': 'Kibana',
}
URL = re.compile(r'https?://[^\s<>"]+')


# ---- alarms ---------------------------------------------------------------------------

def show(value):
    """A source value as text: lists comma-separated, mappings as 'key: value'."""
    if isinstance(value, (list, tuple)):
        return ', '.join(show(v) for v in value)
    if isinstance(value, dict):
        return '; '.join(f'{k}: {show(v)}' for k, v in value.items())
    return str(value)


def fill_template(template, source):
    """The event's template filled in from the alarm's source: %{name} is the value,
    c{name} its length and p{name} a fraction shown as a percentage. A placeholder that doesn't
    fit its value (c{} of a number, p{} of a text) is left as it is."""
    text = template
    for name, value in source.items():
        text = text.replace('%{' + name + '}', show(value))
        if 'c{' + name + '}' in text and hasattr(value, '__len__'):
            text = text.replace('c{' + name + '}', str(len(value)))
        if 'p{' + name + '}' in text and isinstance(value, (int, float)) and not isinstance(value, bool):
            text = text.replace('p{' + name + '}', str(round(value * 100, 3)) + '%')
    return text


def alarm_entry(alarm, template):
    """What an email shows of one alarm (as returned by /alarm/fetch)."""
    tags = alarm.get('tags') or []
    if isinstance(tags, str):
        tags = [tags]
    source = alarm.get('source')
    level = alarm.get('level')
    return {
        'category': alarm['category'],
        'subcategory': alarm['subcategory'],
        'event': alarm['event'],
        'time': alarm['created_at'] / 1000,
        'tags': [str(t) for t in tags if str(t).strip()],
        'level': str(level) if level not in (None, '') else None,
        'body': str(alarm.get('body') or '').strip(),
        'details': fill_template(template, source).strip() if template and isinstance(source, dict) else '',
    }


def lines(text):
    """`text` as trimmed, non-empty lines; templates use \\n and \\t for layout."""
    return [' '.join(line.split()) for line in text.split('\n') if line.strip()]


def describe(entry):
    """(headline, details) of an alarm: its body, then the filled template; without the body
    when it only repeats the event's name or what the template says."""
    def flat(s):
        return ' '.join(s.split()).lower()
    body = entry['body']
    if flat(body) == flat(entry['event']) or (entry['details'] and flat(body) in flat(entry['details'])):
        body = ''
    return body, entry['details']


def level_rank(level):
    if level is None:
        return len(LEVEL_ORDER) + 1
    return LEVEL_ORDER.index(level.lower()) if level.lower() in LEVEL_ORDER else len(LEVEL_ORDER)


def group_by_event(entries):
    """[{category, subcategory, event, alarms, rows}]: the most severe events first, then the
    busiest. `rows` are the event's alarms to show, the most severe first, then the newest;
    an alarm raised again with the same text is one row, its `repeats` counting how often."""
    groups = {}
    for e in entries:
        groups.setdefault((e['category'], e['subcategory'], e['event']), []).append(e)
    out = []
    for (category, subcategory, event), alarms in groups.items():
        alarms.sort(key=lambda a: (level_rank(a['level']), -a['time']))
        rows = {}
        for a in alarms:
            key = (a['level'], tuple(a['tags']), a['body'], a['details'])
            if key in rows:
                rows[key]['repeats'] += 1
            else:
                rows[key] = {**a, 'repeats': 1}
        out.append({'category': category, 'subcategory': subcategory, 'event': event,
                    'alarms': alarms, 'rows': list(rows.values())})
    out.sort(key=lambda g: (level_rank(g['alarms'][0]['level']), -len(g['alarms']), g['event'].lower()))
    return out


def level_counts(entries):
    """[(level, count)] of the alarms that have a level, the most severe first."""
    counts = {}
    for e in entries:
        if e['level']:
            counts[e['level']] = counts.get(e['level'], 0) + 1
    return sorted(counts.items(), key=lambda kv: (level_rank(kv[0]), kv[0]))


def plural(n, word, words=None):
    return f'{n} {word if n == 1 else (words or word + "s")}'


def period(hours):
    return 'hour' if hours <= 1 else f'{hours} hours'


def when(seconds, now):
    """An alarm's time in UTC, with the day when it isn't today."""
    t = time.gmtime(seconds)
    if time.strftime('%Y%m%d', t) == time.strftime('%Y%m%d', time.gmtime(now)):
        return time.strftime('%H:%M UTC', t)
    return time.strftime('%d %b %H:%M UTC', t)


def row_time(row, now):
    """e.g. '11:10 UTC', or '11:10 UTC · ×3' for an alarm raised three times (the last at 11:10)."""
    return when(row['time'], now) + (f' · ×{row["repeats"]}' if row['repeats'] > 1 else '')


def window(hours, now):
    """The time span an email covers, e.g. '02 Nov 2026 · 10:00–11:00 UTC'."""
    start, end = time.gmtime(now - hours * 3600), time.gmtime(now)
    if time.strftime('%Y%m%d', start) == time.strftime('%Y%m%d', end):
        return f"{time.strftime('%d %b %Y', end)} · {time.strftime('%H:%M', start)}–{time.strftime('%H:%M', end)} UTC"
    return f"{time.strftime('%d %b %H:%M', start)} – {time.strftime('%d %b %H:%M', end)} UTC"


def subject(entries):
    """e.g. 'Alarm & Alert · 12 alarms (3 major): path performance degraded and 2 more events'"""
    groups = group_by_event(entries)
    text = plural(len(entries), 'alarm')
    severe = [f'{n} {level}' for level, n in level_counts(entries) if level.lower() in SEVERE]
    if severe:
        text += f' ({", ".join(severe)})'
    text += f': {groups[0]["event"]}'
    if len(groups) > 1:
        text += f' and {plural(len(groups) - 1, "more event")}'
    return f'Alarm & Alert · {text}'


def summary(entries, hours):
    """e.g. '12 new alarms in 3 subscribed events over the past hour'"""
    groups = group_by_event(entries)
    return (f'{plural(len(entries), "new alarm")} in {plural(len(groups), "subscribed event")} '
            f'over the past {period(hours)}')


# ---- plain text -------------------------------------------------------------------------

def render_text(name, entries, hours, service, now=None):
    now = now or time.time()
    service = service.rstrip('/')
    counts = ', '.join(f'{n} {level}' for level, n in level_counts(entries))
    out = [f'Hello {name},', '', summary(entries, hours) + (f' ({counts}).' if counts else '.'), '']
    for g in group_by_event(entries):
        heading = f'{g["event"]} · {g["category"]} / {g["subcategory"]} · {plural(len(g["alarms"]), "alarm")}'
        out += [heading, '=' * len(heading), '']
        for a in g['rows']:
            out.append(' · '.join(x for x in [a['level'], row_time(a, now), ', '.join(a['tags'])] if x))
            headline, details = describe(a)
            out += ['    ' + line for line in lines(headline) + lines(details)]
            out.append('')
    out += [
        f'Manage your subscriptions or mute an issue: {service}/subscriptions',
        f'Email preferences and vacation mode: {service}/profile',
        '',
        'ATLAS Alarm & Alert Service',
    ]
    return '\n'.join(out)


# ---- HTML ---------------------------------------------------------------------------------

def esc(text):
    return html.escape(str(text), quote=True)


def link(url, label):
    return (f'<a class="link" href="{esc(url)}" style="color:{ACCENT_TEXT};font-weight:600;'
            f'text-decoration:none;">{esc(label)}</a>')


def rich(text):
    """`text` as HTML: its line breaks kept and each link named after the site it leads to."""
    out = []
    for line in lines(text):
        parts, last = [], 0
        for m in URL.finditer(line):
            url = m.group(0).rstrip('.,;:)')
            host = urlparse(url).hostname or url
            parts += [esc(line[last:m.start()]), link(url, f'Open in {LINK_NAMES.get(host, host)} →')]
            last = m.start() + len(url)
        parts.append(esc(line[last:]))
        out.append(''.join(parts))
    return '<br>'.join(out)


def pill(text, cls, colour, background):
    return (f'<span class="{cls}" style="display:inline-block;margin:2px 4px 2px 0;padding:2px 8px;'
            f'border-radius:999px;background:{background};color:{colour};font-size:12px;'
            f'font-weight:600;line-height:18px;white-space:nowrap;">{esc(text)}</span>')


def level_pill(level, text=None):
    return pill(text or level, *LEVEL_STYLES.get(level.lower(), ('level-other', OTHER, OTHER_SOFT)))


def tag_pills(tags):
    shown = ''.join(pill(t, 'chip', ACCENT_TEXT, ACCENT_SOFT) for t in tags[:MAX_TAGS])
    if len(tags) > MAX_TAGS:
        shown += pill(f'+{len(tags) - MAX_TAGS}', 'level-other', OTHER, OTHER_SOFT)
    return shown


def card(rows):
    return (f'<tr><td class="card" style="background:{SURFACE};border:1px solid {BORDER};border-radius:12px;">'
            f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">{rows}</table>'
            f'</td></tr><tr><td style="height:16px;line-height:16px;font-size:0;">&nbsp;</td></tr>')


def alarm_row(a, now):
    headline, details = describe(a)
    pills = (level_pill(a['level']) if a['level'] else '') + tag_pills(a['tags'])
    text = ''
    if headline:
        text += (f'<div class="text" style="margin-top:2px;color:{TEXT};font-size:14px;line-height:21px;'
                 f'font-weight:600;">{rich(headline)}</div>')
    if details:
        colour, cls = (SECONDARY, 'secondary') if headline else (TEXT, 'text')
        text += (f'<div class="{cls}" style="margin-top:4px;color:{colour};font-size:14px;'
                 f'line-height:21px;">{rich(details)}</div>')
    return (
        f'<tr><td class="pad rule" style="padding:14px 28px;border-top:1px solid {BORDER};">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td style="vertical-align:top;">{pills}</td>'
        f'<td class="muted" align="right" style="vertical-align:top;padding:5px 0 0 12px;color:{MUTED};'
        f'font-size:12px;line-height:18px;white-space:nowrap;">{esc(row_time(a, now))}</td>'
        f'</tr></table>{text}</td></tr>'
    )


def event_card(g, now, service, max_rows):
    alarms = g['alarms']
    rows = (
        f'<tr><td class="pad" style="padding:20px 28px 14px;">'
        f'<div class="muted" style="color:{MUTED};font-size:11px;line-height:16px;font-weight:600;'
        f'letter-spacing:0.06em;text-transform:uppercase;">{esc(g["category"])} › {esc(g["subcategory"])}</div>'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td class="text" style="padding-top:2px;color:{TEXT};font-size:18px;line-height:25px;font-weight:700;">'
        f'{esc(g["event"])}</td>'
        f'<td class="muted" align="right" style="padding:4px 0 0 12px;color:{MUTED};font-size:13px;'
        f'white-space:nowrap;">{esc(plural(len(alarms), "alarm"))}</td>'
        '</tr></table></td></tr>'
    )
    rows += ''.join(alarm_row(a, now) for a in g['rows'][:max_rows])
    if len(g['rows']) > max_rows:
        rest = sum(a['repeats'] for a in g['rows'][max_rows:])
        rows += (f'<tr><td class="pad rule muted" style="padding:12px 28px 16px;border-top:1px solid {BORDER};'
                 f'color:{MUTED};font-size:13px;">{esc(plural(rest, "more alarm"))} not shown here · '
                 f'{link(service + "/subscriptions", "See ongoing issues →")}</td></tr>')
    return card(rows)


def overview(groups):
    """One line per event: its name, where it sits, how many alarms and the worst level."""
    rows = ''
    for i, g in enumerate(groups):
        border = '' if i == 0 else f'border-top:1px solid {BORDER};'
        worst = g['alarms'][0]['level']
        rows += (
            f'<tr><td class="rule" style="padding:9px 0;{border}">'
            f'<div class="text" style="color:{TEXT};font-size:14px;line-height:20px;font-weight:600;">{esc(g["event"])}</div>'
            f'<div class="muted" style="color:{MUTED};font-size:12px;line-height:17px;">{esc(g["category"])} › '
            f'{esc(g["subcategory"])}</div></td>'
            f'<td class="rule" align="right" style="padding:9px 0 9px 12px;{border}white-space:nowrap;vertical-align:middle;">'
            f'{level_pill(worst) if worst else ""}'
            f'<span class="text" style="display:inline-block;min-width:22px;color:{TEXT};font-size:14px;'
            f'font-weight:600;">{len(g["alarms"])}</span></td></tr>'
        )
    return (f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="margin-top:20px;">{rows}</table>')


def render_html(name, entries, hours, service, now=None):
    now = now or time.time()
    service = service.rstrip('/')
    groups = group_by_event(entries)
    max_rows = max(MIN_ROWS_PER_EVENT, min(MAX_ROWS_PER_EVENT, MAX_ROWS // len(groups)))
    counts = ''.join(level_pill(level, f'{n} {level}') for level, n in level_counts(entries))
    heading = plural(len(entries), 'new alarm')
    preheader = summary(entries, hours) + '.'

    intro = (
        f'<tr><td class="pad" style="padding:26px 28px;">'
        f'<div class="secondary" style="color:{SECONDARY};font-size:14px;line-height:20px;">Hello {esc(name)},</div>'
        f'<div class="title text" style="margin-top:6px;color:{TEXT};font-size:26px;line-height:32px;'
        f'font-weight:700;">{esc(heading)}</div>'
        f'<div class="muted" style="margin-top:4px;color:{MUTED};font-size:14px;line-height:20px;">'
        f'in {esc(plural(len(groups), "subscribed event"))} over the past {esc(period(hours))}</div>'
        + (f'<div style="margin-top:14px;">{counts}</div>' if counts else '')
        + (overview(groups) if len(groups) > 1 else '')
        + '</td></tr>'
    )
    button = (
        '<table role="presentation" cellpadding="0" cellspacing="0" border="0" align="center"><tr>'
        f'<td class="button" style="background:{ACCENT};border-radius:8px;">'
        f'<a href="{esc(service)}/subscriptions" style="display:inline-block;padding:10px 18px;color:#ffffff;'
        'font-size:14px;font-weight:600;text-decoration:none;">Manage subscriptions</a></td></tr></table>'
    )
    footer = (
        '<tr><td style="padding:8px 4px 0;text-align:center;">' + button
        + f'<div class="muted" style="margin-top:16px;color:{MUTED};font-size:13px;line-height:20px;">'
        + link(service + '/subscriptions', 'Mute an issue') + ' &nbsp;·&nbsp; '
        + link(service + '/profile', 'Email preferences') + ' &nbsp;·&nbsp; '
        + link(service + '/profile', 'Vacation mode') + '</div>'
        f'<div class="muted" style="margin-top:12px;color:{MUTED};font-size:12px;line-height:18px;">'
        f'You get this email because you subscribed to these alarms on the ATLAS Alarm &amp; Alert Service.'
        f'<br>Alarms are collected every {esc(period(hours))}; times are in UTC.</div></td></tr>'
    )
    brand = (
        '<tr><td style="padding:0 4px 14px;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td class="text" style="color:{TEXT};font-size:15px;font-weight:700;">'
        f'<span style="display:inline-block;width:10px;height:10px;margin-right:6px;border-radius:5px;'
        f'background:{ACCENT};"></span>Alarm &amp; Alert</td>'
        f'<td class="muted" align="right" style="color:{MUTED};font-size:12px;">{esc(window(hours, now))}</td>'
        '</tr></table></td></tr>'
    )
    return (
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta name="color-scheme" content="light dark"><meta name="supported-color-schemes" content="light dark">'
        f'<title>{esc(subject(entries))}</title><style>{DARK_STYLE}</style>'
        # Outlook on Windows ignores max-width and doesn't pass fonts down into tables
        '<!--[if mso]><style>table, td, div, span, a { font-family: Arial, sans-serif !important; }</style><![endif]-->'
        '</head>'
        f'<body class="page" style="margin:0;padding:0;background:{PAGE};">'
        f'<div style="display:none;max-height:0;overflow:hidden;opacity:0;">{esc(preheader)}</div>'
        f'<table role="presentation" class="page" width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="background:{PAGE};font-family:{FONT};"><tr><td align="center" style="padding:24px 10px;">'
        '<!--[if mso]><table role="presentation" width="640" align="center" cellpadding="0" cellspacing="0" border="0"><tr><td><![endif]-->'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;">'
        + brand + card(intro) + ''.join(event_card(g, now, service, max_rows) for g in groups) + footer
        + '</table><!--[if mso]></td></tr></table><![endif]--></td></tr></table></body></html>'
    )
