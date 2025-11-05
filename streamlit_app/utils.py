from __future__ import annotations


def normalize_series(label: str) -> str:
    import re
    if not label:
        return "Sin producto"
    s = str(label).strip()
    rules: list[tuple[str, str]] = [
        (r"(?i)ebook\s+anal[íi]t[íi]c[ao]s?\s+esencial", "Ebook Analíticas Esenciales"),
        (r"(?i)keto[-\s]?fast", "Keto Fast"),
        (r"(?i)keto\s+optim", "Keto Optimizado"),
        (r"(?i)membres[íi]a.*mensual", "Membresía Intergaláctica Mensual"),
        (r"(?i)membres[íi]a.*anual", "Membresía Intergaláctica Anual"),
        (r"(?i)membres[íi]a.*trimestral", "Membresía Intergaláctica Trimestral"),
        (r"(?i)membres[íi]a.*semestral", "Membresía Intergaláctica Semestral"),
        (r"(?i)curso\s+de\s+magnesio", "Curso de Magnesio"),
        (r"(?i)phit\s*x\s*her", "PHIT x HER"),
        (r"(?i)phit\s*x\s*him", "PHIT x HIM"),
        (r"(?i)phit\s*vip", "PHIT VIP"),
    ]
    for pattern, canon in rules:
        if re.search(pattern, s):
            return canon
    return s.strip()


def build_color_map(labels: list[str]) -> dict[str, str]:
    import colorsys
    uniq = list(dict.fromkeys([l or "Sin producto" for l in labels]))
    n = max(len(uniq), 1)
    color_map: dict[str, str] = {}
    for i, lab in enumerate(uniq):
        h = (i / n) % 1.0
        s = 0.65
        l = 0.5
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        color_map[lab] = f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"
    return color_map


def date_trunc_alias(grain: str) -> str:
    """Devuelve expresión SQL para truncar fechas por granularidad"""
    grain_lower = grain.lower() if grain else "day"
    # Mapeo de grains comunes a expresiones DATE_TRUNC de PostgreSQL
    grain_map = {
        "day": "DATE_TRUNC('day', p.paid_at)",
        "día": "DATE_TRUNC('day', p.paid_at)",
        "d": "DATE_TRUNC('day', p.paid_at)",
        "week": "DATE_TRUNC('week', p.paid_at)",
        "semana": "DATE_TRUNC('week', p.paid_at)",
        "w": "DATE_TRUNC('week', p.paid_at)",
        "month": "DATE_TRUNC('month', p.paid_at)",
        "mes": "DATE_TRUNC('month', p.paid_at)",
        "m": "DATE_TRUNC('month', p.paid_at)",
        "auto": "DATE_TRUNC('day', p.paid_at)",  # Por defecto día
    }
    return grain_map.get(grain_lower, "DATE_TRUNC('day', p.paid_at)")




