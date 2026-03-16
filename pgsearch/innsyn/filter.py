"""Filter for å utelukke tegninger, skisser og bilder fra indeksering."""

SKIP_KEYWORDS = [
    "tegning",
    "tegninger",
    "situasjonskart",
    "situasjonsplan",
    "fasade",
    "fasadetegning",
    "snitt",
    "snittegning",
    "plantegning",
    "perspektiv",
    "fotografi",
    "foto",
    "bilde",
    "bilder",
    "skisse",
    "kart",
]


def should_index(title: str) -> bool:
    """Return False if title indicates a drawing/sketch/photo."""
    if not title:
        return True
    title_lower = title.lower()
    return not any(keyword in title_lower for keyword in SKIP_KEYWORDS)
