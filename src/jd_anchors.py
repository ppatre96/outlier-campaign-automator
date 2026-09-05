"""JD-anchored feature gating for Stage A.

Stage A is a behavioural miner: it searches the binary résumé-feature space for
whatever best predicts screening pass among the people on a project. That is
the right question ONLY if the population is on-brief. When it isn't, Stage A
faithfully reports the strongest signal in the room — and for a project staffed
by existing CBs that signal is "these people can code", regardless of what the
project actually wants (GMR-0029: a graphic-design / audio / video / animation
ramp mined `kubernetes + python`, `objective-c + swift`, `graphql + java`).

The requirement is the holy grail. The job post + the Smart Ramp cohort say who
is wanted; the behavioural layer's job is only to find which of THOSE signals
are common among the CBs who passed screening. So we gate the feature space
Stage A may mine down to JD-relevant columns first, and let Stage A rank within
it.

Worked example (the user's): requirement is "graphic designer". Among activated
CBs, `skills__python` and `skills__graphic_design_internship` are both common.
`python` shares no token with the requirement and is dropped; the internship
column shares "graphic"/"design" and survives — so Stage A picks the design
internship, which is the signal that actually relates to this project.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Iterable

log = logging.getLogger(__name__)

# Facet prefixes carry no meaning for relevance — "skills__python" is about
# python, not about skills.
_FACET_PREFIXES = (
    "skills__", "job_titles_norm__", "fields_of_study__",
    "highest_degree_level__", "accreditations_norm__", "experience_band__",
)

# Recruiting boilerplate that appears in nearly every brief and would re-open
# the gate to everything. These are dropped from the anchor vocabulary — never
# from the columns themselves.
_STOPWORDS = {
    "the", "and", "for", "with", "from", "have", "has", "their", "they", "this",
    "that", "want", "wants", "those", "looking", "who", "are", "you", "your",
    "professional", "professionals", "experienced", "experience", "expert",
    "experts", "senior", "junior", "mid", "level", "years", "year", "strong",
    "good", "great", "excellent", "skills", "skill", "ability", "able",
    "knowledge", "background", "work", "working", "worked", "candidate",
    "candidates", "contributor", "contributors", "people", "person", "quality",
    "high", "must", "should", "would", "can", "will", "new", "current",
    "submit", "submitting", "submission", "submissions", "collection",
    "collect", "activations", "activation", "target", "targets", "project",
    "projects", "task", "tasks", "opportunity", "remote", "global", "each",
    "per", "plus", "when", "into", "over", "under", "about", "also",
}

# Fuzzy-equality floor for two tokens to count as the same concept.
_TOKEN_MATCH_THRESHOLD = 88

# Facets that describe seniority / education LEVEL rather than domain. A
# requirement can't be "off-topic" about them the way a skill can — "5-10
# years" is equally meaningful for a designer and an engineer — and they carry
# no lexical overlap with any requirement, so gating on tokens would drop every
# one of them. Exempt: Stage A stays free to mine them.
_REQUIREMENT_NEUTRAL_PREFIXES = ("experience_band__", "highest_degree_level__")


def _tokenize(text: str) -> set[str]:
    """Lowercase alphanumeric tokens of length >= 3, minus boilerplate."""
    if not text:
        return set()
    raw = re.split(r"[^a-z0-9+#]+", str(text).lower())
    return {t for t in raw if len(t) >= 3 and t not in _STOPWORDS}


def column_tokens(col: str) -> set[str]:
    """Tokens of a binary feature column, ignoring its facet prefix."""
    suffix = col
    for prefix in _FACET_PREFIXES:
        if col.startswith(prefix):
            suffix = col[len(prefix):]
            break
    else:
        suffix = col.split("__", 1)[-1]
    return _tokenize(suffix.replace("_", " "))


def build_jd_anchors(
    *,
    derived_icp: dict | None = None,
    matched_domain: str = "",
    cohort_description: str = "",
    job_post_meta: dict | None = None,
) -> set[str]:
    """The vocabulary that defines "relevant to this requirement".

    Sourced from the authoritative statements of intent only — the LLM's read
    of the job post, the Smart Ramp cohort's matched domain, and the cohort
    description. The ramp-level summary is deliberately NOT included: it is
    long free text and its tokens re-open the gate to everything (the existing
    Stage 1 brief filter takes that route and keeps ~73% of the pool).
    """
    icp = derived_icp or {}
    jp = job_post_meta or {}
    parts: list[str] = []
    parts.extend(str(s) for s in (icp.get("required_skills") or []))
    parts.append(str(icp.get("derived_tg_label") or ""))
    parts.append(str(icp.get("domain") or ""))
    parts.append(str(matched_domain or ""))
    parts.append(str(cohort_description or ""))
    # The job post's own domain/name — the JD in its most compressed form.
    parts.append(str(jp.get("domain") or ""))
    parts.append(str(jp.get("job_name") or ""))

    anchors: set[str] = set()
    for p in parts:
        anchors |= _tokenize(p)
    return anchors


def _same_stem(a: str, b: str) -> bool:
    """Cheap stemmer: one token is a prefix of the other, on >= 4 shared chars.

    design/designer/designs/designing, animate/animation, video/videographer.
    The 4-char floor keeps "art" from swallowing "artificial".
    """
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    return len(short) >= 4 and long.startswith(short)


def _matches(col_toks: set[str], anchors: set[str]) -> bool:
    if col_toks & anchors:
        return True
    try:
        from rapidfuzz import fuzz
    except ImportError:  # pragma: no cover - rapidfuzz is a hard dependency
        fuzz = None
    for ct in col_toks:
        for a in anchors:
            if _same_stem(ct, a):
                return True
            # Fuzzy pass for the variants prefixes miss (e.g. typo-ish forms).
            # Cheap length guard before the (relatively) expensive ratio.
            if fuzz is None or abs(len(ct) - len(a)) > 4:
                continue
            if fuzz.ratio(ct, a) >= _TOKEN_MATCH_THRESHOLD:
                return True
    return False


def jd_relevant_columns(
    bin_cols: Iterable[str],
    anchors: set[str],
    *,
    always_keep: Iterable[str] = (),
) -> tuple[list[str], list[str]]:
    """Split `bin_cols` into (relevant, dropped) against the JD anchors.

    `always_keep` (the ICP's own base-role / required-skill anchor columns) is
    never dropped — those columns ARE the requirement, and the ICP-fallback
    path downstream depends on them existing in the mined space.
    """
    cols = list(bin_cols)
    keep_set = {c for c in always_keep if c}
    if not anchors:
        return cols, []
    relevant, dropped = [], []
    for col in cols:
        if (
            col in keep_set
            or col.startswith(_REQUIREMENT_NEUTRAL_PREFIXES)
            or _matches(column_tokens(col), anchors)
        ):
            relevant.append(col)
        else:
            dropped.append(col)
    return relevant, dropped


def gate_features_to_jd(
    bin_cols: Iterable[str],
    *,
    derived_icp: dict | None = None,
    matched_domain: str = "",
    cohort_description: str = "",
    job_post_meta: dict | None = None,
    always_keep: Iterable[str] = (),
) -> list[str]:
    """Narrow Stage A's feature space to what the requirement asks for.

    Degrades to the full space (with a loud log) when the requirement yields no
    usable anchors, or when nothing at all matches — starving Stage A of every
    column would just push the ramp into the cold-start path without telling
    anyone why.
    """
    cols = list(bin_cols)
    anchors = build_jd_anchors(
        derived_icp=derived_icp,
        matched_domain=matched_domain,
        cohort_description=cohort_description,
        job_post_meta=job_post_meta,
    )
    if not anchors:
        log.warning(
            "jd_anchors: no usable anchors from the job post / matched_domain / "
            "cohort description — Stage A keeps all %d features (behavioural "
            "mining is UNCONSTRAINED; cohorts may be off-requirement)", len(cols),
        )
        return cols

    relevant, dropped = jd_relevant_columns(cols, anchors, always_keep=always_keep)
    if not relevant:
        log.warning(
            "jd_anchors: anchors=%s matched NONE of the %d features — keeping all "
            "(the screening pool has no overlap with the requirement; expect "
            "off-requirement cohorts or a cold start)",
            sorted(anchors)[:12], len(cols),
        )
        return cols

    log.info(
        "jd_anchors: Stage A feature space gated to the requirement — %d/%d "
        "features kept, %d dropped. anchors=%s kept=%s dropped=%s",
        len(relevant), len(cols), len(dropped), sorted(anchors)[:12],
        relevant[:8], dropped[:8],
    )
    return relevant
