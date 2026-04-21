"""Brand Voice Validator — detects violations in copy against Outlier brand guidelines."""

import re
import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional


class ViolationSeverity(Enum):
    """Severity levels for brand voice violations."""
    MUST = "must"  # Hard block, must fix
    SHOULD = "should"  # Recommended fix
    NICE_TO_HAVE = "nice"  # Nice to have, not required


@dataclass
class Violation:
    """Single brand voice violation."""
    rule_id: str  # e.g., "TERM-01", "PATTERN-03"
    rule_name: str  # e.g., "Banned Terminology", "Active Voice"
    severity: ViolationSeverity
    found_text: str  # Exact text from copy that violated
    suggestion: str  # How to fix it
    line_number: int = 0  # Approximate location in copy
    context: str = ""  # Surrounding text


@dataclass
class BrandVoiceReport:
    """Complete validation report for a piece of copy."""
    is_compliant: bool
    violations: List[Violation] = field(default_factory=list)
    must_violations: List[Violation] = field(default_factory=list)  # Hard blocks
    should_violations: List[Violation] = field(default_factory=list)
    nice_violations: List[Violation] = field(default_factory=list)
    confidence_score: float = 1.0  # 1.0 = fully compliant, <1.0 = has issues
    warnings: List[str] = field(default_factory=list)

    def summary(self) -> str:
        """Return human-readable summary."""
        lines = [
            "Brand Voice Validation Report",
            f"Status: {'COMPLIANT' if self.is_compliant else 'VIOLATIONS FOUND'}"
        ]
        if self.must_violations:
            lines.append(f"MUST FIX ({len(self.must_violations)}):")
            for v in self.must_violations[:5]:  # Show first 5
                lines.append(f"  - {v.rule_name}: {v.found_text!r} → {v.suggestion}")
        if self.should_violations:
            lines.append(f"SHOULD FIX ({len(self.should_violations)}):")
            for v in self.should_violations[:3]:  # Show first 3
                lines.append(f"  - {v.rule_name}: {v.found_text!r}")
        if self.nice_violations:
            lines.append(f"NICE TO HAVE ({len(self.nice_violations)}):")
            for v in self.nice_violations[:2]:  # Show first 2
                lines.append(f"  - {v.rule_name}")
        if self.confidence_score < 1.0:
            lines.append(f"Confidence: {self.confidence_score:.0%}")
        return "\n".join(lines)


class BrandVoiceValidator:
    """Main validator class for brand voice compliance."""

    def __init__(self, brand_voice_path: str = "./.claude/brand-voice.md"):
        self.brand_voice_path = Path(brand_voice_path)
        self.log = logging.getLogger("BrandVoiceValidator")
        self.terminology_rules: Dict[str, Dict] = {}
        self.pattern_checks: Dict[str, Dict] = {}
        self._load_rules()

    def _load_rules(self) -> None:
        """Parse brand-voice.md and extract terminology + patterns."""
        if not self.brand_voice_path.exists():
            self.log.warning(f"Brand voice file not found: {self.brand_voice_path}")
            return

        try:
            content = self.brand_voice_path.read_text()
            self._parse_terminology(content)
            self._parse_patterns()
        except Exception as e:
            self.log.error(f"Error loading brand voice rules: {e}")

    def _parse_terminology(self, content: str) -> None:
        """Extract terminology rules from brand-voice.md."""
        # Parse the terminology table (lines with | delimiters)
        lines = content.split("\n")
        in_terminology_section = False
        term_id = 0

        for line in lines:
            # Look for "## Terminology Rules" section
            if "## 1. Terminology Rules" in line or "Terminology" in line:
                in_terminology_section = True
                continue

            # Stop at next major section
            if in_terminology_section and line.startswith("##") and "Terminology" not in line:
                in_terminology_section = False

            # Parse table rows (format: | ID | Don't Say | Instead, Say | Context |)
            if in_terminology_section and line.startswith("|") and "---" not in line and "TERM" not in line and "Don't" not in line:
                parts = [p.strip() for p in line.split("|")[1:-1]]
                if len(parts) >= 3:
                    try:
                        rule_id = parts[0]
                        banned = parts[1]
                        approved = parts[2]
                        context = parts[3] if len(parts) > 3 else "all"
                        if banned and approved and rule_id.startswith("TERM"):
                            self.terminology_rules[rule_id] = {
                                "banned": banned,
                                "approved": approved,
                                "context": context,
                            }
                    except (IndexError, ValueError):
                        continue

    def _parse_patterns(self) -> None:
        """Define the 14 AI pattern checks."""
        patterns = {
            "PATTERN-01": {
                "name": "Active Voice Only",
                "severity": ViolationSeverity.MUST,
                "check_fn": self._check_active_voice,
            },
            "PATTERN-02": {
                "name": "No Staccato Sentences",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_staccato,
            },
            "PATTERN-03": {
                "name": "No Anaphora",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_anaphora,
            },
            "PATTERN-04": {
                "name": "No Parallel Rhetoric",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_parallel,
            },
            "PATTERN-05": {
                "name": "No Superlatives",
                "severity": ViolationSeverity.MUST,
                "check_fn": self._check_superlatives,
            },
            "PATTERN-06": {
                "name": "No Vague Claims",
                "severity": ViolationSeverity.MUST,
                "check_fn": self._check_vague_claims,
            },
            "PATTERN-07": {
                "name": "No Hype Language",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_hype,
            },
            "PATTERN-08": {
                "name": "No Consecutive Colons",
                "severity": ViolationSeverity.NICE_TO_HAVE,
                "check_fn": self._check_colons,
            },
            "PATTERN-09": {
                "name": "No LLM Filler",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_filler,
            },
            "PATTERN-10": {
                "name": "Appropriate List Length",
                "severity": ViolationSeverity.NICE_TO_HAVE,
                "check_fn": self._check_lists,
            },
            "PATTERN-11": {
                "name": "Sufficient Personal Pronouns",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_pronouns,
            },
            "PATTERN-12": {
                "name": "Sentence Variety",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_sentence_variety,
            },
            "PATTERN-13": {
                "name": "Warmth Check",
                "severity": ViolationSeverity.SHOULD,
                "check_fn": self._check_warmth,
            },
            "PATTERN-14": {
                "name": "No Comma Splicing",
                "severity": ViolationSeverity.NICE_TO_HAVE,
                "check_fn": self._check_comma_splice,
            },
        }
        self.pattern_checks = patterns

    # ── Validation methods ──────────────────────────────────────────────────

    def validate_copy(self, copy: str) -> BrandVoiceReport:
        """Main validation method: check copy for all violations."""
        report = BrandVoiceReport(is_compliant=True)

        # 1. Check restricted vocabulary
        self._check_terminology(copy, report)

        # 2. Check AI patterns
        for pattern_id, pattern in self.pattern_checks.items():
            try:
                violations = pattern["check_fn"](copy)
                for v in violations:
                    v.rule_id = pattern_id
                    v.rule_name = pattern["name"]
                    v.severity = pattern["severity"]
                    report.violations.append(v)

                    if v.severity == ViolationSeverity.MUST:
                        report.must_violations.append(v)
                        report.is_compliant = False
                    elif v.severity == ViolationSeverity.SHOULD:
                        report.should_violations.append(v)
                    else:
                        report.nice_violations.append(v)
            except Exception as e:
                self.log.warning(f"Error checking {pattern_id}: {e}")

        # 3. Compute confidence score
        report.confidence_score = max(
            0.0,
            1.0 - (len(report.must_violations) * 0.3 + len(report.should_violations) * 0.1)
        )

        return report

    def _check_terminology(self, copy: str, report: BrandVoiceReport) -> None:
        """Check for banned terminology."""
        copy_lower = copy.lower()
        for rule_id, rule in self.terminology_rules.items():
            banned = rule["banned"].lower()
            approved = rule["approved"]
            if banned in copy_lower:
                # Find all occurrences with word boundaries
                pattern = re.compile(r"\b" + re.escape(banned) + r"\b", re.IGNORECASE)
                for match in pattern.finditer(copy):
                    v = Violation(
                        rule_id=rule_id,
                        rule_name=f"Banned Terminology: {rule['banned']}",
                        severity=ViolationSeverity.MUST,
                        found_text=match.group(),
                        suggestion=f"Use '{approved}' instead",
                        line_number=copy[:match.start()].count("\n"),
                    )
                    report.violations.append(v)
                    report.must_violations.append(v)
                    report.is_compliant = False

    def _check_active_voice(self, copy: str) -> List[Violation]:
        """Flag passive voice (is/are/was/were + past participle)."""
        violations = []
        # Pattern: "is/are/was/were" followed by -ed verb or known past participles
        pattern = r"\b(is|are|was|were)\s+(\w+ed|designed|created|made|given|set|built)\b"
        for match in re.finditer(pattern, copy, re.IGNORECASE):
            violations.append(Violation(
                rule_id="PATTERN-01",
                rule_name="Active Voice Only",
                severity=ViolationSeverity.MUST,
                found_text=match.group(),
                suggestion="Convert to active voice: use a strong verb instead",
                line_number=copy[:match.start()].count("\n"),
            ))
        return violations

    def _check_staccato(self, copy: str) -> List[Violation]:
        """Flag staccato sentences (3+ consecutive short sentences)."""
        violations = []
        sentences = re.split(r'[.!?]+', copy)
        short_count = 0
        for i, sent in enumerate(sentences):
            words = sent.split()
            if 2 <= len(words) <= 8:
                short_count += 1
                if short_count >= 3:
                    violations.append(Violation(
                        rule_id="PATTERN-02",
                        rule_name="No Staccato Sentences",
                        severity=ViolationSeverity.SHOULD,
                        found_text=sent.strip()[:80],
                        suggestion="Vary sentence length: combine short sentences or expand ideas",
                    ))
                    short_count = 0
            else:
                short_count = 0
        return violations

    def _check_anaphora(self, copy: str) -> List[Violation]:
        """Flag anaphora (repeated sentence starts)."""
        violations = []
        sentences = re.split(r'[.!?]+', copy)
        sentence_starts = []
        for s in sentences:
            m = re.match(r"^\s*(\w+)", s)
            if m:
                sentence_starts.append(m.group(1).lower())

        for i in range(len(sentence_starts) - 2):
            if (sentence_starts[i] == sentence_starts[i+1] == sentence_starts[i+2]
                    and sentence_starts[i]):
                v = Violation(
                    rule_id="PATTERN-03",
                    rule_name="No Anaphora",
                    severity=ViolationSeverity.SHOULD,
                    found_text=f"3 sentences starting with '{sentence_starts[i]}'",
                    suggestion="Vary sentence openings to avoid repetitive rhythm",
                )
                violations.append(v)
                break
        return violations

    def _check_parallel(self, copy: str) -> List[Violation]:
        """Flag parallel rhetoric (Not X. Not Y. Not Z.)."""
        violations = []
        pattern = r"(?:Not\s+\w+\.\s+){2,}Not\s+\w+\."
        if re.search(pattern, copy):
            violations.append(Violation(
                rule_id="PATTERN-04",
                rule_name="No Parallel Rhetoric",
                severity=ViolationSeverity.SHOULD,
                found_text="Parallel 'Not X. Not Y. Not Z.' structure",
                suggestion="Use varied syntax instead of repetitive negations",
            ))
        return violations

    def _check_superlatives(self, copy: str) -> List[Violation]:
        """Flag superlative adjectives."""
        superlatives = [
            "best", "amazing", "incredible", "the most", "greatest", "perfect",
            "top-tier", "unbeatable", "unsurpassed", "one-of-a-kind", "unique"
        ]
        violations = []
        for sup in superlatives:
            if re.search(r"\b" + re.escape(sup) + r"\b", copy, re.IGNORECASE):
                violations.append(Violation(
                    rule_id="PATTERN-05",
                    rule_name="No Superlatives",
                    severity=ViolationSeverity.MUST,
                    found_text=sup,
                    suggestion=f"Remove '{sup}' — replace with specific facts or benefits",
                ))
        return violations

    def _check_vague_claims(self, copy: str) -> List[Violation]:
        """Flag vague, unsupported claims."""
        vague = [
            "unlimited", "cutting-edge", "world-class", "state-of-the-art",
            "next-gen", "industry-leading", "pioneering", "groundbreaking"
        ]
        violations = []
        for vague_term in vague:
            if re.search(r"\b" + re.escape(vague_term) + r"\b", copy, re.IGNORECASE):
                violations.append(Violation(
                    rule_id="PATTERN-06",
                    rule_name="No Vague Claims",
                    severity=ViolationSeverity.MUST,
                    found_text=vague_term,
                    suggestion=f"Remove '{vague_term}' — be specific with facts",
                ))
        return violations

    def _check_hype(self, copy: str) -> List[Violation]:
        """Flag hype language."""
        hype = [
            "revolutionary", "life-changing", "once-in-a-lifetime", "game-changing",
            "disruptive", "transformative", "breakthrough", "paradigm shift"
        ]
        violations = []
        for hype_term in hype:
            if re.search(r"\b" + re.escape(hype_term) + r"\b", copy, re.IGNORECASE):
                violations.append(Violation(
                    rule_id="PATTERN-07",
                    rule_name="No Hype Language",
                    severity=ViolationSeverity.SHOULD,
                    found_text=hype_term,
                    suggestion=f"Replace '{hype_term}' with grounded language",
                ))
        return violations

    def _check_colons(self, copy: str) -> List[Violation]:
        """Flag consecutive colons (artificial structure)."""
        violations = []
        if ": " in copy:
            colon_lines = [line for line in copy.split("\n") if ": " in line]
            if len(colon_lines) >= 3:
                violations.append(Violation(
                    rule_id="PATTERN-08",
                    rule_name="No Consecutive Colons",
                    severity=ViolationSeverity.NICE_TO_HAVE,
                    found_text=f"Multiple colon-based lines",
                    suggestion="Vary structure — not every line needs a colon",
                ))
        return violations

    def _check_filler(self, copy: str) -> List[Violation]:
        """Flag LLM filler phrases."""
        fillers = [
            "It's important to", "Whether you're", "In today's world", "As the world continues",
            "It goes without saying", "needless to say", "virtually", "arguably"
        ]
        violations = []
        for filler in fillers:
            if re.search(re.escape(filler), copy, re.IGNORECASE):
                violations.append(Violation(
                    rule_id="PATTERN-09",
                    rule_name="No LLM Filler",
                    severity=ViolationSeverity.SHOULD,
                    found_text=filler,
                    suggestion=f"Remove '{filler}' — get straight to the point",
                ))
        return violations

    def _check_lists(self, copy: str) -> List[Violation]:
        """Flag lists with >4 items."""
        violations = []
        list_pattern = r"^[\s\-\*\•]\s+.+"
        list_items = re.findall(list_pattern, copy, re.MULTILINE)
        if len(list_items) > 4:
            violations.append(Violation(
                rule_id="PATTERN-10",
                rule_name="Appropriate List Length",
                severity=ViolationSeverity.NICE_TO_HAVE,
                found_text=f"{len(list_items)} bullet points",
                suggestion="Limit to 3-4 items max; use prose for more details",
            ))
        return violations

    def _check_pronouns(self, copy: str) -> List[Violation]:
        """Check for sufficient personal pronouns (you, we, your, our, etc.)."""
        violations = []
        pronouns = r"\b(you|your|we|our|us|I|me|my)\b"
        pronoun_count = len(re.findall(pronouns, copy, re.IGNORECASE))
        word_count = len(copy.split())
        pronoun_ratio = pronoun_count / max(word_count, 1)

        if pronoun_ratio < 0.03:  # Less than 3% personal pronouns
            violations.append(Violation(
                rule_id="PATTERN-11",
                rule_name="Sufficient Personal Pronouns",
                severity=ViolationSeverity.SHOULD,
                found_text=f"Only {pronoun_ratio:.0%} personal pronouns",
                suggestion="Use 'you', 'your', 'we' more — speak directly to the reader",
            ))
        return violations

    def _check_sentence_variety(self, copy: str) -> List[Violation]:
        """Check sentence length variety."""
        violations = []
        sentences = re.split(r'[.!?]+', copy)
        lengths = [len(s.split()) for s in sentences if s.strip()]

        if lengths:
            avg_length = sum(lengths) / len(lengths)
            if avg_length >= 20 or avg_length <= 7:
                violations.append(Violation(
                    rule_id="PATTERN-12",
                    rule_name="Sentence Variety",
                    severity=ViolationSeverity.SHOULD,
                    found_text=f"Average sentence length: {avg_length:.0f} words",
                    suggestion="Mix short and longer sentences for rhythm; aim for 10-15 word average",
                ))
        return violations

    def _check_warmth(self, copy: str) -> List[Violation]:
        """Heuristic check for human tone (vs. corporate/robotic)."""
        violations = []
        # Flag: lots of gerunds, passive voice, no contractions
        gerunds = len(re.findall(r"\w+ing\b", copy))
        contractions = len(re.findall(r"\b(don't|can't|won't|it's|that's|we're)\b", copy, re.IGNORECASE))
        passive = len(re.findall(r"\b(is|are|was|were)\s+\w+ed\b", copy))

        if contractions == 0 and passive > 2:
            violations.append(Violation(
                rule_id="PATTERN-13",
                rule_name="Warmth Check",
                severity=ViolationSeverity.SHOULD,
                found_text="Formal tone detected (no contractions, passive voice)",
                suggestion="Add contractions, use active voice, sound more conversational",
            ))
        return violations

    def _check_comma_splice(self, copy: str) -> List[Violation]:
        """Flag comma splice (3+ commas without semicolons/conjunctions)."""
        violations = []
        sentences = re.split(r'[.!?]+', copy)
        for sent in sentences:
            commas = sent.count(",")
            if commas >= 3 and ";" not in sent and not re.search(r",\s+(and|but|or|so|yet)", sent):
                violations.append(Violation(
                    rule_id="PATTERN-14",
                    rule_name="No Comma Splicing",
                    severity=ViolationSeverity.NICE_TO_HAVE,
                    found_text=sent.strip()[:60] + "...",
                    suggestion="Break sentence with semicolon or use conjunctions",
                ))
        return violations


def validate_copy(copy: str, strict: bool = False) -> BrandVoiceReport:
    """Convenience function to validate copy with default validator."""
    validator = BrandVoiceValidator()
    return validator.validate_copy(copy)


def log_validation(report: BrandVoiceReport, context: str = "") -> None:
    """Log validation results."""
    log = logging.getLogger("BrandVoiceValidator")
    log.info(f"Validation {context}: {report.summary()}")
