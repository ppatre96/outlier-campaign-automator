---
phase: 04-brand-voice
plan: 01
subsystem: brand-voice
type: feature
status: complete
completed_date: 2026-04-21
duration_minutes: 15
tasks_completed: 1
files_created: 1
tags: [brand-voice, terminology, validation, copy-generation]
dependency_graph:
  provides: [brand-voice-rules, terminology-reference, ai-pattern-checks]
  affects: [copy-generation-agents, validation-code, future-brand-enforcement]
key_files:
  created:
    - path: ".claude/brand-voice.md"
      lines: 636
      purpose: "Single source of truth for Outlier brand voice, terminology rules, AI pattern checks, platform guidance"
decisions:
  - "Established 18 terminology rules (TERM-01 through TERM-18) with stable IDs for code reference"
  - "Documented 14 AI pattern checks as testable validators: Active Voice, Staccato, Anaphora, Parallel Rhetoric, Superlatives, Vague Claims, Hype, Consecutive Colons, LLM Filler, Lists, Personal Pronouns, Sentence Variety, Warmth, Comma Splicing"
  - "Created platform-specific sections (Email, LinkedIn InMail, LinkedIn Static Ads, Community Posts, SMS) with tone, length, CTA guidance"
  - "Included AI/LLM reference guidelines (when to name models, generic language, how to describe usage)"
  - "Structured for agent integration: Quick Validation Checklist, automation examples, violation reporting"
metrics:
  terminology_rules: 18
  ai_pattern_checks: 14
  platform_guides: 5
  lines_of_documentation: 636
---

# Phase 04 Plan 01: Brand Voice Documentation Summary

**Objective:** Create a comprehensive, centralized brand voice documentation file that all copy generation systems can reference and validate against.

**Status:** COMPLETE ✓

## What Was Built

### `.claude/brand-voice.md` — Comprehensive Brand Voice Guide

A 636-line, production-ready reference document that serves as the single source of truth for Outlier brand voice across all channels.

**Structure:**

1. **Terminology Reference Table (18 Rules)**
   - TERM-01 through TERM-18 with banned terms, approved alternatives, context, and notes
   - Includes vocabulary rules from CLAUDE.md plus extended terminology (leverage, at scale, etc.)
   - Stable IDs enable code reference and validation

2. **Core Tone & Voice Rules**
   - Brand voice essence (professional, warm, direct, outcome-focused)
   - Three pillars (Clarity First, Warmth & Respect, Action-Focused)
   - Banned patterns section: No superlatives, no vague claims, no hype, no consecutive colons, no passive dominance

3. **The 14 AI Pattern Checks**
   - Each check includes: name, rule description, detection method, severity (MUST/SHOULD/NICE-TO-HAVE), example violation, example fix
   - Documented as testable patterns for agent self-checks and validation code
   - Checks: Active Voice, Staccato, Anaphora, Parallel Rhetoric, Superlatives, Vague Claims, Hype, Consecutive Colons, LLM Filler, Lists, Personal Pronouns, Sentence Variety, Warmth, Comma Splicing

4. **Platform-Specific Guidance**
   - Email: opening tone, structure, signature, length, CTA style
   - LinkedIn InMail: subject line, narrative arc, CTAs, tone
   - LinkedIn Static Ads: headline energy, subheadline specificity, social proof
   - Community Posts: tone, engagement style, response tone
   - SMS/Text: brevity, abbreviation rules, emoji rules, character limits

5. **AI/LLM References Section**
   - When to name models explicitly vs. generic language
   - How to describe Outlier's use of AI (approved phrases, banned jargon)

6. **Quick Validation Checklist**
   - 10-point checklist for copy generation before submission
   - Covers terminology, voice, tone, platform-appropriateness, clarity

7. **Integration with Automation**
   - Examples for copy generation agents
   - Validation code patterns (terminology check, pattern detection)
   - Violation reporting format

8. **Reporting & Versioning**
   - How to report new patterns and propose terminology rules
   - Quarterly review cycle note
   - Version history (v1.0, 2026-04-21)

## Key Deliverables

✓ **File created:** `.claude/brand-voice.md` (636 lines, 27KB)

✓ **Terminology rules:** All 18 rules from CLAUDE.md documented with TERM-01 through TERM-18 IDs

✓ **AI pattern checks:** All 14 checks documented with rule description, detection method, severity, examples

✓ **Platform coverage:** Email, LinkedIn (InMail + Static Ads), Community Posts, SMS

✓ **Markdown structure:** Clean hierarchy, no formatting errors, proper YAML frontmatter

✓ **Ready for integration:** Includes agent instructions and validation code examples

## Commit

- **Hash:** `24b3ad2`
- **Message:** `feat(04-01): create comprehensive Outlier brand voice guidelines`
- **Files:** `.claude/brand-voice.md` (+636 lines)

## How to Use This Document

### For Copy Generation Agents

Link to `.claude/brand-voice.md` in agent instructions:
```
Before generating any copy (headlines, email bodies, InMails, SMS):
1. Read .claude/brand-voice.md (sections 3 & 4)
2. Review the 14 AI pattern checks
3. Check terminology against TERM-01 through TERM-18
4. After generating, run the Quick Validation Checklist (section 6)
5. Flag MUST violations and regenerate
6. Only submit copy that passes validation
```

### For Validation Code

Use the patterns documented in section 8 (Integration with Automation):
- Terminology validation: Check for banned terms, suggest alternatives by ID
- Active voice check: Regex for "is/are/be/been" + past participles
- Staccato detection: Flag 3+ consecutive sentences <8 words
- Pattern violations: Implement checks for superlatives, hype, filler phrases
- Personal pronouns: Calculate ratio of "you/your/we/our" to total words

**Output format:**
```
VIOLATION: [Check Name] ([ID])
Severity: [MUST | SHOULD | NICE-TO-HAVE]
Location: [sentence/paragraph]
Violation: [quoted text]
Suggestion: [rewrite]
```

### For Campaign Managers & Writers

Use the Quick Validation Checklist (section 6) before submitting any copy:
- [ ] No banned terminology (TERM-01 through TERM-18)
- [ ] Active voice throughout
- [ ] No staccato sentences
- [ ] No repeated sentence openings
- [ ] No superlatives or vague claims
- [ ] Appropriate for platform
- [ ] Personal pronouns >30%
- [ ] Sentence variety
- [ ] Warm, human tone
- [ ] Clear CTA

## Deviations from Plan

**None.** Plan executed exactly as written.

All 18 terminology rules documented with IDs. All 14 AI pattern checks documented with detection methods, severity, examples, and fixes. All platform-specific sections complete (email, LinkedIn InMail, LinkedIn Static Ads, community posts, SMS). Document is comprehensive, well-structured, and ready for immediate integration.

## Success Criteria ✓

- [x] `.claude/brand-voice.md` exists and is >500 lines (actual: 636 lines)
- [x] All 18 terminology rules documented with stable IDs (TERM-01 through TERM-18)
- [x] All 14 AI pattern checks documented with rule name, description, example, detection method
- [x] Document organized with clear sections: Terminology, Tone Rules, 14 Pattern Checks, Platform Notes, Quick Checklist
- [x] Platform-specific notes cover email, LinkedIn (InMail and static ads), SMS
- [x] AI/LLM reference section complete
- [x] Validation checklist is concise and actionable (10 items)
- [x] No broken links or formatting errors in markdown

## Next Steps

1. **Link to agent instructions:** Update all copy-generation agent instructions to reference `.claude/brand-voice.md`
2. **Implement validation:** Build validation functions for terminology (TERM table) and pattern checks
3. **Create validation API:** Expose pattern checks as Python functions for real-time copy validation
4. **Test with agents:** Run copy generation through validation checklist; refine patterns based on output

---

**Completed:** 2026-04-21 at 15:21 UTC
**Duration:** ~15 minutes
**Status:** Ready for integration
