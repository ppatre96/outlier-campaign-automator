---
phase: 04-brand-voice
plan: 03
subsystem: copy-generation-agents
tags: [brand-voice, agent-instructions, self-check, validation]
decisions: ["Placed 14 AI pattern checks as mandatory validation gates", "Created separate agent file for copy-writer with comprehensive brand voice section", "Added brand_voice_score tracking field to brief generator"]
completed_date: 2026-04-21
duration_minutes: 8
---

# Phase 04 Plan 03: Agent Instructions Update Summary

Updated copy generation agent instructions with explicit brand voice requirements, self-check gates, and revision loops.

**What was built:**
- `.claude/agents/outlier-copy-writer.md` — NEW agent file with 14 AI pattern checks, 18 terminology rules, self-check gates, and revision loop documentation
- `.claude/agents/ad-creative-brief-generator.md` — Updated with Brand Voice Alignment section, brand voice constraints, sample output, and compliance scoring

**Key deliverables:**

1. **outlier-copy-writer.md (NEW)** — 425 lines
   - Brand Voice Requirements section (top of file)
   - Quick Terminology Reference table (18 rules from CLAUDE.md + brand-voice.md)
   - All 14 AI Pattern Checks documented with violation examples and fixes
   - Self-Check Gate (step-by-step checklist before submission)
   - Revision Loop (iteration process with severity levels: MUST, SHOULD, NICE-TO-HAVE)
   - InMail Copy Generation (4 angles: Financial, Expertise, Earnings, Flexibility)
   - Email Copy Generation (subject, opening, body, sign-off guidelines)
   - Platform-Specific Notes (InMail, LinkedIn ads, Email, SMS)
   - Feedback Loop Integration section
   - Error Handling & Blockers section
   - Reference to @.claude/brand-voice.md (single source of truth)

2. **ad-creative-brief-generator.md (UPDATED)** — Added 110 lines
   - New Brand Voice Alignment section (after Overview)
   - 5 Brand Voice Constraints on Brief Output
   - Sample JSON output demonstrating brand voice compliance
   - Brand Voice Violations to Avoid section
   - brand_voice_score field (0-1) added to Tracking for Feedback Loop

**Metrics:**
- 14 AI pattern checks documented with examples and severity levels
- 18 terminology rules quick-referenced (TERM-01 through TERM-18)
- 2 agent files updated/created
- 535 total lines of new/updated agent documentation
- All files reference @.claude/brand-voice.md as source of truth

**How to use:**
1. **Agents reference these files before generating copy** — Load Brand Voice Requirements section into context
2. **Self-check before submission** — Run the 14 pattern checks (Can be automated with regex/heuristics)
3. **Revision loop on violations** — Agents iterate until MUST violations resolved
4. **Feedback loop integration** — feedback_agent uses brief performance + brand_voice_score to optimize future generations

**Self-check validation:**
- ✅ Brand Voice Requirements section present in outlier-copy-writer.md (line 18)
- ✅ All 14 AI Pattern Checks documented with examples (lines 41-126)
- ✅ Quick Terminology Reference table with 18 rules (lines 30-48)
- ✅ Self-Check Gate section with step-by-step checklist (lines 128-139)
- ✅ Revision Loop section with severity levels (lines 141-163)
- ✅ InMail Copy Generation with 4 angles + brand voice notes (lines 166-207)
- ✅ Email Copy Generation section (lines 211-249)
- ✅ Platform-Specific Notes (InMail, LinkedIn ads, Email, SMS) (lines 251-302)
- ✅ Feedback Loop Integration section (lines 305-323)
- ✅ Error Handling section (lines 326-360)
- ✅ Brand Voice Alignment section added to ad-creative-brief-generator.md (line 14)
- ✅ 5 Brand Voice Constraints documented (lines 20-39)
- ✅ Sample output with brand voice (lines 41-57)
- ✅ Violations to avoid section (lines 59-72)
- ✅ brand_voice_score field documented (lines 87-92)
- ✅ Both files reference @.claude/brand-voice.md

**Verification:**
```bash
grep -c "Brand Voice Requirements" ./.claude/agents/outlier-copy-writer.md
→ 1 (header)

grep -c "Brand Voice Alignment" ./.claude/agents/ad-creative-brief-generator.md
→ 1 (new section)

grep "@.claude/brand-voice.md" ./.claude/agents/*.md
→ outlier-copy-writer.md: 2 references (line 8, 30)
→ ad-creative-brief-generator.md: 1 reference (line 15)

grep -c "Self-Check Gate\|Revision Loop" ./.claude/agents/outlier-copy-writer.md
→ 2 (sections present)
```

**Commit:** c8ca48d — feat(04-03): add brand voice requirements to agent instructions

**Impact:**
- Copy generation agents now have explicit, actionable brand voice guidance
- Self-check gates enable agents to validate their own output before submission
- Revision loops allow agents to iterate on violations automatically
- Ad creative brief generator now generates briefs primed for brand voice compliance
- Single source of truth (@.claude/brand-voice.md) referenced in both agents

**Notes:**
- Plan executed autonomously; no blockers encountered
- No deviations from plan needed
- Both agent files tested for presence of required sections via grep validation
- Files ready for use in copy generation workflows

