# Phase 1: Pipeline Integrity - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.

**Date:** 2026-04-21
**Participants:** Pranav Patre + Claude

---

## Session Summary

User's stated intent: "check all the configs and ensure no context is being missed out. since there are multiple sub agents working together in this project, i want the plan mapped out cleanly and no steps must be skipped."

Full pipeline flow was walked through, annotated with bugs at each step, and presented for confirmation.

---

## Q&A Log

**Q: Which gray areas to discuss?**
User: Check all configs and ensure no context is being missed out. Show the current flow.

→ Full annotated pipeline flow was presented with all bugs identified inline.

---

**Q: The has_mj guard checks mj_token AND claude_key — both always empty. Remove has_mj entirely?**
User: "why are we calling generate_midjourney_creative()? We are creating the creatives on gemini"
→ Clarified: generate_midjourney_creative() IS the Gemini pipeline (legacy name). Confirmed two bugs: (1) has_mj guard in main.py, (2) premature GEMINI_API_KEY check in midjourney_creative.py.

**Q: Fix both bugs?**
User: Fix both bugs (Recommended)

---

**Q: InMail dry-run if claude_key gate — remove?**
User: Yes — remove the if claude_key gate (Recommended)

---

**Q: Stage C RuntimeError propagates — match dry_run.py graceful fallback?**
User: Yes — match dry_run.py pattern (Recommended)

---

**Q: SCREENING_END_DATE fix scope — both config.py and call sites?**
User: Fix both: dynamic config default + explicit arg (Recommended)

---

**Q: LinkedIn token auto-refresh — are all 3 creds in .env?**
User: Yes — all three are in .env already

---

**Q: LINKEDIN_INMAIL_SENDER_URN is empty — is this a Phase 1 blocker?**
User: "can we set a random sender"
→ Clarified: must be a real urn:li:person: connected to ad account 510956407.

**Q: Who should the InMail sender be?**
User: "since this is for test, keep sender as tuan"
→ Decision: set urn:li:person:vYrY4QMQH0 (Tuan's internal person URN, discovered in prior session)

---

**Q: Drive upload (DATA-01) — pull into Phase 1 or keep Phase 2?**
User: "skip for now"
→ Phase 2.

---

## Key Clarifications Made

1. `generate_midjourney_creative()` is NOT Midjourney — it is the Gemini/LiteLLM image pipeline. Legacy name from when Midjourney integration was planned.

2. Two separate bugs block creative generation: (a) has_mj guard in main.py that checks dead variables, AND (b) premature GEMINI_API_KEY check in midjourney_creative.py that raises before even trying LiteLLM.

3. InMail sender URN cannot be arbitrary — must be a LinkedIn member connected to ad account 510956407 with InMail send permission.
