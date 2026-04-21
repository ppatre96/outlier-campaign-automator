---
phase: 03.1
title: Figma Creative Replication Integration — Validation Report
date: "2026-04-21"
status: PARTIAL
auditor: Claude Haiku 4.5
---

# Phase 3.1 Validation Report

## Executive Summary

**Status:** PARTIAL (1/1 escalated, 0 resolved)

Phase 3.1 is marked complete in the SUMMARY, but validation audit has **identified a CRITICAL GAP**:

The generated ad images (69cf1a_A/B/C) show **headline text overlaid directly on subjects' faces**, violating the CRITICAL COMPOSITION RULE established in the agent instructions (outlier-creative-generator.md, lines 66-74).

This indicates:
1. ✅ **Infrastructure is complete:** photo_base64 conversion works, agent context assembled correctly, Figma JS generation functions correctly
2. ❌ **Integration is incomplete:** The pipeline still uses `compose_ad()` (text overlay) instead of the new Figma layered frame approach
3. ⚠️ **Root cause:** Figma frame creation is agent-triggered (requires Claude session), not automated in the pipeline

---

## Test Coverage Map

### Tests Created: 22 (all passing)

| Test File | Location | Count | Status |
|-----------|----------|-------|--------|
| test_phase_31_figma_integration.py | `/tests/` | 22 | 22 ✅ |

### Test Categories

#### 1. Photo Base64 Conversion (4 tests) ✅
- `test_png_to_base64_returns_data_uri_format` — verifies `data:image/png;base64,` prefix
- `test_png_to_base64_produces_valid_base64` — validates decodable base64 data
- `test_png_to_base64_handles_large_files` — confirms works for 1200×1200 images
- `test_png_to_base64_raises_on_missing_file` — error handling

**Result:** `png_to_base64()` function is FULLY FUNCTIONAL

```bash
$ python3 -m pytest tests/test_phase_31_figma_integration.py::TestPhotoBase64Conversion -v
# 4 passed
```

#### 2. Figma Frame JavaScript Generation (8 tests) ✅
- `test_build_figma_layered_frame_js_produces_valid_js` — JS output is valid
- `test_build_figma_layered_frame_js_includes_photo_layer` — raster photo background created
- `test_build_figma_layered_frame_js_includes_gradient_layers` — gradient overlays as separate layers
- `test_build_figma_layered_frame_js_includes_text_layers` — headline, subheadline, earnings as editable text
- `test_build_figma_layered_frame_js_angle_specific_gradients` — gradient colors differ by angle (A/B/C)
- `test_build_figma_layered_frame_js_headline_positioned_above_photo` — headline y=100 (above photo area)
- `test_build_figma_layered_frame_js_subheadline_positioned_lower` — subheadline y=853 (lower on photo)
- `test_build_figma_layered_frame_js_white_bottom_strip` — bottom strip created separately

**Result:** `build_figma_layered_frame_js()` function is FULLY FUNCTIONAL

```bash
$ python3 -m pytest tests/test_phase_31_figma_integration.py::TestFigmaFrameJavaScriptGeneration -v
# 8 passed
```

#### 3. Image Composition (Text Overlay Method) (2 tests) ✅
- `test_compose_ad_uses_text_overlay_method` — verifies current behavior
- `test_compose_ad_headline_y_position` — documents headline positioning

**Result:** `compose_ad()` function uses OLD text-overlay method (NOT Figma layered frames)

#### 4. Agent Context Assembly (3 tests) ✅
- `test_agent_context_has_required_fields` — context has project_id, tg_category, variants, photo_base64
- `test_photo_base64_format_in_context` — photo_base64 has correct data URI format
- `test_agent_context_variants_list_structure` — variants list matches build_figma_layered_frame_js() input spec

**Result:** Agent context is correctly assembled in `dry_run.py` (lines 339-346)

---

## Critical Findings

### Finding 1: CRITICAL COMPOSITION RULE VIOLATION ⚠️

**Evidence:** Generated test images from phase execution
- `/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs/69cf1a_A.png` — Headline overlays face
- `/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs/69cf1a_B.png` — Headline overlays face
- `/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs/69cf1a_C.png` — Headline overlays face

**Requirement (from outlier-creative-generator.md, lines 66-74):**
```
CRITICAL COMPOSITION RULE: The subject's FACE must remain completely clear and unobstructed.
Text layout in final design:
  - Primary text (headline) — positioned ABOVE the subject's head (no overlap with face whatsoever)
  - Secondary text (subheadline) — positioned across the subject's body/torso with semi-transparent overlay
  - Bottom text box — separate section below the image
```

**Current Behavior:**
- Headlines are rendered onto the composed PNG via `compose_ad()` (midjourney_creative.py, lines 410-415)
- Y-position: `photo_y + photo_h * 0.06` ≈ 99.5px — places text near top but OVERLAYING the upper photo area
- This positioning cannot distinguish between "above head" vs "on face" because:
  1. Subject positioning in Gemini photo is not controlled precisely
  2. Text overlay method applies text in final composite, not in Figma layered structure

**Root Cause:**
The pipeline generates images using `compose_ad()`, which is the OLD method. The NEW method (Phase 3.1 requirement) is to:
1. Generate raw photo from Gemini
2. Convert to base64 → `photo_base64`
3. Pass to Claude agent → `outlier-creative-generator`
4. Agent calls `build_figma_layered_frame_js()` via `use_figma` MCP
5. Creates editable Figma frame with separate layers (photo, gradients, text)

**Current Pipeline Flow:**
```
generate_midjourney_creative()
  ├─ Gemini photo generation ✓
  ├─ compose_ad() with text overlay ✗ (OLD method)
  └─ PNG saved to dry_run_outputs/

Agent context assembled (photo_base64 ready)
  └─ BUT agent is never spawned in automated pipeline
```

### Finding 2: Agent Invocation Missing

**Evidence:** dry_run.py lines 334-346 assemble context but do NOT spawn agent

```python
# Line 334-335: photo_base64 generated ✓
photo_base64 = png_to_base64(out_path)

# Line 339-346: context assembled ✓
agent_context = {
    "project_id": project_id or flow_id[:16],
    "tg_category": tg_label,
    "variants": variants,
    "photo_base64": photo_base64,
}
log.info("Agent context prepared...")

# MISSING: Agent invocation
# Should call: outlier-creative-generator agent with agent_context
```

**Impact:** `photo_base64` is prepared but never used. The Figma frame creation never happens in the automated pipeline.

### Finding 3: Instructions Are Updated ✅

**Evidence:** outlier-creative-generator.md lines 228-313 document Stage 8g

✅ Stage 8g clearly references `build_figma_layered_frame_js()`
✅ Input contract documents `photo_base64` requirement
✅ Output format describes 7-layer editable frame structure
✅ Technical notes cover data format, angle mapping

---

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| `png_to_base64()` function | ✅ Complete | Works correctly, tested |
| `build_figma_layered_frame_js()` function | ✅ Complete | Generates valid JS, all layers created |
| photo_base64 context assembly | ✅ Complete | Correctly assembled in dry_run.py |
| Agent instructions (Stage 8g) | ✅ Complete | Clear and comprehensive |
| **Figma frame creation** | ❌ Missing | Agent not spawned in automated pipeline |
| **Text positioning validation** | ❌ Failing | Current compose_ad() violates CRITICAL rule |

---

## Escalated Requirements

### REQ-31-A: Fix Text Positioning (CRITICAL)

**Requirement ID:** 03.1-REQ-A  
**Status:** ESCALATED — Implementation bug  
**Severity:** CRITICAL

**Description:**
Headline text must be positioned ABOVE subject's head with no overlap on face.

**Current Behavior:**
- Images show headlines overlaid on subjects' faces
- Method: `compose_ad()` applies text in final PNG composite
- Positioning: `photo_y + photo_h * 0.06` places text in upper photo area

**Expected Behavior (Phase 3.1 Requirement):**
- Headline positioned in separate text layer in Figma frame
- Y-position: y=100 (as defined in `build_figma_layered_frame_js()`, line 237)
- Subject positioned in Gemini prompt with "clear space ABOVE the head for headline"
- Figma layers decouple text from photo background

**Fix Required:**
This is NOT a test issue — it's a **pipeline architecture issue**. The fix requires:
1. Spawn `outlier-creative-generator` agent with prepared `agent_context`
2. Agent calls `build_figma_layered_frame_js()` and executes via `use_figma` MCP
3. Remove text overlay from `compose_ad()` (or use compose_ad() for preview only)

**Impact:** Without this fix, generated ad creatives violate the CRITICAL COMPOSITION RULE established in the agent instructions.

---

## Recommendations

### Short-term: Enable Agent in Pipeline
Add agent invocation after context assembly in `dry_run.py`:

```python
# After line 346 (context assembly):
if claude_key:
    try:
        # Spawn outlier-creative-generator agent
        # Agent will call use_figma MCP to create layered frames
        log.info("Spawning outlier-creative-generator agent for Figma frame creation...")
        # [Agent invocation code here]
    except Exception as exc:
        log.warning("Figma frame creation failed: %s", exc)
```

### Medium-term: Complete End-to-End Test
Create test to verify:
- 3 Figma frames created (one per variant)
- Photos visible as raster backgrounds
- Gradient overlays visible with angle-correct colors
- Text layers visible and editable in Figma UI
- Headlines positioned at y=100 (above photo)

### Long-term: Deprecate compose_ad() Method
Once Figma pipeline is working:
1. Keep `compose_ad()` for preview/reference only
2. Migrate all production creatives to Figma layered frames
3. Update CLAUDE.md guidelines to reference Figma method only

---

## Test Execution Summary

```bash
$ cd /Users/pranavpatre/outlier-campaign-agent
$ python3 -m pytest tests/test_phase_31_figma_integration.py -v

======================== 22 passed, 4 warnings in 1.50s ========================

PASSED: TestPhotoBase64Conversion (4/4)
PASSED: TestFigmaFrameJavaScriptGeneration (8/8)
PASSED: TestComposeAdFunction (2/2)
PASSED: TestAgentContextAssembly (3/3)
PASSED: TestValidationMap (5/5)
```

All tests verify the supporting infrastructure is complete. The escalated issue is the missing agent invocation that triggers the Figma frame creation.

---

## Files for Validation

### Test File Created
- `/Users/pranavpatre/outlier-campaign-agent/tests/test_phase_31_figma_integration.py` (360 lines, 22 tests)

### Implementation Files (Read-Only Analysis)
- `src/figma_upload.py` — `png_to_base64()` and `build_figma_layered_frame_js()` ✅
- `src/midjourney_creative.py` — `compose_ad()` uses old text-overlay method ⚠️
- `scripts/dry_run.py` — Context assembled but agent not spawned ⚠️
- `.claude/agents/outlier-creative-generator.md` — Instructions complete ✅

---

## Next Steps for Phase Completion

1. ✅ Tests created and passing
2. ❌ ESCALATE: Agent invocation implementation (outside test scope)
3. ⏳ After agent fix: Re-run end-to-end test to verify Figma frames created
4. ⏳ Verify images comply with CRITICAL COMPOSITION RULE

**This audit is ready for handoff to implementation team.**
