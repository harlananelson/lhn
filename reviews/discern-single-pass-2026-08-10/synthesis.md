# Plan review synthesis — Discern single-pass fix

**Plan:** `/home/harlan/projects/grok/projects/hmi/DISCERN-SCAN-FIX-PLAN.md`  
**Bundle:** `reviews/discern-plan-bundle/`  
**Date:** 2026-08-10  

## Reviewers run

| Channel | Model | Verdict | Path |
|---|---|---|---|
| call-claude (Fable) | `claude-fable-5` thorough/high | **APPROVE with changes** | `reviews/DISCERN-SCAN-FIX-PLAN-fable-review.md` |
| AskSage | `google-claude-46-opus` | **APPROVE with changes** | `reviews/asksage/…-google-claude-46-opus.md` |
| AskSage | `gpt-5.2` | **APPROVE with changes** | `reviews/asksage/…-gpt-5.2.md` |
| AskSage | `google-gemini-2.5-pro` | **APPROVE with changes** | `reviews/asksage/…-google-gemini-2.5-pro.md` |
| AskSage | `grok-4-20-reasoning` | **APPROVE with changes** | `reviews/asksage/…-grok-4-20-reasoning.md` |
| AskSage | `xai-grok` (legacy id) | **failed** — model retired | use `grok-4-20-*` |

All successful reviewers: **APPROVE with changes**, **CONVERGENCE: NONE** (implementable after must-fixes).

---

## Consensus (3+ models)

### Diagnosis & algorithm
- N-scan-from-union on Spark 2.4.4 is real; tag+explode (indicator-style single pass) is the right fix.
- Preserve union semantics for same-flag multi-concept hits (066 `echo`).
- Phase-2 `has_any` prefilter only after measuring 066.

### Spark default (strong majority)
- **Default: explode → `isNotNull`**, not higher-order `filter(array, λ)`.  
  Gemini alone preferred HOF; Fable/Opus/GPT/Grok-4 all preferred explode path for 2.4.4 robustness.

### push_discern subsetting
- Directionally correct (matches foresight + old hnelson3).
- **Fable (strongest dissent):** decouple from PR1 — land single-pass first; subset push as PR2 after HDL verification of re-broadcast / silent-empty risks.
- Others treat subset as safe required companion with dedup of concept lists.

### Testing
- Stub-register `has_concept_in_context` offline (behavioral, not just structural).
- Assert same-flag duplicate rows; nested retained_fields; no Union in plan if possible.
- HDL smoke + measured exec-timeout for 066 (Fable: reconcile ~8h/pass math vs 6h timeout).

---

## Must-fix checklist (merged, ordered)

| # | Item | Sources |
|---|---|---|
| 1 | **PR1 = single-pass only**; consider **PR2 = push subsetting** after HDL verify | Fable (primary) |
| 2 | **Default explode + `isNotNull`**; no HOF in v1 critical path | Fable, Opus, GPT, Grok-4 |
| 3 | **Dedup concepts per context** before push (`dict.fromkeys`) | Opus, Grok-4, plan |
| 4 | **Do not add duplicate-flag validation** on events (unlike `_flags`); comment why | Opus |
| 5 | **`_keep_expr` once** on post-cohort/post-window `source_df.columns` | Opus, GPT, Grok-4 |
| 6 | **flag / `_flags` collision guard** (or `__lhn_flags__` internal name) | Fable, Opus |
| 7 | **Stub-UDF offline tests** + same-flag multiplicity + nested retained | Fable, Opus, GPT |
| 8 | **066 re-queue:** measured `--exec-timeout`, early day; phase-2 contingency if still slow | Fable |
| 9 | Update push docstring if subset lands; note full-context was “proven 055 path” | GPT |
| 10 | Shared `_lit` helper (optional but preferred) | Opus, Grok-4 |

---

## Speculative: which agent should implement

### Recommended seating (orchestrator-workers doctrine)

| Seat | Agent | Why |
|---|---|---|
| **Orchestrator / adjudicator** | **This Grok session (or human Harlan)** | Holds plan + multi-model reviews; decides which must-fixes to take; verifies worker claims against `extract.py` |
| **Implementer** | **Grounded Claude Opus-class in `~/projects/lhn`** | Best for careful Spark-2.4 contract-preserving edit + pytest; worktree/branch → PR through lhn gate |
| **Alt implementer** | **Human (Harlan)** if preferring hand edit | Domain owner of foresight/hnelson3 patterns; Grok-4 explicitly named this |
| **PR reviewer (independent)** | **call-grok or AskSage panel on the diff** | Cross-check physical plan / edge cases after code exists |
| **Ops / HDL smoke** | **Human + hdl_schedule** | Kernel bump, smoke, 066 re-queue — not pure LLM |

### Concrete invocation for implementer

```bash
# Preferred: autonomous worktree task in lhn (Claude Opus default, not Fable)
~/projects/grok/call-claude.sh --task \
  --repo ~/projects/lhn \
  --label discern-events-single-pass \
  --prompt-file /path/to/implement-prompt.md \
  --max-turns 80
```

Implement prompt should attach:
- the plan + this synthesis must-fix checklist  
- instruction: **PR1 only** (single-pass events; leave push full-context unless user flips)  
- Spark 2.4: explode default; no `F.call_udf`; preserve union semantics  

### Who should NOT implement

| Agent | Why not |
|---|---|
| **Fable as implementer** | Excellent plan reviewer here; weaker for “plausible but wrong” Spark plan subtleties on a critical path |
| **Haiku / low-effort Sonnet** | Contract + 2.4 constraints; all reviewers warned |
| **Contextless `-p` any model** | Risk of Spark 3 APIs / wrong helper |
| **AskSage panel as implementer** | Review-only; no repo write/test loop |
| **This grok session editing lhn without worktree discipline** | OK if user asks, but preferred: lhn-rooted task with PR |

### My speculative ranking (probability of clean PR1)

1. **Claude Opus task mode, grounded on lhn + plan** (~best balance of fidelity and speed)  
2. **Harlan hand-implement** (highest domain trust; slower)  
3. **This Grok session implementing in lhn worktree** (strong Spark reasoning; verify with second model)  
4. **Fable task mode** (only if tightly constrained by checklist + human/Grok review before merge)  
5. **Avoid:** unguided multi-agent “just fix it” without the must-fix list  

---

## Adjudication notes (orchestrator)

- **Take Fable’s PR split seriously.** Subsetting is valuable but not what unblocks the 4× scan; silent-empty under subset is a real failure mode until proven. Default plan revision: **PR1 scan fix, PR2 subset push**.  
- **Ignore Gemini’s HOF-default** for v1 unless HDL proves explode is worse (unlikely at N≤10).  
- **Do not invent `_legacy_union` flag** — consensus prefers smoke A/B without shipping dead code.  
- **Runtime math risk is real:** if one pass is still multi-hour, single-pass alone may need timeout/dispatch/phase-2 — track as ops contingency, not a reason to keep N scans.

---

## Recommended next step

1. Amend plan defaults: explode default; PR1/PR2 split; stub-UDF tests; collision guard.  
2. Launch **Claude Opus `--task`** on `~/projects/lhn` with the amended checklist.  
3. Adjudicate PR with Grok/AskSage on the **diff**, not the plan again.  
4. HDL smoke → re-queue 066 only after kernel has PR1.
