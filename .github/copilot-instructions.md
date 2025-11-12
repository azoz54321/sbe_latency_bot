
# Copilot Instructions - sbe_latency_bot

Agent: English-only policy (no Arabic anywhere in the repository)

Scope (effective immediately):

Use English only for everything committed to the repo: code, comments, docs/READMEs, filenames/dirs, config keys, environment variables, commit messages, PR titles/descriptions, CI/workflow names, log messages.

No Arabic text and no non-ASCII punctuation (e.g., EM/EN dashes, smart quotes), and no zero-width/RTL/BiDi characters.

Encoding is UTF-8 (no BOM) across the repo.

Headings & punctuation:

Prefer ASCII punctuation. Use a plain hyphen - in headings and YAML.
Example (first line of this file):
# Copilot Instructions - sbe_latency_bot

Logging:

Keep these exact tags stable and uppercase: [BOOT] [HARAM] [SHADOW] [BUY] [SELL].

Log text must be English.

CI & workflows:

All workflow/job/step names English ASCII only.

Avoid Unicode punctuation in YAML (use -, not —).

If you encounter non-English content:

Do not commit it. Replace with English equivalents.

If localization is needed for end-users, generate it outside the repo (release notes, external docs) or behind an owner-approved location that is excluded from CI.

Pre-commit sanity (optional, fast checks):

Find non-ASCII:
git grep -nP "[^\x00-\x7F]" -- . ":(exclude)*/.git/*"

Find Arabic script specifically:
git grep -nP "[\x{0600}-\x{06FF}]"

Commit style:

English commits only. Example:
docs(ci): enforce English-only project policy; replace non-ASCII punctuation

Do not change (contract):

Do not rename or alter the five log tags.

Do not reintroduce Unicode punctuation in CI or docs.

If you want, I can also add a tiny pre-commit hook that blocks non-ASCII in staged text files.

---

Agent: Change Proposal Protocol (follow before any modification)

English-only / ASCII-only: All code, comments, docs, CI, logs, commit messages, and file names must be English with ASCII punctuation. Encoding: UTF-8 (no BOM). Do not introduce Arabic or non-ASCII characters.

Approval gate: Never push changes without explicit human approval. Post a proposal first; wait for the maintainer to reply APPROVE. After approval, implement exactly the approved scope.

1) Propose, don’t apply (What / Why / Impact)

What: Describe the minimal change you intend to make (files, functions, symbols).

Why: Tie to the project’s goals (low-latency pipeline, stability, ops observability).

Impact: Any risk to log tags, build, CI, or runtime behavior.

2) Show the plan as a diff preview

Provide a small, surgical diff (unified diff) or a file-by-file edit list.

Keep to the smallest viable scope; split unrelated edits.

3) Respect project invariants (do not break these)

Log tags are sacred: [BOOT] [HARAM] [SHADOW] [BUY] [SELL] must not be renamed or reformatted.

Shadow-mode first: New logic must be safe under --features shadow-mode (no side effects) before enabling live paths.

Schema changes: If schemas/*.xml is touched, require a clean build and native decoder re-gen (cargo clean && cargo build --release).

Daily reset semantics: Do not change 03:00 KSA reset behavior, universe rebuild, daily bans, or AHI gating without explicit approval.

4) Build & run checks (pre-merge)

Local build: cargo build --release --features shadow-mode (no warnings if possible).

Boot in shadow-mode and confirm logs include the tags above (examples in README/logs).

No changes to external interfaces (FFI layout, log tag names) unless explicitly approved.

5) Commit & PR hygiene (after approval)

Commit message (English):
feat(scope): one-line summary or ci/docs: for non-runtime changes.

Scope control: Only include approved files. No opportunistic refactors.

Rollback note: Provide a one-line revert plan (files to revert) in the PR description.

6) CI & workflow edits (special care)

Use ASCII punctuation in workflow names/steps (use -, not —).

Avoid brittle Unicode greps. Prefer token-based checks (e.g., match Copilot and sbe_latency_bot).

When generating YAML from PowerShell, use single-quoted here-strings (@' ... '@) to avoid ${{ }} expansion.

7) Communication contract

Before coding: post the proposal + diff preview → wait for APPROVE.

If anything is unclear, ask concise questions with concrete options (A/B), defaulting to the least risky change.