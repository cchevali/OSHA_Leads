# Codex Working Guide

`AGENTS.md` at the repo root is the canonical instruction contract. Use this file as the default working style only when it does not conflict with `AGENTS.md`, an active Task Packet, or higher-priority system/developer instructions.

## 1. Plan Mode Default

1. Enter planning for any non-trivial task: 3 or more steps, meaningful verification, or architectural decisions.
2. Write the active execution checklist in `tasks/todo.md` before implementation.
3. Include verification steps in the plan, not just build steps.
4. If new facts break the plan, stop, update the plan, and continue from the revised plan.
5. Write detailed enough specs up front to remove ambiguity, while still choosing one highest-odds path.

## 2. Parallel Helper Strategy

1. Use subagents or parallel helper work when tooling supports it and the task is complex enough to benefit.
2. Offload research, repo exploration, and independent verification to focused helper threads when that keeps the main context cleaner.
3. Give each helper one clear responsibility.
4. Prefer direct execution over extra coordination for simple fixes.

## 3. Self-Improvement Loop

1. Review `tasks/lessons.md` at the start of each session.
2. After an explicit user correction, add the pattern and the prevention rule to `tasks/lessons.md`.
3. Favor durable rules that prevent repeat mistakes rather than one-off notes.

## 4. Verification Before Done

1. Do not mark work complete without proving it works.
2. Diff behavior against the previous state when relevant.
3. Ask: "Would a staff engineer approve this?"
4. Run tests, inspect logs where useful, and document the evidence in `tasks/todo.md`.

## 5. Demand Elegance, Without Over-Engineering

1. For non-trivial changes, ask whether there is a cleaner solution with lower long-term cost.
2. If the current fix feels hacky, step back and implement the more elegant version.
3. Skip this extra pass for simple fixes where the straightforward change is already the right one.

## 6. Autonomous Bug Fixing

1. When a bug is reported, diagnose it directly from logs, errors, and tests.
2. Minimize context switching for the user.
3. Fix failing CI-style tests automatically when they are part of the scoped issue.

## 7. Task Management

1. `tasks/todo.md` is the active task plan and review log for the current unit of work.
2. `docs/TODO.md` remains the durable repo backlog required by `AGENTS.md`.
3. Track progress by checking off items as work completes.
4. Add a short review section to `tasks/todo.md` before closing the task.
5. Update `tasks/lessons.md` after user corrections.

## 8. Core Principles

### Simplicity First

Make every change as small and low-risk as possible while still fixing the real problem.

### No Laziness

Find root causes, avoid temporary patches, and hold the work to a senior-engineering standard.
