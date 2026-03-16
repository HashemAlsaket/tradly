# Implementation Audit Workflow

This document defines the required workflow for building the next-step modeling stack.

The goal is to keep work narrow, auditable, and repeatable.

## Working Rule

Every implementation step must follow this sequence:

1. implement one step only
2. stop
3. audit that step
4. if the audit is weak, iterate
5. only then move to the next step

Do not combine multiple modeling steps into a single implementation pass.

## Canonical Local Runtime

All verification commands should use the repo-local virtualenv and `PYTHONPATH=src`.

Canonical Python:

```bash
/Users/hashemalsaket/Desktop/workspace/tradly/.venv/bin/python
```

Canonical import path:

```bash
PYTHONPATH=src
```

Use this pattern for all step verification:

```bash
PYTHONPATH=src /Users/hashemalsaket/Desktop/workspace/tradly/.venv/bin/python -m <module>
```

## Canonical Verification Rules

Each step should produce all of the following when feasible:

1. code for the step
2. one concrete runtime artifact or successful runtime verification
3. one explicit audit summary with pass/fail judgment

If a step cannot produce a runtime artifact because of missing external credentials, missing provider access, or intentional isolation, that limitation must be stated explicitly in the audit.

## Audit Rubric

A step passes audit only if all of the following are true:

1. `Scope Discipline`
   - only the intended step was implemented
   - no unrelated architecture was bundled in

2. `Contract Honesty`
   - required inputs are actually enforced or reflected in coverage/freshness/confidence
   - output fields match the declared spec for that step
   - the model does not claim stronger evidence than it actually has

3. `Single Source Of Truth`
   - metadata and step-specific configuration are not duplicated across multiple files without reason
   - the implementation uses canonical registry/spec/runtime definitions where applicable

4. `Runtime Verifiability`
   - the step imports successfully
   - the step runs successfully when feasible
   - the step writes an expected output artifact or returns an expected verification signal

5. `Auditability`
   - important evidence fields are present
   - failure and thin-evidence states are inspectable
   - the output is understandable enough to debug the step in isolation

6. `No Blocking Findings`
   - the audit may note calibration risks or future improvements
   - the audit must not leave unresolved blocking contract violations

## Step Output Convention

For each modeling step, the audit should state:

1. what was implemented
2. what command was used to verify it
3. what artifact or signal was produced
4. whether the step passes audit
5. any residual risks

## Current Intended Modeling Sequence

The current sequence is:

1. modeling spec finalization
2. model registry scaffolding
3. `market_regime_v1`
4. `sector_movement_v1`
5. `symbol_movement_v1`
6. `sector_news_v1`
7. `symbol_news_v1`
8. `range_expectation_v1`
9. `ensemble_v1`
10. LLM review upgrade
11. dashboard integration

Each step should be completed and audited before the next begins.
