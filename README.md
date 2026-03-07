# tradly

Initial scaffold for a long-only, manual-execution trading intelligence platform.

## Current scope
- Trading and data specs in:
  - `TRADING_SPEC_V1.md`
  - `DATA_CONTRACT_V1.md`
- Typed schema and service scaffold under `src/tradly/`
- Broker-state freshness gate implemented in `src/tradly/services/freshness_gate.py`
- OpenAI-only model suite config in `src/tradly/config/model_suite.py`
- Basic tests in `tests/`

## Quick start
1. Create a virtual env and install deps:
   - `python -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -e '.[dev]'`
2. Run tests:
   - `pytest`

## OpenAI model suite env vars
- `OPENAI_LLM_MODEL`
- `OPENAI_VLM_MODEL`
- `OPENAI_STT_MODEL`
