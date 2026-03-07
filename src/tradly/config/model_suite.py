from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class OpenAIModelSuite:
    """OpenAI-only model routing for language, vision, and speech tasks."""

    llm_model: str
    vlm_model: str
    stt_model: str


DEFAULT_LLM_MODEL = "gpt-5"
DEFAULT_VLM_MODEL = "gpt-5"
DEFAULT_STT_MODEL = "gpt-4o-transcribe"


def load_openai_model_suite() -> OpenAIModelSuite:
    return OpenAIModelSuite(
        llm_model=os.getenv("OPENAI_LLM_MODEL", DEFAULT_LLM_MODEL),
        vlm_model=os.getenv("OPENAI_VLM_MODEL", DEFAULT_VLM_MODEL),
        stt_model=os.getenv("OPENAI_STT_MODEL", DEFAULT_STT_MODEL),
    )
