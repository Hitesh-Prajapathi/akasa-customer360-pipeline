"""'Ask about this page' chat panel (Groq), rendered at the bottom of every dashboard page.

Each page passes only what it shows (already PII-stripped by page_context.finalize) plus
a glossary of its metrics. No retrieval, no vector DB: the whole page context goes into
the system prompt.
"""
import json
import os

import streamlit as st

# Groq model; override with GROQ_MODEL in secrets/env. Groq retires models often, so the
# default was chosen from what the account can actually call (client.models.list()).
GROQ_MODEL = "openai/gpt-oss-120b"
MAX_TURNS = 6  # user+assistant pairs of history sent with each question

# "Try asking" buttons shown above the chat input, per page key.
SUGGESTIONS = {
    "overview": [
        "Why is successful_records lower than total_records?",
        "Which city brings in the most revenue, and is it driven by customer count?",
        "What does the segment split say about our customer base?",
    ],
    "pipeline": [
        "Walk me through what happened to each source.",
        "What does 'balanced' mean and why does it matter?",
        "Why was a support row quarantined?",
    ],
    "lookup": [
        "Summarise this customer and any data quality problems.",
        "Which systems know about this customer, and what is missing?",
        "Should we be worried about this customer?",
    ],
    "quality": [
        "What does the high value, high friction number mean, and what should we do?",
        "Which data quality issue matters most for the business?",
        "What do the order recency numbers tell us?",
    ],
}

SYSTEM_TEMPLATE = """You are an analyst assistant for the Customer 360 dashboard \
(customer data merged from CRM, orders and support systems, with data quality checks).
The user is looking at the "{page_title}" page.

Rules:
- Answer ONLY from the PAGE CONTEXT and GLOSSARY below. If the answer is not in them, \
say that it is not shown on this page (and, if obvious, which page might have it). \
Never invent numbers, customers or causes.
- Be genuinely useful: interpret the numbers. Explain what they mean for the business \
or for data quality, why they matter, and point out notable patterns or outliers.
- Cite the specific metrics and values you used (e.g. "duplicates = 1 in crm").
- Keep it moderate: a few short paragraphs or bullet points. Not a one-liner, not an essay.
- Email addresses are deliberately never given to you (data governance). If asked for one, say that \
emails are withheld from the assistant on purpose and can be seen only by authorised users in the source data.
- The dashboard has exactly four pages: Overview, Pipeline flow, Customer lookup, Quality & risk. \
Only refer to these pages; never invent other pages, reports or data sources.
- When a context contains precomputed figures (e.g. "gap_explained"), use them as given rather \
than re-adding numbers yourself, and never count the same issue twice under two names.
- Tables may be capped: "total_rows" is the full count, "rows" only the first rows shown.

PAGE CONTEXT (JSON):
{context}

GLOSSARY (JSON):
{glossary}
"""


def build_system_prompt(page_title: str, context: dict, glossary: dict) -> str:
    return SYSTEM_TEMPLATE.format(
        page_title=page_title,
        context=json.dumps(context, separators=(",", ":"), default=str),
        glossary=json.dumps(glossary, separators=(",", ":")),
    )


def _setting(name: str):
    """st.secrets[name], else env var. st.secrets raises when no secrets.toml exists."""
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:
        pass
    return os.environ.get(name)


def _get_client():
    """A Groq client, or None when no API key is configured."""
    key = _setting("GROQ_API_KEY")
    if not key:
        return None
    # Imported lazily so the pages still load if the groq package is missing.
    from groq import Groq
    return Groq(api_key=key)


def _stream_answer(messages: list[dict], client=None):
    """Yield the answer text chunk by chunk (Groq stream=True)."""
    client = client or _get_client()
    model = _setting("GROQ_MODEL") or GROQ_MODEL
    extra = {}
    if model.startswith("openai/gpt-oss"):
        # Reasoning model: keep its hidden thinking short so the budget goes to the answer.
        extra["reasoning_effort"] = "low"
    stream = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.2,
        max_tokens=2000,  # includes the reasoning tokens of reasoning models
        stream=True,
        **extra,
    )
    for chunk in stream:
        text = chunk.choices[0].delta.content if chunk.choices else None
        if text:
            yield text


def _friendly_error(e: Exception) -> str:
    if type(e).__name__ == "RateLimitError":
        return "The chat service is busy (rate limit reached). Please wait a moment and try again."
    if type(e).__name__ == "AuthenticationError":
        return "The chat service rejected the API key. Check GROQ_API_KEY."
    if type(e).__name__ == "NotFoundError":
        return "The configured chat model is not available on this Groq account. Set GROQ_MODEL to an available model."
    return "The chat service could not answer right now. Please try again shortly."


def render_chat(page_key: str, page_title: str, context: dict, glossary: dict) -> None:
    """Chat panel; history is kept per page in st.session_state."""
    st.divider()
    st.subheader("Ask about this page")

    client = _get_client()
    if client is None:
        st.info("Chat is unavailable: no GROQ_API_KEY configured")
        return

    history_key = f"chat_history_{page_key}"
    history = st.session_state.setdefault(history_key, [])
    st.caption("Answers use only the data shown on this page. Emails are never sent to the model.")

    box = st.container()  # messages go above the input box
    with box:
        for msg in history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # Suggestion buttons: a click returns True for this run only, like a submitted chat_input.
    clicked = None
    suggestions = SUGGESTIONS.get(page_key, [])
    if suggestions:
        st.caption("Try asking:")
        for i, (col, text) in enumerate(zip(st.columns(len(suggestions)), suggestions)):
            if col.button(text, key=f"suggest_{page_key}_{i}"):
                clicked = text

    # Typed question or clicked suggestion feed the same answer path below.
    typed = st.chat_input("Ask a question about this page", key=f"chat_input_{page_key}")
    question = typed or clicked
    if not question:
        return

    with box:
        with st.chat_message("user"):
            st.markdown(question)
        messages = ([{"role": "system", "content": build_system_prompt(page_title, context, glossary)}]
                    + history[-2 * MAX_TURNS:]
                    + [{"role": "user", "content": question}])
        with st.chat_message("assistant"):
            try:
                answer = st.write_stream(_stream_answer(messages, client))
            except Exception as e:  # API error / rate limit: warn, never crash the page
                st.warning(_friendly_error(e))
                return
    history += [{"role": "user", "content": question}, {"role": "assistant", "content": str(answer)}]
