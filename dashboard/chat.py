"""'Ask about this page' chat panel (Groq), rendered at the bottom of every dashboard page.

Each page passes only what it shows (already PII-stripped by page_context.finalize) plus
a glossary of its metrics. No retrieval, no vector DB: the whole page context goes into
the system prompt.
"""
import json
import os

import streamlit as st

# Groq production model (console.groq.com/docs/models); override with GROQ_MODEL in secrets/env.
GROQ_MODEL = "llama-3.3-70b-versatile"
MAX_TURNS = 6  # user+assistant pairs of history sent with each question

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
- Do not ask for or reveal email addresses; they are deliberately not provided.
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
    stream = client.chat.completions.create(
        model=_setting("GROQ_MODEL") or GROQ_MODEL,
        messages=messages,
        temperature=0.2,
        max_tokens=700,
        stream=True,
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

    question = st.chat_input("Ask a question about this page", key=f"chat_input_{page_key}")
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
