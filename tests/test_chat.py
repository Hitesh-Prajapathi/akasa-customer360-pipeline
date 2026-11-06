"""Per-page chatbot: PII stripping, prompt grounding, context size, and the render path.

No network: the Groq client is always faked.
"""
import json
import os
import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from conftest import DATA_DIR, ROOT, write_config
from streamlit.testing.v1 import AppTest

sys.path.insert(0, os.path.join(ROOT, "dashboard"))
import chat  # noqa: E402
import page_context as pc  # noqa: E402

from customer360.pipeline import run  # noqa: E402

TOKEN_BUDGET = 6000
PAGES = ["dashboard/app.py", "dashboard/pages/1_Pipeline_Flow.py",
         "dashboard/pages/2_Customer_Lookup.py", "dashboard/pages/3_Quality_and_Risk.py"]


# ---------- data: a fresh pipeline run in a temp dir ----------

@pytest.fixture(scope="module")
def data(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("chat")
    cfg_path = write_config(tmp, DATA_DIR)
    run(cfg_path)
    cfg = json.loads(open(cfg_path).read())
    out = os.path.join(cfg["output_dir"])

    def report(name):
        with open(os.path.join(out, "reports", f"{name}.json")) as f:
            return json.load(f)

    def parquet(layer, name):
        return pd.read_parquet(os.path.join(out, layer, f"{name}.parquet"))

    # Same shaping as dashboard/common.load_customer_360
    c360 = parquet("curated", "customer_360")
    c360["city"] = c360["city"].fillna("Unknown")
    seg = pd.DataFrame(report("customer_segments"), columns=["customer_id", "segment"])
    c360 = c360.merge(seg, on="customer_id", how="left")
    c360["segment"] = c360["segment"].fillna("Unknown")
    return SimpleNamespace(cfg=cfg, c360=c360, report=report, parquet=parquet)


def all_contexts(d):
    """Every page's context, built with the same functions the pages call."""
    cfg, c360 = d.cfg, d.c360
    issues = d.report("dq_issues")
    raw = {s: d.parquet("raw", s) for s in ["crm", "orders", "support"]}
    id_cols = {"crm": "customer_id", "orders": "cust_id", "support": "customerId"}
    row = c360[c360["customer_id"] == "C0250"].iloc[0]
    matches = {s: df[df[id_cols[s]].astype(str).str.strip() == "C0250"] for s, df in raw.items()}
    issues_df = pd.DataFrame(issues)
    related = issues_df[issues_df["customer_id"].eq("C0250")]
    return {
        "overview": pc.overview_context(c360, d.report("dq_report"), issues, cfg, []),
        "pipeline": pc.pipeline_context(d.report("dq_report"), cfg,
                                        {"source": "crm", "layer": "raw", "data": raw["crm"]}),
        "lookup": pc.lookup_context(row, matches, related),
        "quality": pc.quality_context(c360, issues,
                                      {s: d.parquet("quarantine", s) for s in ["crm", "orders", "support"]}, cfg),
    }


# ---------- PII ----------

def test_strip_pii_drops_email_always_and_name_unless_allowed():
    obj = {"email": "a@b.com", "name": "Ann", "city": "Pune",
           "rows": [{"customer_id": "C0001", "email": "x@y.com", "name": "Bob"}],
           "note": "contact z@q.org"}
    out = pc.strip_pii(obj)
    assert "email" not in out and "name" not in out and "name" not in out["rows"][0]
    assert "@" not in json.dumps(out)
    kept = pc.strip_pii(obj, allow_names=True)
    assert kept["name"] == "Ann" and kept["rows"][0]["name"] == "Bob" and "email" not in kept

    df = pd.DataFrame([{"customer_id": "C1", "name": "Ann", "email": "a@b.com"}])
    assert list(pc.strip_pii(df).columns) == ["customer_id"]


def test_page_contexts_never_contain_emails_and_names_only_on_lookup(data):
    ctxs = all_contexts(data)
    emails = [e for e in data.c360["email"].dropna() if "@" in e]
    names = list(data.c360["name"].dropna())
    for page, ctx in ctxs.items():
        text = json.dumps(ctx)
        assert '"email"' not in text, page
        assert not any(e in text for e in emails), page
        if page != "lookup":
            assert '"name"' not in text, page
            assert not any(f'"{n}"' in text for n in names), page
    lookup = ctxs["lookup"]
    assert lookup["customer_profile"]["name"]  # names are allowed on the lookup page
    assert "email" not in lookup["customer_profile"]


def test_contexts_fit_token_budget(data):
    for page, ctx in all_contexts(data).items():
        assert pc.approx_tokens(ctx) < TOKEN_BUDGET, page


def test_overview_context_respects_city_filter(data):
    city = data.c360["city"].iloc[0]
    df = data.c360[data.c360["city"] == city]
    ctx = pc.overview_context(df, data.report("dq_report"), data.report("dq_issues"), data.cfg, [city])
    assert ctx["city_filter"] == [city]
    assert {r["city"] for r in ctx["revenue_by_city"]} == {city}
    assert ctx["customers_in_view"] == len(df)


# ---------- prompt ----------

def test_system_prompt_has_context_glossary_and_grounding_rule(data):
    ctx = all_contexts(data)["overview"]
    prompt = chat.build_system_prompt("Overview", ctx, pc.glossary_for("overview", data.cfg))
    assert json.dumps(ctx["data_quality"]["kpis"], separators=(",", ":")) in prompt
    assert "Answer ONLY from the PAGE CONTEXT" in prompt
    assert "not shown on this page" in prompt
    assert "reconciliation" in prompt and "Gold: lifetime value >" in prompt


def test_every_page_glossary_term_is_defined(data):
    for page in pc.PAGE_TERMS:
        g = pc.glossary_for(page, data.cfg)
        assert g and all(v for v in g.values())


# ---------- streaming with a fake client ----------

def fake_client(parts=None, error=None):
    def create(**kwargs):
        if error:
            raise error
        assert kwargs["stream"] is True and kwargs["model"]
        return [SimpleNamespace(choices=[SimpleNamespace(delta=SimpleNamespace(content=p))]) for p in parts]
    return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))


def test_stream_answer_yields_text_chunks():
    out = list(chat._stream_answer([{"role": "user", "content": "hi"}], fake_client(["Hello", None, " world"])))
    assert out == ["Hello", " world"]


# ---------- pages via AppTest ----------

@pytest.fixture
def no_key(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.chdir(ROOT)


@pytest.mark.parametrize("page", PAGES)
def test_pages_render_without_key(no_key, page):
    at = AppTest.from_file(os.path.join(ROOT, page), default_timeout=120).run()
    assert not at.exception, at.exception
    assert any("Chat is unavailable: no GROQ_API_KEY configured" in i.value for i in at.info)


def test_chat_answer_renders_with_mocked_client(monkeypatch):
    monkeypatch.chdir(ROOT)
    monkeypatch.setenv("GROQ_API_KEY", "dummy")
    monkeypatch.setattr(chat, "_get_client", lambda: fake_client(["Duplicates = 1 ", "in crm."]))
    at = AppTest.from_file(os.path.join(ROOT, PAGES[1]), default_timeout=120).run()
    assert not at.exception
    at.chat_input[0].set_value("What is quarantined?").run()
    assert not at.exception
    text = " ".join(m.value for msg in at.chat_message for m in msg.markdown)
    assert "What is quarantined?" in text and "Duplicates = 1 in crm." in text
    assert len(at.session_state["chat_history_pipeline"]) == 2


def test_api_error_shows_warning_not_crash(monkeypatch):
    class RateLimitError(Exception):
        pass

    monkeypatch.chdir(ROOT)
    monkeypatch.setattr(chat, "_get_client", lambda: fake_client(error=RateLimitError("429")))
    at = AppTest.from_file(os.path.join(ROOT, PAGES[1]), default_timeout=120).run()
    at.chat_input[0].set_value("hello").run()
    assert not at.exception
    assert any("rate limit" in w.value for w in at.warning)
