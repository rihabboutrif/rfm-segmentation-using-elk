# app_rfm_dashboard_predef_alerts.py
import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd

# ---------------- CONFIG -----------------
ES_URL = "http://localhost:9200"
INDEX_NAME = "ecommerce_customers"
es = Elasticsearch(ES_URL)

# ---------- Helpers ----------
def es_search(body):
    return es.search(index=INDEX_NAME, body=body)

def agg_avg_rating():
    q = {"size": 0, "aggs": {"avg_rating": {"avg": {"field": "Average Rating"}}}}
    return es_search(q)["aggregations"]["avg_rating"]["value"]

def agg_total_customers():
    q = {"size": 0}
    res = es_search(q)
    return res["hits"]["total"]["value"] if isinstance(res["hits"]["total"], dict) else res["hits"]["total"]

def agg_unsatisfied_count():
    q = {
        "size": 0,
        "query": {"term": {"Satisfaction_Level.keyword": "Unsatisfied"}},
        "aggs": {"count_unsat": {"value_count": {"field": "Customer ID"}}}
    }
    return es_search(q)["aggregations"]["count_unsat"]["value"]

def agg_count_by(field_keyword):
    q = {"size": 0, "aggs": {"by_field": {"terms": {"field": field_keyword, "size": 100}}}}
    res = es_search(q)
    buckets = res["aggregations"]["by_field"]["buckets"]
    return {b["key"]: b["doc_count"] for b in buckets}

def agg_metric_by_group(metric_field, agg_type, group_field):
    q = {
        "size": 0,
        "aggs": {
            "group_by": {
                "terms": {"field": group_field, "size": 100},
                "aggs": {"metric": {agg_type: {"field": metric_field}}}
            }
        }
    }
    res = es_search(q)
    buckets = res["aggregations"]["group_by"]["buckets"]
    return {b["key"]: b["metric"]["value"] for b in buckets}

# ---------- RFM ----------
def get_percentiles():
    q = {
        "size": 0,
        "aggs": {
            "recency_pct": {"percentiles": {"field": "Days Since Last Purchase", "percents": [20,40,60,80]}},
            "frequency_pct": {"percentiles": {"field": "Items Purchased", "percents": [20,40,60,80]}},
            "monetary_pct": {"percentiles": {"field": "Total Spend", "percents": [20,40,60,80]}}
        }
    }
    res = es_search(q)["aggregations"]
    return {"r": res["recency_pct"]["values"], "f": res["frequency_pct"]["values"], "m": res["monetary_pct"]["values"]}

def compute_rfm_segments():
    pct = get_percentiles()
    r_cut = pct["r"]
    f_cut = pct["f"]
    m_cut = pct["m"]

    script = f"""
        double r = doc['Days Since Last Purchase'].value;
        double f = doc['Items Purchased'].value;
        double m = doc['Total Spend'].value;

        int R = (r <= {r_cut['20.0']} ? 5 :
                r <= {r_cut['40.0']} ? 4 :
                r <= {r_cut['60.0']} ? 3 :
                r <= {r_cut['80.0']} ? 2 : 1);

        int F = (f <= {f_cut['20.0']} ? 1 :
                f <= {f_cut['40.0']} ? 2 :
                f <= {f_cut['60.0']} ? 3 :
                f <= {f_cut['80.0']} ? 4 : 5);

        int M = (m <= {m_cut['20.0']} ? 1 :
                m <= {m_cut['40.0']} ? 2 :
                m <= {m_cut['60.0']} ? 3 :
                m <= {m_cut['80.0']} ? 4 : 5);

        int code = R*100 + F*10 + M;

        if (code >= 544) return 'Champions';
        else if (R == 5 || (R == 4 && F >= 4)) return 'Loyal Customers';
        else if (F == 5) return 'Frequent Buyers';
        else if (M == 5) return 'Big Spenders';
        else if (R >= 4 && F <= 2) return 'Potential Loyalists';
        else if (R == 2) return 'At Risk';
        else return 'Hibernating';
    """

    q = {"size": 0, "aggs": {"rfm_segments": {"terms": {"script": script, "size": 10}}}}
    res = es_search(q)
    buckets = res["aggregations"]["rfm_segments"]["buckets"]
    return {b["key"]: b["doc_count"] for b in buckets}

# ---------- Alerts ----------
def check_alerts():
    alerts = []
    total = agg_total_customers()
    avg_rating = agg_avg_rating() or 0.0
    unsat = agg_unsatisfied_count()
    unsat_pct = (unsat / total * 100) if total else 0

    if avg_rating < 3.5:
        alerts.append(("Low average rating", f"Average rating is {avg_rating:.2f} (< 3.5)"))

    if unsat_pct > 10:
        alerts.append(("High unsatisfied rate", f"{unsat_pct:.1f}% of customers are unsatisfied (>10%)"))

    segs = compute_rfm_segments()
    at_risk = segs.get("At Risk", 0)
    if total and (at_risk / total * 100) > 15:
        alerts.append(("Large At-Risk group", f"{at_risk} customers ({at_risk/total*100:.1f}%) are At Risk (>15%)"))

    return alerts

# ---------- Streamlit ----------
st.set_page_config(page_title="ELK RFM Dashboard + Alerts", layout="wide")
st.title("📊 ELK — RFM Dashboard with Predefined Questions & Alerts")

# Questions prédéfinies
PREDEFINED_QUERIES = {
    "Average age by membership": lambda: agg_metric_by_group("Age", "avg", "Membership_Type.keyword"),
    "Total spend by membership": lambda: agg_metric_by_group("Total Spend", "sum", "Membership_Type.keyword"),
    "Average rating by gender": lambda: agg_metric_by_group("Average Rating", "avg", "Gender.keyword"),
    "Average items purchased by membership": lambda: agg_metric_by_group("Items Purchased", "avg", "Membership_Type.keyword"),
    "Count of customers by satisfaction level": lambda: agg_count_by("Satisfaction_Level.keyword"),
}

# Sidebar
st.sidebar.header("Select a predefined question")
question = st.sidebar.selectbox("Question:", [""] + list(PREDEFINED_QUERIES.keys()) + ["Show alerts"])

# Action
if question:
    if question == "Show alerts":
        alerts = check_alerts()
        if not alerts:
            st.success("No alerts detected.")
        else:
            for title, msg in alerts:
                st.error(f"{title}: {msg}")
    else:
        st.subheader(f"Results for: {question}")
        data = PREDEFINED_QUERIES[question]()
        if isinstance(data, dict):
            df = pd.Series(data).sort_values(ascending=False)
            st.bar_chart(df)
            st.table(pd.DataFrame({"Key": df.index, "Value": df.values}))
        else:
            st.write(data)

# Dashboard KPIs
st.header("Dashboard Overview")
tot = agg_total_customers()
avg_r = agg_avg_rating() or 0.0
unsat_c = agg_unsatisfied_count()
k1, k2, k3 = st.columns(3)
k1.metric("Total customers", tot)
k2.metric("Average rating", f"{avg_r:.2f}")
k3.metric("Unsatisfied customers", unsat_c)

# RFM Segments
st.subheader("RFM Segments")
segments = compute_rfm_segments()
s = pd.Series(segments).sort_values(ascending=False)
st.bar_chart(s)
st.table(pd.DataFrame({"Segment": s.index, "Count": s.values}))
