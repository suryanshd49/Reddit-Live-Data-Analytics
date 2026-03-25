import streamlit as st
import pandas as pd
import json, os, altair as alt
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Reddit Live Analytics", layout="wide")
st_autorefresh(interval=3000, key="reddit_refresh")

st.title("📊 Live Reddit Analytics Dashboard")

TOP_FILES = {
    "subreddits": "./analytics/top_subreddits.json",
    "authors": "./analytics/top_authors.json",
    "words": "./analytics/top_words.json",
    "comments": "./analytics/most_commented.json",
    "hours": "./analytics/active_hours.json"
}

def load_json(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        with open(path, "r") as f:
            return pd.DataFrame(json.load(f))
    except:
        return pd.DataFrame()

tabs = st.tabs(["🏠 Overview", "📈 Subreddits", "🧑 Authors", "🗣️ Words", "💬 Comments", "⏰ Active Hours"])

# Overview tab
with tabs[0]:
    df_sub = load_json(TOP_FILES["subreddits"])
    df_auth = load_json(TOP_FILES["authors"])
    df_words = load_json(TOP_FILES["words"])
    total_posts = int(df_sub["post_count"].sum()) if not df_sub.empty else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Posts", f"{total_posts}")
    col2.metric("Top Subreddit", df_sub.iloc[0]["subreddit"] if not df_sub.empty else "—")
    col3.metric("Top Author", df_auth.iloc[0]["author"] if not df_auth.empty else "—")

# Subreddit tab
with tabs[1]:
    df = load_json(TOP_FILES["subreddits"])
    if not df.empty:
        st.subheader("Top Subreddits by Post Count")
        c = alt.Chart(df).mark_bar().encode(
            x=alt.X("subreddit", sort='-y'),
            y="post_count",
            color="avg_score"
        )
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No subreddit data yet.")

# Authors
with tabs[2]:
    df = load_json(TOP_FILES["authors"])
    if not df.empty:
        st.subheader("Top Authors by Post Activity")
        st.bar_chart(df.set_index("author")["author_count"].head(10))
    else:
        st.info("No author data yet.")

# Words
with tabs[3]:
    df = load_json(TOP_FILES["words"])
    if not df.empty:
        st.subheader("Most Frequent Words in Titles")
        st.bar_chart(df.set_index("word")["word_count"].head(20))
    else:
        st.info("No word data yet.")

# Most commented
with tabs[4]:
    df = load_json(TOP_FILES["comments"])
    if not df.empty:
        st.subheader("Top Commented Posts")
        st.dataframe(df[["title", "subreddit", "num_comments", "url"]].head(10))
    else:
        st.info("No comment data yet.")

# Active hours
with tabs[5]:
    df = load_json(TOP_FILES["hours"])
    if not df.empty:
        st.subheader("Active Hours (UTC)")
        c = alt.Chart(df).mark_line(point=True).encode(x="hour:O", y="posts:Q")
        st.altair_chart(c, use_container_width=True)
    else:
        st.info("No hourly data yet.")
