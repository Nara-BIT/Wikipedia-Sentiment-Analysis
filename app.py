import streamlit as st
import pandas as pd
import psycopg2
import time

# Page Setup
st.set_page_config(page_title="Wikipedia Sentiment Live", page_icon="🌍", layout="wide")
st.title("🌍 Wikipedia Live Edit Sentiment Analysis")
st.markdown("Watching real-time human edits across Wikipedia and scoring their sentiment using NLP.")

# Fetch Data
def get_data():
    try:
        conn = psycopg2.connect(
            host="localhost", database="voting_db", user="user", password="password", port="5432"
        )
        df = pd.read_sql("SELECT title, \"user\", comment, sentiment_label, sentiment_score FROM wiki_sentiment", conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame()

df = get_data()

# Build the UI
if not df.empty:
    
    # --- 🔥 NEW: Extreme Sentiments Section ---
    st.markdown("### 🔥 Extreme Sentiments of the Session")
    col_pos, col_neg = st.columns(2)
    
    # Find the most positive comment
    most_positive = df.loc[df['sentiment_score'].idxmax()]
    with col_pos:
        st.success(f"**Most Positive Edit (+{most_positive['sentiment_score']:.2f})**")
        st.write(f"👤 **{most_positive['user']}** edited *{most_positive['title']}*")
        st.write(f"💬 \"{most_positive['comment']}\"")

    # Find the most negative comment
    most_negative = df.loc[df['sentiment_score'].idxmin()]
    with col_neg:
        st.error(f"**Most Negative Edit ({most_negative['sentiment_score']:.2f})**")
        st.write(f"👤 **{most_negative['user']}** edited *{most_negative['title']}*")
        st.write(f"💬 \"{most_negative['comment']}\"")
        
    st.divider() # A nice visual break line

    # --- Original Visuals ---
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("📊 Sentiment Breakdown")
        sentiment_counts = df['sentiment_label'].value_counts().reset_index()
        sentiment_counts.columns = ['Sentiment', 'Count']
        st.bar_chart(data=sentiment_counts, x='Sentiment', y='Count', color="#4B8BBE")
        
    with col2:
        st.subheader("📝 Live Edit Feed")
        st.dataframe(df.tail(15), use_container_width=True, hide_index=True)
        
else:
    st.info("⏳ Waiting for data to arrive from Spark... (Make sure your producer and Spark job are running!)")

# Auto-refresh every 2 seconds
time.sleep(2)
st.rerun()