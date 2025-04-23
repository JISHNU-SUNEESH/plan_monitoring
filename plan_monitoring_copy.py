import requests
import json
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
from datetime import datetime
import streamlit as st

# Load environment variables (only used locally)
load_dotenv()

# Secrets from Streamlit Cloud or local secrets.toml
access_token = st.secrets["talend_api_token"]
region = "eu"

# ------------------------- API FUNCTIONS ------------------------- #
def get_plans_name_df():
    plans_name_url = f"https://api.{region}.cloud.talend.com/orchestration/executables/plans/"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(plans_name_url, headers=headers)
    plans = response.json() if response.status_code == 200 else {}
    df_pl_name = pd.read_json(StringIO(json.dumps(plans)))
    plans_name = dict(df_pl_name["items"])
    lst_pln_name = [value for key, value in plans_name.items()]
    df_plan_names = pd.DataFrame(lst_pln_name)
    df_plan_names = df_plan_names[['executable', 'name', 'workspace']]
    df_plan_names.rename(columns={"executable": "planId", "workspace": "env"}, inplace=True)
    df_plan_names['env'] = df_plan_names['env'].apply(lambda x: x['environment']['name'])
    return df_plan_names

def get_plan_status():
    limit = 100
    offset = 0
    all_plans_exec = []
    while True:
        params = {'limit': limit, 'offset': offset, "lastDays": "7"}
        plans_execution_url = f"https://api.{region}.cloud.talend.com/processing/executables/plans/executions"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(plans_execution_url, headers=headers, params=params)
        data = response.json()
        items = data.get("items")
        if not items:
            break
        all_plans_exec.extend(items)
        offset += limit
    return pd.DataFrame(all_plans_exec)

def get_agg_table():
    return pd.merge(get_plans_name_df(), get_plan_status(), on="planId", how="left")

def get_edw_plan_status():
    df = get_agg_table()
    df['startTimestamp'] = pd.to_datetime(df['startTimestamp'])
    df['finishTimestamp'] = pd.to_datetime(df['finishTimestamp'])
    df = df.sort_values(by='finishTimestamp').drop_duplicates(subset=['name', 'planId'], keep='last')
    return df

# ------------------------- MAIN APP ------------------------- #
def main():
    st.set_page_config("Talend Plan Monitor", layout="wide")
    st.title("Talend Plan Monitoring Dashboard")

    # Initialize session state
    if "edw_plan_status" not in st.session_state:
        st.session_state.edw_plan_status = None
        st.session_state.last_refreshed = None

    # Refresh button
    col1, col2,col3 = st.columns([1, 5])
    with col1:
        refresh = st.button("🔄 Refresh")
    with col2:
        last_refresh=st.button("🕒 Fetch Last Refresh")
    with col3:
        if st.session_state.last_refreshed and last_refresh:
            elapsed = datetime.now() - st.session_state.last_refreshed
            minutes = int(elapsed.total_seconds() // 60)
            seconds = int(elapsed.total_seconds() % 60)
            time_str = f"{minutes} min {seconds} sec" if minutes else f"{seconds} sec"
            st.caption(f"Last refreshed {time_str} ago")

    # Fetch data
    if refresh or st.session_state.edw_plan_status is None:
        with st.spinner("Fetching plan details..."):
            st.session_state.edw_plan_status = get_edw_plan_status()
            st.session_state.last_refreshed = datetime.now()

    df = st.session_state.edw_plan_status
    df_filtered = df[df['env'] == 'ENV_PRD'][['name', 'status']].dropna().reset_index(drop=True)

    # Display styled dataframe with color logic
    def style_df(df):
        def style_status(val):
            status = str(val).strip().lower()
            if status == "execution_successful":
                return "background-color: #d4edda; color: #155724; font-weight: bold; text-align: center"
            elif status == "execution_failed":
                return "background-color: #f8d7da; color: #721c24; font-weight: bold; text-align: center"
            else:
                return "background-color: #d1ecf1; color: #0c5460; font-weight: bold; text-align: center"

        def style_name(val):
            return "background-color: #f5f5f5; color: #333333; font-weight: 600; text-align: left"

        styled = df.style.applymap(style_status, subset=['status'])
        styled = styled.applymap(style_name, subset=['name'])
        return styled

    st.subheader("✅ ENV_PRD Plan Status")
    st.dataframe(
        style_df(df_filtered),
        use_container_width=True
    )

if __name__ == "__main__":
    main()
