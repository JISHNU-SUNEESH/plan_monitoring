import requests
import json
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
# import tasks
from datetime import datetime, timedelta
import streamlit as st
load_dotenv()

access_token=st.secrets["talend_api_token"]
region="eu"
def get_plans_name_df():
    
    plans_name_url=f"https://api.{region}.cloud.talend.com/orchestration/executables/plans/"

    headers={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'

    }

    response=requests.get(plans_name_url, headers=headers)
    if response.status_code == 200:
        plans = response.json()
        print("Plans retrieved successfully.")

    df_pl_name=pd.read_json(StringIO(json.dumps(plans)))
    plans_name=dict(df_pl_name["items"])

    lst_pln_name=[]
    for key,value in plans_name.items():
        lst_pln_name.append(value)

    df_plan_names=pd.DataFrame(lst_pln_name)
    # df_plan_names=df_plan_names[['executable','name']]
    # df_plan_names.rename(columns={"executable":"planId"},inplace=True)

    df_plan_names=df_plan_names[['executable','name','workspace']]
    df_plan_names.rename(columns={"executable":"planId","workspace":"env"},inplace=True)
    df_plan_names['env']=df_plan_names['env'].apply((lambda x:x['environment'])).apply(lambda x: x['name'])

    return df_plan_names
    
def get_plan_status():
    # plans_execution_url=f"https://api.{region}.cloud.talend.com/processing/executables/plans/executions"

    # headers={
    #     'Authorization': f'Bearer {access_token}',
    #     'Content-Type': 'application/json'

    # }

    # response=requests.get(plans_execution_url, headers=headers)
    # if response.status_code == 200:
    #     plans = response.json()
    #     print("Plans retrieved successfully.")

    # df=pd.read_json(StringIO(json.dumps(plans)))
    # plans_dict=dict(df["items"])
    # lst_pln_id=[]
    # for key,value in plans_dict.items():
    #     lst_pln_id.append(value)
    
    limit=100
    offset=0
    all_plans_exec=[]
    while True:
        params={
            'limit':limit,
            'offset':offset,
            "lastDays": "7"
        }
        plans_execution_url=f"https://api.{region}.cloud.talend.com/processing/executables/plans/executions"

        headers={
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'

         }
        response=requests.get(plans_execution_url, headers=headers,params=params)
        data=response.json()
        items=data.get("items")
        if not items:
                break
        all_plans_exec.extend(items)
        offset+=limit   
    
    plan_execution=pd.DataFrame(all_plans_exec)
    return plan_execution

def get_agg_table():
    plan_execution=get_plan_status()
    df_plan_names=get_plans_name_df()
    df_inner=pd.merge(df_plan_names,plan_execution,on="planId",how="left")
    return df_inner

def get_edw_plan_status():

    df_inner=get_agg_table()
    df_inner['startTimestamp']=pd.to_datetime(df_inner['startTimestamp'])
    df_inner['finishTimestamp']=pd.to_datetime(df_inner['finishTimestamp'])
    # df_inner=df_inner[df_inner['planId']!='8cad702c-aea3-44c1-b786-ac68d92d5244']
    edw_plan_latest=df_inner.sort_values(by='finishTimestamp').drop_duplicates(subset=['name','planId'],keep='last')
    return edw_plan_latest




def main():
    st.header("Plan Monitoring")

    if "edw_plan_status" not in st.session_state:
        st.session_state.edw_plan_status = None
        st.session_state.last_refreshed = None

    refresh = st.button("Refresh")

    # Fetch data if refresh is clicked or it's the first load
    if refresh or st.session_state.edw_plan_status is None:
        with st.spinner("Fetching plan details..."):
            edw_plan_status = get_edw_plan_status()
            st.session_state.edw_plan_status = edw_plan_status
            st.session_state.last_refreshed = datetime.now()

    edw_plan_status = st.session_state.edw_plan_status

    if edw_plan_status is not None:
        # Show "Last refreshed" time
        if st.session_state.last_refreshed:
            elapsed = datetime.now() - st.session_state.last_refreshed
            minutes = int(elapsed.total_seconds() // 60)
            seconds = int(elapsed.total_seconds() % 60)

            if minutes > 0:
                st.caption(f"Last refreshed {minutes} min {seconds} sec ago")
            else:
                st.caption(f"Last refreshed {seconds} sec ago")

        st.dataframe(
            edw_plan_status[edw_plan_status['env'] == 'ENV_PRD'][['name', 'status']]
            .dropna()
            .reset_index(drop=True)
        )



if __name__ == "__main__":
    main()
