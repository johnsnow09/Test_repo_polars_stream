import streamlit as st
import pandas as pd
import polars as pl
from plotly import express as px

# import re
from datetime import date, datetime

# st.title('My Health Test Results Compiled')


############### Setting Configuration ###############

st.set_page_config(page_title="My Health Test Results Compiled2",
                    layout='wide',
                    initial_sidebar_state="expanded")


# Setting configuration to diable plotly zoom in plots
config = dict({'scrollZoom': False})

############### Setting Configuration Ends ###############




############### Custom Functions ###############

# from: https://discuss.streamlit.io/t/how-to-add-extra-lines-space/2220/7
def v_spacer(height, sb=False) -> None:
    for _ in range(height):
        if sb:
            st.sidebar.write('\n')
        else:
            st.write('\n')

############### Custom Functions Ends ###############




############################## GET DATA ##############################

@st.cache_resource
def load_pivot_data():
    df_pivot = pl.read_parquet(r"df_pivot.parquet")
    return df_pivot


df_pivot = load_pivot_data()


@st.cache_resource
def load_melt_data():
    df = pl.read_parquet(r"df_melt.parquet")
    return df


df = load_melt_data()


@st.cache_resource
def load_tsh_data():
    tsh_df = pl.read_parquet(r"TSH_only_combined.parquet")
    return tsh_df


TSH_only_combined = load_tsh_data()
############################## GET DATA ##############################




############################## FIRST FILTER STATE ##############################

with st.sidebar:

    st.write("This is Selection Control Panel")

    # State_List = df.lazy().select(pl.col('State')).unique().collect().to_series().to_list()
    # State_List = df.collect().to_pandas()["State"].unique().tolist()

    @st.cache_data
    def get_test_list():
        # return get_data().select('State').unique().to_series().to_list()
        return df_pivot.select('Category').unique(maintain_order=True).to_series().to_list()
    
    Test_List = get_test_list()
    Test_index = Test_List.index('VITAMIN D 25 HYDROXY')
    # st.write(State_index)

    Test_Selected = st.selectbox(label="Select Test by Name",
                                  options = Test_List,
                                  index = Test_index)
    

############################## FIRST FILTER STATE DONE ##############################




############################## FILTERED DATA ##############################

    Filtered_Data = df_pivot.filter(pl.col('Category') == Test_Selected)
    Filtered_df = df.filter(pl.col('Category') == Test_Selected)

############################## FILTERED DATA ##############################



st.subheader('Test Report Selected data', divider='rainbow')
st.dataframe(Filtered_Data.to_pandas(), width=1800)


############################## PLOT ##############################
plot_data = Filtered_df.to_pandas()

Lower_range = plot_data.iloc[0,2]
Upper_range = plot_data.iloc[0,3]

fig1 = px.line(plot_data, x='Date', y='Value'
            #   ,labels={'status_perf_end_prop_cum':'Cumulative Default'}
              )

fig2 = (px.scatter(plot_data, 
                  x='Date', y='Value', color = 'Color_flag', color_discrete_sequence=["red", "green"], 
              title = f'{plot_data.iloc[0,0]} Blood Test <br><sup>(Patient - Vineet)</sup>')
              .add_traces(fig1.data)
              .add_hline(y=Upper_range, line_width=2, line_dash="dash", line_color="red")
              .add_hline(y=Lower_range, line_width=2, line_dash="dash", line_color="red")
              .add_vline(x=date(2023,10,17), line_width=2, line_dash="dash", line_color="grey")
              )


st.plotly_chart(fig2,use_container_width=True, config = config)
# st.divider()

############################## PLOT ##############################



st.subheader('TSH only timeline data', divider='rainbow')
############################## TSH PLOT ##############################
plot_data2 = TSH_only_combined.to_pandas()
Lower_range2 = plot_data2.iloc[0,2]
Upper_range2 = plot_data2.iloc[0,3]

fig21 = px.line(plot_data2, x='Date', y='Value'
            #   ,labels={'status_perf_end_prop_cum':'Cumulative Default'}
              )

fig22 = (px.scatter(plot_data2, 
                  x='Date', y='Value', color = 'Color_flag', color_discrete_sequence=["red", "green"], 
              title = 'TSH Blood Test <br><sup>(With More Test Results)</sup>')
              .add_traces(fig21.data)
              .add_hline(y=Upper_range2, line_width=2, line_dash="dash", line_color="red")
              .add_hline(y=Lower_range2, line_width=2, line_dash="dash", line_color="red")
              .add_vline(x=date(2023,10,17), line_width=2, line_dash="dash", line_color="grey")
              )


st.plotly_chart(fig22,use_container_width=True, config = config)
# st.divider()

############################## TSH PLOT ##############################




############################## RAW DATA ##############################

st.subheader('Complete Raw Timelined Blood Test Data', divider='rainbow')
st.dataframe(df_pivot.to_pandas(), width=1800)
# st.divider()
############################## RAW DATA ##############################




############################## TEST DIFF PLOT ##############################
df_difference_bef_aftr = (df
               .with_columns(pl.when(pl.col('Date') < date(2023,10,17))
               .then(pl.lit("Before")).otherwise(pl.lit("After")).alias("Treatment_status"))
               .group_by(['Category','Treatment_status']).agg(pl.col('Value').mean())
               .pivot(index='Category',columns='Treatment_status',values='Value')
               .with_columns(((pl.col('After')-pl.col('Before'))*100/pl.col('Before')).alias('Diff_%'))
               .with_columns(pl.col('Diff_%').abs().alias('Diff_%_abs'))
               .filter(pl.col('Diff_%').is_not_null())
               .filter(pl.col('Diff_%').is_not_nan())
               .sort(pl.col('Diff_%'),descending=True)
        )

fig_diff = px.bar(df_difference_bef_aftr.to_pandas(), y='Category',x='Diff_%', height = 800,
       title="Mean Difference in test Before & After Treatment").update_yaxes(autorange="reversed")

st.subheader('% Difference in Test Before & After Treatment', divider='rainbow')
st.plotly_chart(fig_diff,use_container_width=True, config = config)
st.divider()
############################## TEST DIFF PLOT ##############################
