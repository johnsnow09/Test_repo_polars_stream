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



st.subheader('Test data')
st.dataframe(Filtered_Data, width=1800)


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
              )


st.plotly_chart(fig2,use_container_width=True, config = config)


############################## PLOT ##############################


st.write('Raw Timelined Blood Test Data')
st.dataframe(df_pivot, width=1800)

