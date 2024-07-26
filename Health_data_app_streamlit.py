import streamlit as st
import pandas as pd
import polars as pl
from plotly import express as px
import seaborn as sns
import matplotlib.pyplot as plt

# import re
from datetime import date, datetime

import os
# os.chdir("V:/1. R & Python work/Python/2.AnalytixLabs Practice/Health Viny downloaded Github")

# st.title('My Health Test Results Compiled')


############### Setting Configuration ###############

st.set_page_config(page_title="Anaylsis on Health Test Results",
                    layout='wide',
                    initial_sidebar_state="expanded")


# Setting configuration to diable plotly zoom in plots
config = dict({'scrollZoom': False})

st.header("Analytics on Blood Test Reports for Data Insights")
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
    df_pivot = pl.read_parquet(r"df_pivot_r.parquet")
    return df_pivot


df_pivot = load_pivot_data()


@st.cache_resource
def load_melt_data():
    df = pl.read_parquet(r"df_melt_r.parquet")
    return df


df = load_melt_data()


@st.cache_resource
def load_tsh_data():
    tsh_df = pl.read_parquet(r"TSH_only_combined.parquet")
    return tsh_df


TSH_only_combined = load_tsh_data()
############################## GET DATA ##############################





############################## CREATING TEST DATA MAPPING ##############################
test_list = ["BLOOD UREA","BLOOD UREA NITROGEN (BUN)","SERUM CREATININE",
               "SERUM URIC ACID","UREA / CREATININE RATIO","BUN / CREATININE RATIO",
               "INORGANIC PHOSPHORUS","eGFR",
               "SODIUM (Na+)","POTASSIUM (K+)","CHLORIDE(Cl-)",
               "TOTAL CALCIUM (Ca)","IONIZED CALCIUM","NON-IONIZED CALCIUM","pH.",
               "TOTAL CHOLESTEROL SERUM","TRIGLYCERIDES SERUM","HIGH DENSITY LIPOPROTEIN CHOLESTEROL",
               "VERY LOW DENSITY LIPOPROTEIN VLDL","LOW DENSITY LIPOPROTEIN","TOTAL CHOLESTEROL / HDL CHOLESTEROL",
               "LDL / HDL CHOLESTEROL RATIO","NON- HDL CHOLESTEROL","TOTAL LIPID",
               "BILIRUBIN TOTAL","BILIRUBIN DIRECT","BILIRUBIN INDIRECT",
               "PROTEIN TOTAL SERUM","ALBUMIN SERUM","GLOBULIN SERUM","ALBUMIN / GLOBULIN RATIO","SGOT / AST",
               "SGPT / ALT","SGOT/SGPT Ratio","ALKALINE PHOSPHATASE (ALP)",
               "GAMMA GT","LDH",
               "HbA1C (Glycosylated Hemoglobin)","BLOOD GLUCOSE FASTING,Plasma Floride",
               "ESTIMATED AVERAGE PLASMA GLUCOSE",
               "TOTAL TRI IODOTHYRONINE - T3","TOTAL THYROXINE - T4","THYROID STIMULATING HORMONE - TSH",
               "VITAMIN D 25 HYDROXY","SERUM VITAMIN B12",
               "Hemoglobin (Hb)","Red Blood Cell Count (RBC)","RBC Distribution Width (RDW-CV)",
               "RBC Distribution Width (RDW-SD)","Mean Corpuscular Volume (MCV)","Mean Corpuscular Haemoglobin (MCH)",
               "Mean Corpuscular Hb Concentration(MCHC)","Haematocrit / PCV / HCT","Total Leucocyte Count (TLC)",
               "NEUTROPHIL","LYMPHOCYTE","EOSINOPHIL","MONOCYTE","BASOPHIL","ABSOLUTE NEUTROPHIL COUNT(ANC)",
               "ABSOLUTE LYMPHOCYTE COUNT (ALC)","ABSOLUTE EOSINOPHIL COUNT (AEC)","ABSOLUTE MONOCYTE COUNT(AMC)",
               "ABSOLUTE BASOPHIL COUNT","Platelet Count","MPV","PDW","PCT","P-LCR","P-LCC",
               "LIC","ABSOLUTE LIC",
               "Red Blood Cell Count (RBC)","Haematocrit / PCV / HCT",
               "Iron (fe)","UIBC","TIBC","TRANSFERRIN SERUM","% Saturation Transferrin",
               "VOLUME","pH","SPECIFIC GRAVITY","PUS CELLS",
                "MAGNESIUM"] # "GLUCOSE IN URINE",


df_test_mapping = pl.DataFrame({'test' : test_list})

df_test_mapping = df_test_mapping.with_columns(
    pl.when(pl.col('test').is_in(["BLOOD UREA","BLOOD UREA NITROGEN (BUN)","SERUM CREATININE",
               "SERUM URIC ACID","UREA / CREATININE RATIO","BUN / CREATININE RATIO",
               "INORGANIC PHOSPHORUS","eGFR"]))
               .then(pl.lit('KFT'))
               .when(pl.col('test').is_in(["SODIUM (Na+)","POTASSIUM (K+)","CHLORIDE(Cl-)",
               "TOTAL CALCIUM (Ca)","IONIZED CALCIUM","NON-IONIZED CALCIUM","pH.","MAGNESIUM"]))
               .then(pl.lit('Electrolyte Profile'))
               .when(pl.col('test').is_in(["TOTAL CHOLESTEROL SERUM","TRIGLYCERIDES SERUM","HIGH DENSITY LIPOPROTEIN CHOLESTEROL",
               "VERY LOW DENSITY LIPOPROTEIN VLDL","LOW DENSITY LIPOPROTEIN","TOTAL CHOLESTEROL / HDL CHOLESTEROL",
               "LDL / HDL CHOLESTEROL RATIO","NON- HDL CHOLESTEROL","TOTAL LIPID"]))
               .then(pl.lit('Lipid Profile'))
               .when(pl.col('test').is_in(["BILIRUBIN TOTAL","BILIRUBIN DIRECT","BILIRUBIN INDIRECT",
               "PROTEIN TOTAL SERUM","ALBUMIN SERUM","GLOBULIN SERUM","ALBUMIN / GLOBULIN RATIO","SGOT / AST",
               "SGPT / ALT","SGOT/SGPT Ratio","ALKALINE PHOSPHATASE (ALP)",
               "GAMMA GT","LDH"]))
               .then(pl.lit('LFT'))
               .when(pl.col('test').is_in(["HbA1C (Glycosylated Hemoglobin)","BLOOD GLUCOSE FASTING,Plasma Floride","GLUCOSE IN URINE",
               "ESTIMATED AVERAGE PLASMA GLUCOSE"]))
               .then(pl.lit('Diabetes Profile'))
               .when(pl.col('test').is_in(["TOTAL TRI IODOTHYRONINE - T3","TOTAL THYROXINE - T4","THYROID STIMULATING HORMONE - TSH"]))
               .then(pl.lit('Thyroid Profile'))
               .when(pl.col('test').is_in(["VITAMIN D 25 HYDROXY","SERUM VITAMIN B12"]))
               .then(pl.lit('Vitamin Profile'))
               .when(pl.col('test').is_in(["Hemoglobin (Hb)","Red Blood Cell Count (RBC)","RBC Distribution Width (RDW-CV)",
               "RBC Distribution Width (RDW-SD)","Mean Corpuscular Volume (MCV)","Mean Corpuscular Haemoglobin (MCH)",
               "Mean Corpuscular Hb Concentration(MCHC)","Haematocrit / PCV / HCT","Total Leucocyte Count (TLC)"]))
               .then(pl.lit('CBC'))
               .when(pl.col('test').is_in(["NEUTROPHIL","LYMPHOCYTE","EOSINOPHIL","MONOCYTE","BASOPHIL","ABSOLUTE NEUTROPHIL COUNT(ANC)",
               "ABSOLUTE LYMPHOCYTE COUNT (ALC)","ABSOLUTE EOSINOPHIL COUNT (AEC)","ABSOLUTE MONOCYTE COUNT(AMC)",
               "ABSOLUTE BASOPHIL COUNT","Platelet Count","MPV","PDW","PCT","P-LCR","P-LCC",
               "LIC","ABSOLUTE LIC"]))
               .then(pl.lit('DLC'))
               .when(pl.col('test').is_in(["Red Blood Cell Count (RBC)","Haematocrit / PCV / HCT",
               "Iron (fe)","UIBC","TIBC","TRANSFERRIN SERUM","% Saturation Transferrin"]))
               .then(pl.lit('Anemia Profile'))
               .when(pl.col('test').is_in(["VOLUME","pH","SPECIFIC GRAVITY","PUS CELLS"]))
               .then(pl.lit('Urine Examination'))
               .otherwise(pl.lit('Others')).alias('test_category')  
)

############################## CREATING TEST DATA MAPPING ##############################




############################## FIRST FILTER STATE ##############################

st.markdown(
    f'''
        <style>
            .sidebar .sidebar-content {{
                width: 550px;
            }}
        </style>
    ''',
    unsafe_allow_html=True
)

with st.sidebar:

    st.subheader("Single Test Selection Panel For Comparison", divider='rainbow')

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

    Filtered_Data = df_pivot.filter(pl.col('Category') == Test_Selected).to_pandas().reset_index(drop=True)
    Filtered_df = df.filter(pl.col('Category') == Test_Selected)

############################## FILTERED DATA ##############################


# with st.sidebar:    


############################## PLOT ##############################

    plot_data = Filtered_df.to_pandas()


    Lower_range = plot_data.iloc[0,2]

    Upper_range = plot_data.iloc[0,3]


    fig1 = px.line(plot_data, x='Date', y='Value'
            #   ,labels={'status_perf_end_prop_cum':'Cumulative Default'}
              )


    fig2 = (px.scatter(plot_data, 
                  x='Date', y='Value', color = 'Color_flag', color_discrete_sequence=["red", "green"], 
              title = f'{plot_data.iloc[0,0]} Blood Test <br><sup>(For - Vineet)</sup>')
              .add_traces(fig1.data)
              .add_hline(y=Upper_range, line_width=2, line_dash="dash", line_color="red")
              .add_hline(y=Lower_range, line_width=2, line_dash="dash", line_color="red")
              .add_vline(x=date(2023,10,17), line_width=2, line_dash="dash", line_color="grey")
              .update_layout(height=350, legend_title_text='Test Result',legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                          ))
              )



    st.plotly_chart(fig2,use_container_width=True, config = config)

    st.write('Test Report Selected data')
    st.dataframe(Filtered_Data, hide_index=True, width=1800)

# st.divider()

############################## PLOT ##############################





st.subheader('Select Test by Category', divider='rainbow')
############################## CATEGORY PLOT ##############################


Category_Selected = st.selectbox(label="Select Test Category",
                                options = df_test_mapping.select('test_category').unique(maintain_order=True).to_series().to_list(),
                                index = 0)

# filtered_test_list = df_test_mapping.filter(pl.col('test_category') == Category_Selected).select('test').to_series().unique(maintain_order=True).sort().to_list()
filtered_test_list = df_test_mapping.filter(pl.col('test_category') == Category_Selected).sort('test').get_column('test').unique(maintain_order=True).to_list()
Filtered_all_test_polars = df.filter(pl.col('Category').is_in(filtered_test_list)).sort('Category')
Filtered_all_test_df = Filtered_all_test_polars.to_pandas()
Filtered_all_test_df['New_Category'] = Filtered_all_test_df['Category'].str.wrap(10)
Filtered_all_test_df['New_Category'] = Filtered_all_test_df['New_Category'].str.replace("\n","<br>")

Filtered_all_test_df_pivot = df_pivot.filter(pl.col('Category').is_in(filtered_test_list)).sort('Category').to_pandas()

corr_df = Filtered_all_test_df.loc[:,['Category','Value','Date']].pivot(index='Date',columns='Category',values='Value').corr()
corr_df_full = (df.select(['Category','Value','Date'])
 .pivot(index='Date',columns='Category',values='Value')
 .select(pl.exclude('Date'))
 .to_pandas().corr().round(2)
 )



tab1, tab2, tab3, tab4 = st.tabs(["Chart", "Data Table","Correlation Chart","Correlation Table"])

with tab1:
    on = st.toggle("Switch for Multiple Column")

    if Category_Selected in ['LFT','DLC']:
        set_height = 2000
    else:
        set_height = 1100
      
    if not on:
        
        fig_facet_catg_31 = px.line(Filtered_all_test_df, x='Date', y='Value', facet_row="New_Category", 
                            facet_col_wrap=2,facet_row_spacing=0.035)

        fig_facet_catg_3 = (px.scatter(Filtered_all_test_df, 
                        x='Date', y='Value', color = 'Color_flag',
                        title = f"{Category_Selected} Group of Test <br><sup>(Dash Vertical line is beginning of Treatment)</sup><br><sup>(Dash Horizontal line is Test limits)</sup>",  
                        color_discrete_sequence=["red", "green"], facet_row="New_Category"
                        , facet_row_spacing=0.035 # ,facet_col_wrap=2
                        #   ,title = f'{plot_data.iloc[0:3,0]}<br><sup>(For - Vineet)</sup>'
                    )
                    .add_traces(fig_facet_catg_31.data)
                    .add_vline(x=date(2023,10,17), line_width=2, line_dash="dash", line_color="grey")
                    .update_yaxes(matches=None,showticklabels=True)
                    .update_layout(height=set_height, legend_title_text='Test Result'
                                #    ,legend=dict(
                                #     orientation="h",
                                #     yanchor="bottom",
                                #     y=1.02,
                                #     xanchor="right",
                                #     x=1)
                                  )
                    .for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
                    )
        
        n_categories = len(filtered_test_list)
        for i,elem in enumerate(filtered_test_list):
            data = Filtered_all_test_polars.filter(pl.col('Category')==elem)
            upper = data.item(0,"Upper Range")
            lower = data.item(0,"Lower Range")
            # st.write(i, elem, upper, lower)

            fig_facet_catg_3 = (fig_facet_catg_3.add_hline(y=upper, row=n_categories-i, line_dash="dash",line_color="red")
                                                .add_hline(y=lower, row=n_categories-i, line_dash="dash",line_color="red"))



        st.plotly_chart(fig_facet_catg_3,use_container_width=True, config = config)
    else:
        fig_facet_catg_1 = px.line(Filtered_all_test_df, x='Date', y='Value', facet_col="New_Category", 
                            facet_col_wrap=2,facet_row_spacing=0.035)
    # .update_yaxes(autorange="reversed")

        fig_facet_catg_2 = (px.scatter(Filtered_all_test_df, 
                        x='Date', y='Value', color = 'Color_flag', 
                        title = f"{Category_Selected} Group of Test <br><sup>(Dash Vertical line is beginning of Treatment)</sup>", 
                        color_discrete_sequence=["red", "green"], facet_col="New_Category"
                        ,facet_col_wrap=2, facet_row_spacing=0.035
                        #   ,title = f'{plot_data.iloc[0:3,0]}<br><sup>(For - Vineet)</sup>'
                    )
                    .add_traces(fig_facet_catg_1.data)
                    .add_vline(x=date(2023,10,17), line_width=2, line_dash="dash", line_color="grey")
                    .update_yaxes(matches=None,showticklabels=True)
                    .update_layout(height=700, legend_title_text='Test Result'
                                #    ,legend=dict(
                                #     orientation="h",
                                #     yanchor="bottom",
                                #     y=1.02,
                                #     xanchor="right",
                                #     x=1)
                                  )
                    .update_xaxes(matches='x')
                    .for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
                    )
        
        st.plotly_chart(fig_facet_catg_2,use_container_width=True, config = config)

with tab2:
    st.dataframe(Filtered_all_test_df_pivot, hide_index=True, width=1800)

with tab3:
    sns.set(font_scale = .4)
    fig_heat,ax = plt.subplots()
    sns.heatmap(corr_df, ax=ax, annot=True, cmap = "Blues").set_title(f'Correlation Heatmap within {Category_Selected} Group Test')
    st.write(fig_heat)

with tab4:
    st.write(corr_df, hide_index=True, width=1800)

############################## CATEGORY PLOT ##############################






############################## CORRELATION SELECTION ##############################

st.subheader('Test Correlation Selection', divider='rainbow')
Corr_Test_Selected = st.selectbox(label="Select Correlation Test",
                                  options = Test_List,
                                  index = Test_index)


corr_selected = corr_df_full[corr_df_full.index.isin([Corr_Test_Selected])].melt()
corr_selected['absolute_value'] = corr_selected.value.abs()
corr_selected = corr_selected.sort_values('absolute_value',ascending=False)
corr_selected = corr_selected.drop('absolute_value', axis=1)

tab_corr1, tab_corr2 = st.tabs(["Chart","Data Table"])

with tab_corr1:
    corr_plot_data = (corr_df_full[corr_df_full.index.isin([Corr_Test_Selected])]
                        .reset_index()
                        .melt(id_vars='index')
                        .drop('index',axis=1))

    corr_plot_data = corr_plot_data.reindex(corr_plot_data.value.abs().sort_values(ascending=False).index)

    corr_plot_data = (corr_plot_data[~corr_plot_data.variable.isin([Corr_Test_Selected])]
                        .reset_index(drop=True)
                        .query('value > .6 | value < -.6 '))
    
    fig_corr = (px.scatter(corr_plot_data, x = 'value', y = 'variable', text = 'value',
                           title = f"Top Positive/Negative Correlation for [{Corr_Test_Selected}] with Other Test",
                           labels={
                     "variable": "Correlated Blood Test",
                     "value": "Correlation Value (-ve/+ve)"
                 }).add_vline(x=0, line_width=1, line_dash="dash", line_color="red")
                    .update_yaxes(autorange="reversed").update_layout(height=800)
                    .update_traces(textposition="top center"))

    st.plotly_chart(fig_corr,use_container_width=True, config = config)

with tab_corr2:
    st.dataframe(corr_selected) # ,'LYMPHOCYTE'



############################## CORRELATION SELECTION ##############################






st.subheader('Thyroid(TSH) only timeline data', divider='rainbow')
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
              .update_layout(height=350, legend_title_text='Test Result',legend=dict(
                              orientation="h",
                              yanchor="bottom",
                              y=1.02,
                              xanchor="right",
                              x=1
                          ))
              )


st.plotly_chart(fig22,use_container_width=True, config = config)
# st.divider()

############################## TSH PLOT ##############################






############################## RAW DATA ##############################

st.subheader('Complete Raw Timelined Blood Test Data', divider='rainbow')
st.dataframe(df_pivot.to_pandas(), hide_index=True, width=1800)
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

fig_diff = px.bar(df_difference_bef_aftr.to_pandas(), y='Category',x='Diff_%', height = 1400,
       title="Mean Difference in test Before & After Treatment").update_yaxes(autorange="reversed")

st.subheader('% Difference(+ve/-ve) in Test Values Before & After Treatment', divider='rainbow')
st.plotly_chart(fig_diff,use_container_width=True, config = config)
st.divider()
############################## TEST DIFF PLOT ##############################




############################## MY INSIGHTS / OBSERVATIONS FROM DATA ##############################

st.subheader('MY INSIGHTS / OBSERVATIONS FROM DATA', divider='rainbow')

st.write("\n1. **Thyroid - TSH** is continuously **high** except in 2 instance causing **Hypothyroidism**.    \n\
          \n2. **T4** is looking fine but **T3** is at `low level` across the time indicating **liver** needs correction as T4 to T3 conversion is lacking somwhere.   \n\
          \n3. **Hypothyroidism** is causing changes in the **Lipids**.\n\
          \n4. **Lymphocytes** are also continuosly `high` and showing **correlation** with **T4** (this article suggest correlation https://www.ncbi.nlm.nih.gov/pmc/articles/PMC9010816/).   \n\
         \n5. **Chlorine** is also at `higher range` continuosly and **cholride** is also responsible for **iodine** absorbtion cutoff which can also lead to **Hypothyroidism**.   \n\
         \n6. **Cholride** shows **correlation** with **lymphocytes** (correlation research report proof https://pubmed.ncbi.nlm.nih.gov/7687269/).    \n\
         \n7. **TSH** shows very high +ve correlation with **Hba1c** and -ve correlation with **Bilrubin Indirect** which indicates it can lead to or already leading to    \
         **Diabetes** and `low` **Bilrubin Production** and it shows in `increasing` **hba1c** over time and `falling` **Bilrubin indirect** but `abnormal increase` in **Bilrubin direct** indicating **liver** damage.    \
         This can lead to **stones** in the **Gall Bladder** in Future.    \n\
         \n8. `Low Levels` of **Potassium** can be causing issues of **Insulin Resistance** as **Low Potassium** leads to `low` **Insulin Production** (as per this article: https://www.webmd.com/diabetes/potassium-diabetes).    \n\
         \n9. `Low Levels` of **Potassium & Sodium** recently indicates **dehydration** which can be the reason of weight loss. This shows electrolytes imbalance and also has `low` **Calcium**. All of this indicating possible    \
         `low levels` of **Magnesium** in the body too.    \n\
         \n10. `Bad Levels` of **eGFR** already shows **Kidney** impact indicating **dehydration** or **higher protein** or both")

############################## MY INSIGHTS / OBSERVATIONS FROM DATA ############################## 
