import oracledb
import getpass
import pandas as pd 
import random 
import plotly.graph_objects as go

username = "READ_ONLY"
dsn = "vestadwhprod_high"
pw = "RDly20232Q2023"

# wallet_pw = getpass.getpass("Enter the wallet password: ")
wallet_pw="^&$#7aBcdeFgHiJkLmNpQrStUvWxYz@!%"
conn = oracledb.connect(
    user=username,
    password=pw,
    dsn=dsn,
    config_dir=r'C:\Users\Guest1\Desktop\New_Wallet_VestaDWHProd',
    wallet_location=r'C:\Users\Guest1\Desktop\New_Wallet_VestaDWHProd',
    wallet_password=wallet_pw
)

print(conn.version)

# ---------------------------------------------------------------------------------------------------
def get_x_axis_measure(x):
    if 'Prospectación' in x:
        return random.uniform(0, 1)
    if 'Prospecto' in x:
        return random.uniform(0, 1)
    if 'RFP' in x:
     return random.uniform(0, 1) + 1
    if 'Propuesta' in x:
     return random.uniform(0, 1) + 2
    if 'Formalización / Elaboración de contrato' in x:
     return random.uniform(0, 1) + 3
    if 'Formalización de compra' in x:
     return random.uniform(0, 1) + 3
    if 'Cierre' in x:
     return random.uniform(0, 1) + 4


def normalize_data_size(x, MIN_GLA, MAX_GLA, size=40):
    z = x / MAX_GLA
    return (z * size) + 20
    # +5
def get_color(x):
    try:
        if 'NORTE' in x.upper():
            return '#000078'
        if 'BAJIO' in x.upper():
            return '#FA5A45'
        if 'CENTRO' in x.upper():
         return '#3ED7E6'
        else:
            return '#97999B'
    except:
         return '#97999B'
     
        
     
        

qry = """ SELECT OPTYID, ALLOWANCE,MONTOALLOWANCE,
        LEASETECHNICALEXHIBITS,ROC,SEED,TISUSM,
        TISUSPSF from admin.crm_rfp_view"""

qry2 = """ SELECT OPTYID,OPTYCREATIONDATE, NAME,STATUSPHASE,TARGETCLOSING,PROSPECT,
            BUILDING,BUILDINGID,COUNTERPARTYRISKAPPROVALS,
            COUNTERPARTYRISK,DEALTYPE,LASTCONTACT,LEASECLOSED,
            LOIEXECUTION,REGION,ESTADO,MARKET,PARK,RESPONSIBLEOFFICER,
            TERM,TRANSACTIONTIME,TRANSACTIONNAME 
            FROM admin.CRM_OPPORTUNITY_VIEW
            WHERE STATUSCODE = 'OPEN'"""


qry3 = """ SELECT OPTYID, GLASF, RENTCOSTPERSF from admin.crm_opportunity_revenue_view"""

qry4 = """SELECT ORGANIZATIONPROFILEID, PARTYNUMBER from admin.crm_account"""

qry5 = """SELECT PARTY_NUMBER, PARTY_NAME from admin.erp_parties"""

qry6 = """SELECT SUM(gla_sq_ft) FROM admin.unifier_building_view"""


crm_rfp = pd.read_sql_query(qry, conn)
crm_op = pd.read_sql_query(qry2, conn)
crm_rev = pd.read_sql_query(qry3, conn)
crm_ac = pd.read_sql_query(qry4, conn)
erp_par = pd.read_sql_query(qry5, conn)
total_gla = pd.read_sql_query(qry6, conn)

crm_rfp['OPTYID'] = crm_rfp['OPTYID'].astype(str)
crm_op['OPTYID'] = crm_op['OPTYID'].astype(str)
crm_rev['OPTYID'] = crm_rev['OPTYID'].astype(str)
crm_op['LOIEXECUTION'] = pd.to_datetime(crm_op['LOIEXECUTION'], utc=True)
crm_op['LOIEXECUTION']  = crm_op['LOIEXECUTION'].dt.tz_localize(None)


crm_rev = crm_rev.dropna()

crm_rfp = crm_rfp.drop_duplicates()
crm_op = crm_op.drop_duplicates()
crm_rev = crm_rev.drop_duplicates()


crm_1 = pd.merge(crm_op, crm_rfp, how='left', on='OPTYID')
crm_2 = pd.merge(crm_1, crm_rev, how='left', on='OPTYID')
crm_3 = pd.merge(crm_2, crm_ac, how='left', left_on='PROSPECT', right_on='ORGANIZATIONPROFILEID')
crm_4 = pd.merge(crm_3, erp_par, how='left', left_on='PARTYNUMBER', right_on='PARTY_NUMBER')

crm_4['DAYS_TO_TARGET'] = crm_4['TARGETCLOSING'] - pd.Timestamp.today()
crm_4['DAYS_TO_LASTCONTACT'] = pd.Timestamp.today() - crm_4['LASTCONTACT'] 
crm_4['DAYS_TO_LOI'] = pd.Timestamp.today() - crm_4['LOIEXECUTION'] 
crm_4['DAYS_ALIVE'] = pd.Timestamp.today() - crm_4['OPTYCREATIONDATE'] 
crm_4['IMPACT_VACANCY'] = crm_4['GLASF'].apply(lambda x:(x / total_gla.iloc[0]) * 100)



columnas_sorted = ['TRANSACTIONNAME','DEALTYPE','REGION', 'MARKET', 'PARK',
                   'BUILDING', 'PARTY_NAME', 'LEASECLOSED', 'STATUSPHASE',
                   'COUNTERPARTYRISK', 'COUNTERPARTYRISKAPPROVALS', 'ALLOWANCE',
                   'LEASETECHNICALEXHIBITS', 'GLASF', 'IMPACT_VACANCY','TERM',
                   'RENTCOSTPERSF','TISUSM', 'TISUSPSF',
                   'ROC', 'SEED', 'LOIEXECUTION', 'DAYS_TO_LOI','LASTCONTACT',
                   'DAYS_TO_LASTCONTACT','OPTYCREATIONDATE', 'DAYS_ALIVE',
                   'TARGETCLOSING', 'DAYS_TO_TARGET']


crm_final = crm_4[columnas_sorted]


TARGET = 120
MAX_GLA = crm_final['GLASF'].max()
MIN_GLA = 0


    
crm_final['GLASF_FULL'] = crm_final['GLASF'].fillna(0)
crm_final['BUILDING'] = crm_final['BUILDING'].fillna('')   


crm_final['x_axis'] = crm_final['STATUSPHASE'].apply(lambda x: get_x_axis_measure(x))
# ---------------------------------------------------------------------------
# crm_final['y_axis'] = crm_final['DAYS_ALIVE'].apply(lambda x: x)
# crm_final['DAYS_ALIVE'] = pd.to_datetime(crm_final['DAYS_ALIVE'], errors='coerce')
#  applying the lambda function
# crm_final['y_axis'] = crm_final['DAYS_ALIVE'].dt.day
crm_final['y_axis'] = crm_final['DAYS_ALIVE'].apply(lambda x: x.days)
# ----------------------------------------------------------------------------

crm_final['size'] = crm_final['GLASF_FULL'].apply(lambda x: normalize_data_size(x, MIN_GLA, MAX_GLA))
crm_final['color'] = crm_final['REGION'].apply(lambda x: get_color(x))
crm_final['text'] = crm_final.apply(lambda x: """Oportunidad: {} <br>Prospecto: {} <br>Edificio: {} <br>Región: {} <br>GLA Sft: {:} <br>Creation Date: {}""".format(x['TRANSACTIONNAME'],
                                    x['PARTY_NAME'], x['BUILDING'], x['REGION'],
                                    x['GLASF'], x['OPTYCREATIONDATE']),axis=1)

crm_final['y_axis_var'] = crm_final['y_axis'].apply(lambda x: TARGET + (TARGET * 0.1 ) if x > TARGET else x)


crm_final.to_pickle('df_crm.pkl', protocol=4)  

print(crm_final['DAYS_ALIVE'],crm_final['x_axis'],crm_final['size'])
df = pd.DataFrame(crm_final)

# Save the DataFrame to an Excel file
# df.to_excel('Crm_data.xlsx', index=False)

# ploting a graph 

fig = go.Figure()


fig.add_trace(go.Scatter(x=crm_final['x_axis'], y=crm_final['y_axis_var'],
                    mode='markers',
                    marker=dict(size=crm_final['size'],
                                color=crm_final['color']),
                    hovertext=crm_final['text'],
                    hoverinfo="text"))

fig.add_shape(type="line",
    xref="paper", yref="paper",
    x0=0, y0=0, x1=5,
    y1=5,
    line=dict(
        color="gray",
        width=2,
    ),
)

fig.add_vrect(x0=0, x1=1, 
              annotation_text="PROSPECTO", annotation_position="bottom right",
              fillcolor="white", opacity=0.25, line_width=0)

fig.add_vrect(x0=1, x1=2, 
              annotation_text="RFP", annotation_position="bottom right",
              fillcolor="blue", opacity=0.08, line_width=0)

fig.add_vrect(x0=2, x1=3, 
              annotation_text="PROPUESTA", annotation_position="bottom right",
              fillcolor="white", opacity=0.25, line_width=0)
fig.add_vrect(x0=3, x1=4, 
              annotation_text="FORMALIZACION", annotation_position="bottom right",
              fillcolor="blue", opacity=0.08, line_width=0)
fig.add_vrect(x0=4, x1=5, 
              annotation_text="CIERRE", annotation_position="bottom right",
              fillcolor="white", opacity=0.25, line_width=0)
# fig.update_xaxes(visible=False)

fig.update_layout(yaxis_title='Número de Días',
                    xaxis_title='Etapa')

# fig.update_traces(marker_size=10)
# print([{'name': i, 'id': i, 'deletable': False} for i in crm_final.columns])

fig.show()
