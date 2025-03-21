import gspread
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os

credential_path = os.path.dirname(os.path.abspath(__file__)) + '/credentials.json'
gc = gspread.oauth(
credentials_filename=credential_path
)


customers = ['Fiba',
             'Mey Diageo',
             'Alfamart',
             'AFG-RPL',
             'Apparel',
             'Fozzy',
             'Eve',
             'HugoBoss EMEA',
             'HugoBoss APAC',
             'HugoBoss NCSA',
             'CCC',
             'Ebebek',
             'FiveBelow',
             'TailoredBrands Rental',
             'Liwa']

start_date = pd.to_datetime(input("Enter start date (YYYY-MM-DD): "))
end_date = pd.to_datetime(input("Enter end date (YYYY-MM-DD): "))


dailydiary = gc.open_by_key('1xV0Q_gHqXp19uSQ2A2Hp_G2c7xcqrV8wjHdZIXny9hk')
faillogs = gc.open_by_key('16VqMEgBhaZtcTN_k-O3VvDTmmGY3oPWt17YbT1_fcr8')
worksheet = faillogs.worksheet('Fails')
rows = worksheet.get_all_records()
df = pd.DataFrame(rows)
df['Date'] = pd.to_datetime(df.Date)
df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
df = df.drop(df[df['DailyDiary'] == 'FALSE'].index)
df = df.assign(AnyError='1',SecondErrorSource='',SecondErrorDagName='',SecondErrorFailSummary='',SecondErrorFixTime='',
               emptyColumn1='',emptyColumn2='',emptyColumn3='',emptyColumn4='',emptyColumn5='',emptyColumn6='',emptyColumn7='',emptyColumn8='',SecondSeverity='')
df = df[['Customer', 'Date', 'AnyError', 'emptyColumn1', 'emptyColumn2', 'emptyColumn3', 'Source', 'Region', 'Severity', 'Product', 'DagName', 'emptyColumn7', 'FailSummary',
          'emptyColumn4', 'emptyColumn5', 'emptyColumn6', 'SecondErrorSource', 'SecondSeverity', 'SecondErrorDagName', 'emptyColumn8', 'SecondErrorFailSummary', 'FixTime', 'SecondErrorFixTime']]
df['Customer'].loc[(df['Customer'] == 'AFG')] = 'AFG-RPL'
df['Customer'].loc[(df['Customer'] == 'HugoBoss') & (df['Region'] == 'EMEA')] = 'HugoBoss EMEA'
df['Customer'].loc[(df['Customer'] == 'HugoBoss') & (df['Region'] == 'APAC')] = 'HugoBoss APAC'
df['Customer'].loc[(df['Customer'] == 'HugoBoss') & (df['Region'] == 'NCSA')] = 'HugoBoss NCSA'
df["FixTime"] = [pd.to_timedelta(str(i)).total_seconds() / 3600 for i in df["FixTime"]]
df['FixTime'] = df['FixTime'].round(2)


for customer in customers:
    customer_df = df[df['Customer'] == customer].set_index('Date')
    dates = pd.date_range(start=start_date, end=end_date, freq='1d')
    dates = dates.to_frame(name='Date')
    customer_df = dates.join(customer_df, how='left')
    customer_df['Customer'] = customer
    customer_df['AnyError'] = customer_df['AnyError'].fillna('0')
    customer_df = customer_df.fillna('')
    customer_df = customer_df[['Customer', 'Date', 'AnyError', 'emptyColumn1', 'emptyColumn2', 'emptyColumn3', 'Source', 'Severity', 'DagName', 'emptyColumn7', 'FailSummary',
          'emptyColumn4', 'emptyColumn5', 'emptyColumn6', 'SecondErrorSource', 'SecondSeverity', 'SecondErrorDagName', 'emptyColumn8', 'SecondErrorFailSummary', 'FixTime', 'SecondErrorFixTime']]
    customer_df['Date'] = customer_df['Date'].dt.strftime('%Y-%m-%d')
    prev_date = ''
    pass_date = ''
    customer_df = customer_df.reset_index(drop=True)
    for i, row in customer_df.iterrows():
        if (prev_date == row['Date']) & (pass_date != row['Date']):
            customer_df.at[prev_row, 'SecondErrorSource'] = customer_df.at[i, 'Source']
            customer_df.at[prev_row, 'SecondErrorDagName'] = customer_df.at[i, 'DagName']
            customer_df.at[prev_row, 'SecondErrorFailSummary'] = customer_df.at[i, 'FailSummary']
            customer_df.at[prev_row, 'SecondErrorFixTime'] = customer_df.at[i, 'FixTime']
            customer_df.at[prev_row, 'SecondSeverity'] = customer_df.at[i, 'Severity']
            pass_date = row['Date']
            customer_df.drop(i, inplace=True)
        elif (pass_date == row['Date']):
            customer_df.drop(i, inplace=True)
        prev_date = row['Date']
        prev_row = i
    df_values = customer_df.values.tolist()
    dailydiary.values_append('Summary Table', {'valueInputOption': 'RAW'}, {'values': df_values})