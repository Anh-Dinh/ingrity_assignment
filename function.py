import azure.functions as func
import pandas as pd
from datetime import datetime
import pyodbc
from io import StringIO

def main(myblob: func.InputStream):
    # Read the CSV file
    csv_content = myblob.read().decode('utf-8')
    df = pd.read_csv(StringIO("sample_data.csv"), sep='\t')  # Note the tab separator

    # Normalize MovementDateTime to ISO format
    df['MovementDateTime'] = pd.to_datetime(df['MovementDateTime'], format='%m/%d/%Y %H:%M').dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Fill missing or zero speeds with average speed for each CallSign
    def fill_speed(group):
        mask = (group['MoveStatus'] == 'Under way using engine') & ((group['Speed'] == 0) | group['Speed'].isnull())
        avg_speed = group.loc[group['Speed'] > 0, 'Speed'].mean()
        group.loc[mask, 'Speed'] = avg_speed
        return group

    df = df.groupby('CallSign').apply(fill_speed).reset_index(drop=True)

    # Create BeamRatio feature
    df['BeamRatio'] = df['Beam'] / df['Length']

    # Store the cleaned data in Azure SQL Server
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=.database.windows.net;'
                          'DATABASE=;'
                          'UID=;'
                          'PWD=')

    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute("""
        INSERT INTO your_table_name (MovementDateTime, CallSign, Speed, Beam, Length, MoveStatus, BeamRatio)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, row['MovementDateTime'], row['CallSign'], row['Speed'], row['Beam'], row['Length'], row['MoveStatus'], row['BeamRatio'])

    conn.commit()
    conn.close()

    print(f"Processed {len(df)} rows and stored in Azure SQL Server")

    