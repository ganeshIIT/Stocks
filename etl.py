import numpy as np
import pandas as pd
import pyodbc

import helper
import dataloader


# Extract from SQL server


def get_data_from_sql(query):       
    return helper.get_dataframe_from_sqlserver_query(query=query)


# Transformation begins
def tranform_data(df_original):
    df_bookings = (df_original
                .assign(
                        ProgramDuration = lambda _df: _df['ProgramDuration'].fillna(_df['ProgramDuration'].median()).astype('int16'),
                        DaysToCAX = lambda _df: (_df['DateCaxed'] - _df['DateBooked']).dt.days.astype('Int64'),
                        WeekNum = lambda _df: _df['WeekBooked'].astype('str').str[-2:].astype('int8'),
                        hasCaxed = lambda _df: _df['hasCaxed'].astype('bool'),
                        hasBooked = lambda _df: _df['hasBooked'].astype('bool'),
                    )
                .rename(columns={'Booking': 'BookingId'})
    )

    df_booked = (df_bookings
        .rename(columns={'DateBooked': 'MainDate'})
        .assign(
            MainDate = lambda _df: _df['MainDate'].dt.normalize(),
            RankBin = lambda _df: pd.cut(df_bookings['OriginalRanking'], bins = [0,4,5,10], labels=['1-4', '5', '6-10'],),
        )
        .drop(['OriginalRanking'], axis = 1)
        
    )

    df_booked= pd.get_dummies(df_booked, columns=['MethodOfCreation', 'RankBin'], prefix = {'MethodOfCreation':'', 'RankBin':'Rank'}, prefix_sep = '')

    df_booked = df_booked.groupby(['MainDate', 'MarketName', 'SalesRepID', 'SalesRepName', 'WeekBooked', 'WeekNum',
        'SortOrder','FiscalYear',
        ])[['hasBooked', 'hasCaxed', 'ProgramDuration', 'EnteredByUser', 'Excel Import', 'Website', 'Rank1-4', 'Rank5','Rank6-10']].sum()


    df_caxed = (df_bookings[df_bookings['hasCaxed']==True]
        .rename(columns={'DateBooked': 'MainDate'})
        .assign(
            MainDate = lambda _df: _df['MainDate'].dt.normalize(),
            RankBin = lambda _df: pd.cut(df_bookings['OriginalRanking'], bins = [0,4,5,10], labels=['1-4', '5', '6-10'],),
        )
        .drop(['OriginalRanking'], axis = 1)
        
    )
    df_caxed= pd.get_dummies(df_caxed, columns=['MethodOfCreation', 'RankBin'], prefix = {'MethodOfCreation':'', 'RankBin':'Rank'}, prefix_sep = '')

    df_caxed = (df_caxed.groupby(['MainDate', 'MarketName', 'SalesRepID', 'SalesRepName', 'WeekBooked', 'WeekNum',
        'SortOrder','FiscalYear',
        ])[['hasBooked', 'hasCaxed', 'ProgramDuration', 'EnteredByUser', 'Excel Import', 'Website', 'Rank1-4', 'Rank5','Rank6-10']].sum()
                .drop(['hasBooked', 'hasCaxed'], axis=1)
                )



    df_merged = (df_booked
                    .merge (df_caxed, left_index=True, right_index=True, how='left', suffixes=('', '_cax'))
                    .assign(
                        ProgramDuration_cax = lambda _df: _df['ProgramDuration_cax'].fillna(0).astype('int16'),
                        EnteredByUser_cax = lambda _df: _df['EnteredByUser_cax'].fillna(0).astype('int16'),      
                        **{'Excel Import_cax' : lambda _df: _df['Excel Import_cax'].fillna(0).astype('int16')},    
                        Website_cax = lambda _df: _df['Website_cax'].fillna(0).astype('int16'),
                        **{'Rank1-4_cax' : lambda _df: _df['Rank1-4_cax'].fillna(0).astype('int16')},
                        **{'Rank5_cax' : lambda _df: _df['Rank5_cax'].fillna(0).astype('int16')},
                        **{'Rank6-10_cax' : lambda _df: _df['Rank6-10_cax'].fillna(0).astype('int16')},   
                    )
                    .rename(columns={'Excel Import_cax': 'ExcelImport_cax', 'Excel Import': 'ExcelImport'})
                    ).reset_index()
    
    return df_merged

# Transformation Ends - final data is df_merged


# Loading the data to SQL server

def etl_Data(query):
    print("Extracting data from SQL Server...")
    df = get_data_from_sql(query)
    print(f'Extracted {df.shape[0]} rows')
    print()
    
    print('Transforming Data...')
    data = tranform_data(df)
    print('Transformation complete')
    print()
    
    print('Loading Data to SQL Server...')
    dataloader.full_load(df=data, tbl="DailySalesData", hasindex=False, custom = {"id": "INT PRIMARY KEY",
                                                                                  "MainDate": "DATE"
          })
    print("Done: Full loaded DailySalesData to SQL Server")
    print()