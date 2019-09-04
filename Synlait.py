# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 14:37:48 2019

@author: KatieSi

This program will calculate a few summary statistics on the consents supplied by Synlait.
It calculates the annual volume on each WAP. It also calculates the number of 
days with at least partial data for each WAP. 

Known Issues:
    1. Spikes are not removed. 
2. Days with a little as one time stamp are considored full data days.
"""
##############################################
# Import packages
import numpy as np
import pandas as pd
import pdsql


###############################################
# Set Parameters

# Date Parameters
TelemetryFromDate = '2014-07-01'
TelemetryToDate = '2019-06-30'


###############################################
# Import Consent Numbers

# Import Lists of Synlait member consents
SynlaitSW = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\SynlaitSurfacewaterTake.csv")
SynlaitGW = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\SynlaitGroundwaterTake.csv")

# Create list of target consents
SynlaitSW = SynlaitSW[['ConsentNo']]
SynlaitGW = SynlaitGW[['ConsentNo']]
SynlaitConsents = pd.concat([SynlaitSW,SynlaitGW],ignore_index=True)
SynlaitConsents = list(set(SynlaitConsents['ConsentNo'].values.tolist()))


##############################################
# Find WAPS on consents

# Import consent WAP relationships
WAPCol = [
        'WAP',
        'RecordNumber',
        'Activity'
        ]
WAPColNames = {
        'RecordNumber' : 'ConsentNo'
        }
WAPImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNumber' : SynlaitConsents
        }
WAPwhere_op = 'AND'
WAPServer = 'SQL2012Prod03'
WAPDatabase = 'DataWarehouse'
WAPTable = 'D_ACC_Act_Water_TakeWaterWAPAllocation'

WAP = pdsql.mssql.rd_sql(
                   server = WAPServer,
                   database = WAPDatabase, 
                   table = WAPTable,
                   col_names = WAPCol,
                   where_op = WAPwhere_op,
                   where_in = WAPImportFilter
                   )

# Reshape data
WAP.rename(columns=WAPColNames, inplace=True)
WAP['ConsentNo'] = WAP['ConsentNo'].str.strip().str.upper()
WAP['Activity'] = WAP['Activity'].str.strip().str.lower()
WAP['WAP'] = WAP['WAP'].str.strip().str.upper()

# Aggregate to unique consent-WAP combinations
Allocation = WAP.groupby(
        ['ConsentNo','WAP'], as_index = False
        ).agg({
                'Activity' : 'count'
                })

# Create list of target WAPS
WAPMaster = list(set(Allocation['WAP'].values.tolist()))


##########################################
# Find Telemetery data on each WAP

# Import daily usage data from hydro
HydroUsageCol = [
        'ExtSiteID',
        'DatasetTypeID',
        'DateTime',
        'Value'
        ]
HydroUsageColNames = {
        'ExtSiteID' : 'WAP',
        'Value' : 'DailyVolume'
        }
HydroUsageImportFilter = {
       'DatasetTypeID' : ['9','12'],
       'ExtSiteID' : WAPMaster
        }
HydroUsage_date_col = 'DateTime'
HydroUsage_from_date = TelemetryFromDate
HydroUsage_to_date = TelemetryToDate
HydroUsageServer = 'EDWProd01'
HydroUsageDatabase = 'Hydro'
HydroUsageTable = 'TSDataNumericDaily'

HydroUsage = pdsql.mssql.rd_sql(
                   server = HydroUsageServer,
                   database = HydroUsageDatabase, 
                   table = HydroUsageTable,
                   col_names = HydroUsageCol,
                   where_in = HydroUsageImportFilter,
                   date_col = HydroUsage_date_col,
                   from_date= HydroUsage_from_date,
                   to_date = HydroUsage_to_date
                   )

# Reshape data
HydroUsage.rename(columns=HydroUsageColNames, inplace=True)
HydroUsage['WAP'] = HydroUsage['WAP'] .str.strip().str.upper()


##############################################
# Calculate summary statistics

# Create counter for days with partial data
HydroUsage['Day'] = 1

# Reformat DateTime to Financial Years
HydroUsage['time'] = pd.to_datetime(HydroUsage['DateTime'], errors='coerce')
HydroUsage['year'] = HydroUsage['time'].dt.strftime('%Y').astype(int)
HydroUsage['month'] = HydroUsage['time'].dt.strftime('%m').astype(int)
HydroUsage['FY'] = np.where(HydroUsage['month'] < 7, 
            HydroUsage['year']-1, HydroUsage['year'])

# Sum daily usage and data days over a year for each WAP
SynlaitUsage = HydroUsage.groupby(
        ['WAP','FY'], as_index=False
        ).agg(
                {
                'DailyVolume' : 'sum',
                'Day' : 'count'                
                })

# Reshape data
SynlaitUsage.rename(columns = 
         {
          'DailyVolume' :'AnnualVolume',
          'Day' : 'DaysOfData'
         },inplace=True)

##########################################
# Prepare final export

# Attach consent number
SynlaitUsage = pd.merge(SynlaitUsage, Allocation, on = 'WAP', how = 'left')

# Select columns to export
SynlaitUsage = SynlaitUsage[['ConsentNo','WAP','FY','AnnualVolume','DaysOfData']]

# Export data
SynlaitUsage.to_csv('SynlaitUsage.csv')

