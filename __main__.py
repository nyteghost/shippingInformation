import pandas as pd
import pyodbc
import sys
import os
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
import doorKey as dk
from sqlalchemy import create_engine
import sqlalchemy as sa
import urllib
import datetime

#Notes for updates

# Loads config
config = dk.tangerine()

#Connects to SQL Database
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                'Server='+(config['database']['Server'])+';'
                                'Database=GCAAssetMGMT;'
                                'UID='+(config['database']['UID'])+';'
                                'PWD='+(config['database']['PWD'])+';')

conn = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))


# STID = input("Enter STID: ")
# if STID == 'X' or STID =='x':
#     exit()
STID = '176572'

### 176572 For testing 
### 2389755 For testing only showing delivered
print('\n')


### Notes Formatting ###
assetship= """--- GCA-{asset} ---
Shipped via {trackingNumber} on {shipDate} to {shipToInsert} at {address}{address2} {zip} {city} GA with historical UPS data showing as {shipmentStatus}.
"""
noTracking = """--- GCA-{asset} ---
No Tracking Information Found
"""
gopherLastUsed = """Assigned to {student} and last accessed on {recentSessions} by {mostRecentUser}"""

### Global Variables ###
conversionDate = datetime.datetime(2020, 2, 5)

### Global Lists ###
list_a = []
list_b = []
list_c = []

### Beginning Queries ####
unreturned_query = f"EXEC IsolatedSafety.dbo.[uspFamUnreturnedDevCheck] " + STID # Checks Unreturned Equipment in SQL by STID
unreturned = pd.read_sql(unreturned_query , conn)
currentassetsquery = f"EXEC IsolatedSafety.[dbo].[uspFamCurrentAssignByOrgID] " + STID
currentassets = pd.read_sql(currentassetsquery , conn)
returnsquery = f"db_denydatawriter.uspReturnsUsingLabelsSent2Fam "+ STID
returnedAssets = pd.read_sql(returnsquery , conn)
# familyLookUpquery = f"EXEC db_denydatawriter.uspFamilyLookUp " + STID
# familyLookUp = pd.read_sql(familyLookUpquery , conn)

# ### Generate list of student IDs in Family
# familySTIDS = familyLookUp['StudentID']
# for i in familySTIDS:
#     list_b.append(i)

def shipData(x):
    upsDataByAssetquery = f"EXEC db_denydatawriter.uspUPSDataByAssetNum " + str(x)
    upsDataByAsset = pd.read_sql(upsDataByAssetquery , conn)
    upsDataByAsset = upsDataByAsset.loc[upsDataByAsset['Status'] == 'Delivered']
    upsDataByAsset['Ship To Address Line 2'] = upsDataByAsset['Ship To Address Line 2'].fillna("")
    upsDataByAsset.reset_index(drop=True, inplace=True)
    shipToName = upsDataByAsset['Ship To Name'].loc[0]
    if shipToName != "SCA":
        trackingNumber = upsDataByAsset['Tracking Number'].loc[0]
        shipmentStatus = upsDataByAsset['Status'].loc[0]
        shipDate = upsDataByAsset['Manifest Date'].loc[0]
        shipToAttention = upsDataByAsset['Ship To Attention'].loc[0]
        address = upsDataByAsset['Ship To Address Line 1'].loc[0]
        address2 = upsDataByAsset['Ship To Address Line 2'].loc[0]
        city = upsDataByAsset['Ship To City'].loc[0]
        if shipDate > conversionDate:
            shipToInsert = shipToName
        else:
            shipToInsert = shipToAttention
        print("\n")
        assetshipprint = assetship.format(asset=x,trackingNumber=trackingNumber,shipToInsert=shipToInsert,shipDate=shipDate.date(),address=address,address2=address2,zip="", city=city,shipmentStatus=shipmentStatus).strip()
        print(assetshipprint)
        list_c.append(assetshipprint)
    elif shipToName == 'SCA':
        worldShipQuery = f"ExEC GCAAssetMGMT.db_denydatawriter.uspWorldshipAssetLookup " +str(x)
        worldShip = pd.read_sql(worldShipQuery , conn)
        worldShip['Label_Method'] = worldShip['Label_Method'].fillna("SHIPMENT")
        worldShip['Address2'] = worldShip['Address2'].fillna("")
        worldShip = worldShip.loc[worldShip['Label_Method'] == 'SHIPMENT']
        worldShip.reset_index(drop=True, inplace=True)
        trackingNumber = worldShip['TrackingNumber'].loc[0]
        shipDate = worldShip['Date'].loc[0]
        shipToAttention = worldShip['Attn'].loc[0]
        address = worldShip['Address'].loc[0]
        address2 = worldShip['Address2'].loc[0]
        zipCode = worldShip['Zip'].loc[0]
        city = worldShip['City'].loc[0]
        print("\n\n\n#################")
        print("WorldShip Shipping Data")
        print(x)
        print("\n")
        assetshipprint = assetship.format(asset=x,trackingNumber=trackingNumber,shipToInsert=shipToAttention,shipDate=shipDate.date(),address=address,address2=address2,zip=zipCode, city=city,shipmentStatus='Delivered').strip()
        print(assetshipprint)
        list_c.append(assetshipprint)
        print("\n\n\n#################")
    else: 
        print("\n",noTracking.format(asset=x))
    if y in ('14e Chromebook', 'Chromebook 5400','Chromebook 3400'): 
        gopherquery = f"EXEC db_denydatawriter.uspGophDataByAsset " + str(x) # Checks Gopher Data in SQL by STID
        gopher = pd.read_sql(gopherquery , conn)
        student = gopher['StudentID'].loc[0].strip() + ' ' + gopher['FirstName'].loc[0].strip()
        recentSessions = gopher['Recent Sessions'].loc[0]
        recentSessions = recentSessions.split("\n")[0]
        recentSessions = recentSessions.replace("|","for")
        recentSessions = recentSessions.replace("min","minutes")
        mostRecentUser = gopher['Most Recent User'].loc[0]
        gopherLastUsedprint = gopherLastUsed.format(student=student,recentSessions=recentSessions,mostRecentUser=mostRecentUser)
        print(gopherLastUsedprint)
        list_c.append(gopherLastUsedprint)
        


### Outstanding Assets Search ###
if not unreturned.empty:
    print("Outstanding assets are as followed.")
    print(unreturned)
    for x,y,z in zip(unreturned['AssetID'], unreturned['Model_Number'], currentassets['Assignment_Timestamp']):
        list_a.append(x)
        shipData(x)
            

### Current Assets Search ###
print("Current assets are as followed.")
print(currentassets)
for x,y,z in zip(currentassets['AssetID'], currentassets['Model_Number'], currentassets['Assignment_Timestamp']):
    if x not in list_a:
       shipData(x)
print("\n\n\n")
print('Shipping List Printout')            
for i in list_c:
    print(i)
# print("\n\nReturned assets are as followed.")
# print(returnedAssets)

# for i in range(len(returnedAssets)) :
#   print("GCA-",returnedAssets.loc[i, "AssetID"]," was returned on", returnedAssets.loc[i, "Assignment_Timestamp"].date()," via Tracking Number", returnedAssets.loc[i,'Tracking'])


