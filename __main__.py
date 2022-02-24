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
from colorama import Fore, Back, Style
from pprint import pprint as pp



# Loads config
config = dk.tangerine()

#Connects to SQL Database
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                'Server='+(config['database']['Server'])+';'
                                'Database=GCAAssetMGMT;'
                                'UID='+(config['database']['UID'])+';'
                                'PWD='+(config['database']['PWD'])+';')

conn = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
### Notes Formatting ###
assetship= """
{shippingUsed} --- GCA-{asset} --- {STATUS}
Model - {model}
Shipped via {trackingNumber} on {shipDate} to {shipToInsert} at {address}{address2} {zip} {city} GA with historical UPS data showing as {shipmentStatus}."""

noTracking = """
--- GCA-{asset} ---
Model - {model}
No Tracking Information Found"""

gopherLastUsed = """Assigned to {student} and last accessed on {recentSessions} by {mostRecentUser}"""
Program_On = 1
while Program_On == 1:
    STID = input("Enter Student ID or Staff Username: ")
    staff = 0
    if STID[0].isdigit() == False:
        STAID = STID
        findStaffID_query = f"EXEC isolatedSafety.[dbo].[staffUserNameToStaffID] " +str(STID) # Checks Unreturned Equipment in SQL by STID
        findStaffID = pd.read_sql(findStaffID_query , conn)
        STID = findStaffID['Org_ID'].loc[0]
        staff = 1
    if STID == 'X' or STID =='x':
        exit()
    # STID = '1732590 '

    ### 176572 For testing when SCA is in name
    ### 2389755 For testing only showing delivered
    ### 2060437 Testing with Outstanding Equipment
    ### 1732590 Causes Erorr
    print('\n')




    ### Global Variables ###
    conversionDate = datetime.datetime(2020, 2, 5)

    ### Global Lists ###
    list_a = []
    list_b = []
    list_c = []

    ### Global Dictionary ###
    dict_a = {}

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

    ### Functions ###

    def Convert(lst):
        res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
        return res_dct


    def worldShipData(x,y,listingStatus):
        worldShipQuery = f"ExEC GCAAssetMGMT.db_denydatawriter.uspWorldshipAssetLookup " +str(x)
        worldShip = pd.read_sql(worldShipQuery , conn)
        print("World Ship Before filling in Label_Method")
        print(worldShip)
        worldShip['Label_Method'] = worldShip['Label_Method'].fillna("SHIPMENT")
        worldShip['Address2'] = worldShip['Address2'].fillna("")
        worldShip = worldShip.loc[worldShip['Label_Method'] == 'SHIPMENT']
        worldShip.reset_index(drop=True, inplace=True)
        print("World Ship After filling in Label Method")
        print(worldShip)
        if worldShip.empty:
            assetshipprint = noTracking.format(asset=x,model=y)
        else:
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
            shippingUsed = 'WorldShip'
            assetshipprint = assetship.format(model = y,shippingUsed = shippingUsed,asset=x,STATUS = listingStatus,trackingNumber=trackingNumber,shipToInsert=shipToAttention,shipDate=shipDate.date(),address=address,address2=address2,zip=zipCode, city=city,shipmentStatus='Delivered')
        print(assetshipprint)
        list_c.append(assetshipprint)
        list_e.append(assetshipprint)
        print("\n\n\n#################")
        return assetshipprint


    def upsData(x,y,listingStatus):
        upsDataByAssetquery = f"EXEC db_denydatawriter.uspUPSDataByAssetNum " + str(x)
        upsDataByAsset = pd.read_sql(upsDataByAssetquery , conn)
        upsDataByAsset = upsDataByAsset.loc[upsDataByAsset['Status'] == 'Delivered']
        upsDataByAsset['Ship To Address Line 2'] = upsDataByAsset['Ship To Address Line 2'].fillna("")
        upsDataByAsset.reset_index(drop=True, inplace=True)
        shipToName = upsDataByAsset['Ship To Name'].loc[0]
        shipToAttention = upsDataByAsset['Ship To Attention'].loc[0]
        shipDate = upsDataByAsset['Manifest Date'].loc[0]
        if shipDate > conversionDate:
            shipToInsert = shipToName
        else:
            shipToInsert = shipToAttention

        if  shipToInsert != "SCA":
            trackingNumber = upsDataByAsset['Tracking Number'].loc[0]
            shipmentStatus = upsDataByAsset['Status'].loc[0]
            shipDate = upsDataByAsset['Manifest Date'].loc[0]
            shipToAttention = upsDataByAsset['Ship To Attention'].loc[0]
            address = upsDataByAsset['Ship To Address Line 1'].loc[0]
            address2 = upsDataByAsset['Ship To Address Line 2'].loc[0]
            city = upsDataByAsset['Ship To City'].loc[0]
            print("\n")
            shippingUsed = "UPS"
            assetshipprint = assetship.format(model = y,shippingUsed=shippingUsed, asset=x,STATUS = listingStatus,trackingNumber=trackingNumber,shipToInsert=shipToInsert,shipDate=shipDate.date(),address=address,address2=address2,zip="", city=city,shipmentStatus=shipmentStatus)
            print(assetshipprint)
            list_c.append(assetshipprint)
            list_e.append(assetshipprint)
            return assetshipprint
        else:
            worldShipData(x,y,listingStatus)

    def gopherData(x,y):
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
            list_e.append(gopherLastUsedprint)
            return gopherLastUsedprint

    def shipData(x,y,listingStatus):
        upsDataByAssetquery = f"EXEC db_denydatawriter.uspUPSDataByAssetNum " + str(x)
        upsDataByAsset = pd.read_sql(upsDataByAssetquery , conn)
        upsDataByAsset = upsDataByAsset.loc[upsDataByAsset['Status'] == 'Delivered']
        if not upsDataByAsset.empty:
            upsreturn = upsData(x,y,listingStatus)
            gopherLastUsedprint = gopherData(x,y)
        else:
            wsreturn = worldShipData(x,y,listingStatus)
            gopherLastUsedprint = gopherData(x,y)
            
            
    ### Outstanding Assets Search ###
    if not unreturned.empty:
        print("Outstanding assets are as followed.")
        print(unreturned)
        for x,y,z in zip(unreturned['AssetID'], unreturned['Model_Number'], currentassets['Assignment_Timestamp']):
            list_e = []
            listingStatus = "Outstanding Asset"
            list_a.append(x)
            shipData(x,y,listingStatus)
            dict_a[x] = [list_e]
            
                
    ### Current Assets Search ###
    print("Current assets are as followed.")
    print(currentassets)
    for x,y,z in zip(currentassets['AssetID'], currentassets['Model_Number'], currentassets['Assignment_Timestamp']):
        list_e = []
        listingStatus = ""
        if x not in list_a:
            shipData(x,y,listingStatus)
            dict_a[x] = [list_e]

    print("\n\n\n")
    print(Fore.GREEN)
    print('Shipping List Printout:')
    print(Style.RESET_ALL)
    if staff == 1:
        print("Staff Username: ", STAID)
    else:
        print("Student ID: ", STID)
    print(Fore.CYAN)        
    print(*list_c, sep = "\n")
    print(Style.RESET_ALL)
    print("\n")
     
    
    # pp(dict_a)
    
    # for k,v in dict_a.items():
    #     print(k,'--')
    #     pp(v)

    # for k,v in dict_a.items():
    #     print(k,'--')
    #     print(*v, sep = ", ")

   



    # print("\n\nReturned assets are as followed.")
    # print(returnedAssets)

    # for i in range(len(returnedAssets)) :
    #   print("GCA-",returnedAssets.loc[i, "AssetID"]," was returned on", returnedAssets.loc[i, "Assignment_Timestamp"].date()," via Tracking Number", returnedAssets.loc[i,'Tracking'])


