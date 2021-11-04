# -*- coding: utf-8 -*-
"""
Created on Jul 28 2020


Assumption
1. We now have a folder called OUTDIR/ZIPCODE/ with all the relevent files generated from
   filter_data.py

"""

# MACHINERY TO WORK WITH ZTRAX DATA
############################################################################################################
import pandas as pd
import os, glob
import sys
import argparse
import logging
import pickle
import numpy as np
import csv

def CreateDir(dirName, clean = False):
    try:
        # Create target Directory
        os.mkdir(dirName)
        logging.debug("Directory " + dirName +  " Created ")
    except FileExistsError:
        if clean:
            os.system("rm -rf " + dirName + "/*.*")
        logging.debug("Directory " + dirName +  " already exists")

def Get_AllZipCodes(outdir):
    # Property Zipcode in ZAssmt is 29 (indexed at 0)
    ofile = os.path.join(outdir, "Zipcodes.pkl")
    logging.debug("Generating All Zipcodes")
    if os.path.exists(ofile):
        logging.debug("Found " + ofile + "Skipping reading the archive, loading the pickle file")
        df = pd.read_pickle(ofile)
        return list(df)
    logging.debug("Please run extract_data.py to generate zipcodes")
    sys.exit(-1)

def Load_DF(outdir, FileName):
    outpath = os.path.join(outdir, FileName)
    mdf = pd.read_pickle(outpath)
    print(mdf)
    return mdf

def clean_data(outdir, csvdir, zipcode):
    # STEP 1
    # Load all the relevent DF for this zipcode
    outdirPath = os.path.join(outdir, str(zipcode))
    csvdirPath = os.path.join(csvdir, str(zipcode))
    CreateDir(csvdirPath)
    za_main = Load_DF(outdirPath, "ZAsmt.Main.txt.pkl")
    za_pool = Load_DF(outdirPath, "ZAsmt.Pool.txt.pkl")
    za_bldg = Load_DF(outdirPath, "ZAsmt.Building.txt.pkl")
    za_sale = Load_DF(outdirPath, "ZAsmt.SaleData.txt.pkl")
    za_gar  = Load_DF(outdirPath, "ZAsmt.Garage.txt.pkl")
    zt_prop = Load_DF(outdirPath, "ZTrans.PropertyInfo.txt.pkl")
    zt_main = Load_DF(outdirPath, "ZTrans.Main.txt.pkl")

    #########################################
    #STEP 10: Run this block of code to select variables and reformat some variables
    #########################################

    #Select the variables of interest from the different ZA and ZT tables
    za_main_keep = za_main[['RowID',
                            'FIPS',
                           'ImportParcelID',
                           'UnformattedAssessorParcelNumber',
                           'County',
                           'PropertyFullStreetAddress',
                           'PropertyCity',
                           'PropertyState',
                           'PropertyZip',
                           'CensusTract',
                           'LotSizeAcres',
                           'TaxAmount',
                           'TaxYear',
                           'NoOfBuildings',
                           'PropertyAddressLatitude',
                           'PropertyAddressLongitude',
                           'PropertyAddressCensusTractAndBlock']]

    za_bldg_keep = za_bldg[['RowID',
                            'BuildingOrImprovementNumber',
                           'TotalRooms',
                           'TotalBedrooms',
                           'TotalCalculatedBathCount',
                           'TotalActualBathCount',
                           'YearBuilt',
                           'EffectiveYearBuilt',
                           'FireplaceFlag',
                           'NoOfStories',
                           'PropertyLandUseStndCode',
                           'BuildingQualityStndCode',
                           'BuildingConditionStndCode']]

    za_sale_keep = za_sale[['RowID',
                            'SaleSeqNum',
                            'SalesPriceAmount',
                            'SalesPriceAmountStndCode',
                            'RecordingDate',
                            'DocumentDate']]

    za_gar_keep = za_gar[['RowID',
                          'BuildingOrImprovementNumber',
                          'GarageStndCode',
                          'GarageAreaSqFt',
                          'GarageNoOfCars']]

    #Fix the formatting of the dataframe to have all info by RowID and BuildingOrImprovementNumber on one row

    if len(za_gar_keep) > 0:
        tmp_garcode = za_gar_keep.groupby(['RowID', 'BuildingOrImprovementNumber'])['GarageStndCode'].apply(','.join).reset_index()
        tmp_gar_oth = za_gar_keep.groupby(['RowID', 'BuildingOrImprovementNumber'])[['GarageAreaSqFt', 'GarageNoOfCars']].first().reset_index()
        za_gar_keep = pd.merge(tmp_garcode, tmp_gar_oth, how='left', on=['RowID', 'BuildingOrImprovementNumber'])
        del(tmp_garcode,tmp_gar_oth)

    za_pool_keep = za_pool[['RowID',
                            'BuildingOrImprovementNumber',
                            'PoolStndCode',
                            'PoolSize']]

    #Fix the formatting of the dataframe to have all info by RowID and BuildingOrImprovementNumber on one row
    if len(za_pool_keep) > 0:
        tmp_poolcode = za_pool_keep.groupby(['RowID', 'BuildingOrImprovementNumber'])['PoolStndCode'].apply(','.join).reset_index()
        za_pool_keep = za_pool[['RowID', 'BuildingOrImprovementNumber', 'PoolSize']]
        tmp_pool_oth = za_pool_keep.groupby(['RowID','BuildingOrImprovementNumber']).first()
        za_pool_keep = pd.merge(tmp_poolcode, tmp_pool_oth, how='left', on=['RowID', 'BuildingOrImprovementNumber'])
        del(tmp_poolcode, tmp_pool_oth)

    zt_main_keep = zt_main[['TransId',
                            'FIPS',
                            'State',
                            'County',
                            'RecordingDate',
                            'SalesPriceAmount',
                            'SalesPriceAmountStndCode',
                            'DataClassStndCode',
                            'PropertyUseStndCode']]

    zt_prop_keep = zt_prop[['TransId',
                            'ImportParcelID',
                            'PropertySequenceNumber',
                            'UnformattedAssessorParcelNumber',
                            'PropertyFullStreetAddress',
                            'PropertyCity',
                            'PropertyState',
                            'PropertyZip',
                            'PropertyAddressLatitude',
                            'PropertyAddressLongitude',
                            'PropertyAddressCensusTractAndBlock']]

    #########################################
    #STEP 20: Run this block of code to merge the ZA tables together
        #Output two files
        #za_char_only which only housing characteristics and no assessment sales data
        #za which has the assessment sales data merged in
    #########################################

    #Start with the ZA building, pool, and garage which should be merged using (RowID, BuildingOrImprovementNumber)
    za_tmp = pd.merge(za_bldg_keep, za_gar_keep, how='left', on=['RowID', 'BuildingOrImprovementNumber'])
    za_tmp = pd.merge(za_tmp, za_pool_keep, how='left', on=['RowID', 'BuildingOrImprovementNumber'])

    #Now we can merge this file with main. Should be one row per property with characteristics.
    za_char_only = pd.merge(za_main_keep, za_tmp, how='left', on='RowID')

    #Now we can merge this file with the sales information. Should be multiple rows based on sales.
    za = pd.merge(za_char_only, za_sale_keep, how='left', on='RowID')

    #Drop the created datasets we don't need
    del(za_main, za_main_keep, za_bldg, za_bldg_keep, za_sale, za_sale_keep, za_gar, za_gar_keep, za_pool, za_pool_keep, za_tmp)

    #########################################
    #STEP 30: Run this block of code to merge the zt tables together
    #########################################

    #Merge the ZT tables together using TransId as the link
    zt = pd.merge(zt_main_keep, zt_prop_keep, on='TransId')
    del(zt_main, zt_main_keep, zt_prop, zt_prop_keep)
    #########################################
    #STEP 40: Run this block of code to select the sales data from the ZTrans file and merge with the ZA characteristics
    #########################################
    zt_sale = zt[['ImportParcelID',
                'RecordingDate',
                'SalesPriceAmount',
                'SalesPriceAmountStndCode',
                'PropertyAddressLatitude',
                'PropertyAddressLongitude',
                'PropertyUseStndCode']]

    #Rename the latitude and longitude coordinates for easy compariso
    # commentted this line out as it was causing warnings
    #zt_sale.rename(columns = {'PropertyAddressLatitude': 'zt_lat', 'PropertyAddressLongitude': 'zt_long'}, inplace=True)
    za_char_only.rename(columns = {'PropertyAddressLatitude': 'za_lat', 'PropertyAddressLongitude': 'za_long'}, inplace=True)

    #Merging the ZA Characteristics to the ZT Sales Data using the ImportParcelID
    df = pd.merge(zt_sale, za_char_only, on='ImportParcelID')

    #Generate indicators for matched latitude or longitude
    df['lat_match'] = np.where(df['PropertyAddressLatitude'] == df['za_lat'], 1, 0)
    df['long_match'] = np.where(df['PropertyAddressLongitude'] == df['za_long'], 1, 0)

    #Sort transactions by date for each ImportParcelID
    df = df.sort_values(['ImportParcelID', 'RecordingDate'], ascending=[True, True])

    #Generate a new variable to number the different transactions by parcel ID
    df['TransNum'] = df.groupby('ImportParcelID').cumcount() + 1

    #########################################
    #STEP 50: Export the dataframes to CSV. df should be the one we want to run the analysis on
    #########################################

    za.to_csv(os.path.join(csvdirPath,"za.csv"), index = False)
    zt.to_csv(os.path.join(csvdirPath, "zt.csv"), index = False)
    df.to_csv(os.path.join(csvdirPath, "df.csv"), index = False)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(process)d %(levelname)s:%(message)s',
                        level=logging.DEBUG,
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.basicConfig(filename='./clean_data.py.log',level=logging.DEBUG)
    logging.debug('Clean dataframes and Merge data by Zipcode')
    parser = argparse.ArgumentParser(description='Extract data from zillow')
    parser.add_argument("--outdir", "-o", default="OUTDIR/",
                        help= "Path to the OUTDIR folder with extracted pkl data")
    parser.add_argument("--csvdir", "-c", default="OUTDIR/ZIPCODES",
                        help="Path to the output folder for extracted csv files")
    parser.add_argument("--zipcode", "-z", default="all",
                        help= "Zipcode , required")
    args = parser.parse_args()

    # clean_data(args.outdir, args.zipcode)

    zipcode_clean = os.path.join(args.outdir, "zipcode_clean.csv")
    data = pd.read_csv(zipcode_clean)
    zipcode_list = data['zipcode'].tolist()

    for i in zipcode_list:
        print(i)
        clean_data(args.outdir, args.csvdir, str(i))
