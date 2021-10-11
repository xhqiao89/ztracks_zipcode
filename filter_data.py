# -*- coding: utf-8 -*-
"""
Created on Jul 28 2020

Assumption
1. The extract_data.py has been executed and we have a folder with extracted Main.txt from both ZTrans and ZAsmt.
2. We also have the PropertInfo.txt processed into pickle file

"""

# MACHINERY TO WORK WITH ZTRAX DATA
############################################################################################################
import pandas as pd
import os, glob
import sys
import argparse
import logging
import pickle
import glob
import numpy as np
import math
import zipfile
from time import time
from threading import Thread
from queue import Queue
import csv


def Get_MainKey(dirPath, outdir, FileName, zipcode,colname):
    '''
        This function spits out the main key and the DF into the particular zipcode folder
    '''
    outKeyPath = os.path.join(outdir, colname + ".pkl")
    outDFPath = os.path.join(outdir, FileName + ".pkl")

    if os.path.exists(outDFPath):
        logging.debug(outDFPath + " already exists, reading it, delete it to regenerate")
        mkey = pd.read_pickle(outKeyPath)
        return mkey

    fullPath = os.path.join(dirPath, FileName)
    flist = glob.glob(fullPath + "*")
    mkey = pd.DataFrame([])
    mdf = pd.DataFrame([])

    for fpath in flist:
        logging.debug("Read and process " + fpath)
        chunk = pd.read_pickle(fpath)
        if zipcode != "all":
            chunk = chunk[chunk["PropertyZip"] == zipcode]
            mdf = mdf.append(chunk,ignore_index=True)
        chunk = chunk[[colname]]
        mkey = mkey.append(chunk)
    with open(outKeyPath, 'wb') as f:
        pickle.dump(mkey, f)
    if zipcode != "all":
        with open(outDFPath, 'wb') as f:
            pickle.dump(mdf, f)
    return mkey


def FilterDFByZipcode(dirPath, outdir, FileName, colname, colDF):
    outpath = os.path.join(outdir, FileName + ".pkl")

    if os.path.exists(outpath):
        logging.debug(outpath + " already exists, reading it, delete it to regenerate")
        return nil
    else:
        mdf = pd.DataFrame([])

    fullPath = os.path.join(dirPath, FileName)
    flist = glob.glob(fullPath + "*")
    for fpath in flist:
        logging.debug("Read and process " + fpath)
        chunk = pd.read_pickle(fpath)
        chunk = pd.merge(chunk, colDF, on=colname)
        mdf = mdf.append(chunk, ignore_index=True)
    with open(outpath, 'wb') as f:
        pickle.dump(mdf, f)
    return mdf

def CreateDir(dirName, clean = False):
    try:
        # Create target Directory
        os.mkdir(dirName)
        logging.debug("Directory " + dirName +  " Created ")
    except FileExistsError:
        if clean:
            os.system("rm -rf " + dirName + "/*.*")
        logging.debug("Directory " + dirName +  " already exists")

def ProcessZipcode(zipcode, outdir):
    pass

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(process)d %(levelname)s:%(message)s',
                        level=logging.DEBUG,
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.basicConfig(filename='./filter_data.py.log',level=logging.DEBUG)
    logging.debug('Filtering the extraction of data by Zipcode')
    parser = argparse.ArgumentParser(description='Extract data from zillow')
    parser.add_argument("--outdir", "-o", default="./OUTDIR/",
                        help= "Path to the output folder for extracted data")
    parser.add_argument("--zipcode", "-z", default="all",
                        help= "Zipcode , send all for all of them")
    args = parser.parse_args()

    zipcode = args.zipcode
    asmtFiles = ["Pool", "Building", "SaleData", "Garage"]
    if zipcode != "all":
        # Process a particular zipcode
        outdirPath = os.path.join(args.outdir, zipcode)
        CreateDir(outdirPath)
        asmtKeyDF = Get_MainKey(args.outdir, outdirPath, "ZAsmt.Main.txt", zipcode, "RowID")
        for f in asmtFiles:
            FilterDFByZipcode(args.outdir, outdirPath, "ZAsmt." + f + ".txt", "RowID", asmtKeyDF)
        tranKeyDF = Get_MainKey(args.outdir, outdirPath, "ZTrans.PropertyInfo.txt", zipcode, "TransId")
        zt_main = FilterDFByZipcode(args.outdir, outdirPath, "ZTrans.Main.txt", "TransId", tranKeyDF)
    else:
        # ifile = os.path.join(args.outdir, "Zipcodes.pkl")
        # ofile = os.path.join(args.outdir, "Zipcodes.csv")
        # nmpy = pd.read_pickle(ifile)
        # pd.DataFrame(nmpy).to_csv(ofile)
        # print("Saved zipcode into txt file @ " + ofile + "\n Use xargs to parallely process filter")
        zipcode_clean = os.path.join(args.outdir, "zipcode_clean.csv")
        data =  pd.read_csv(zipcode_clean)
        zipcode_list = data['zipcode'].tolist()
        for i in zipcode_list:
            print(i)
            zipcode = str(i)
            print(zipcode)
            outdirPath = os.path.join(args.outdir, str(i))
            CreateDir(outdirPath)
            asmtKeyDF = Get_MainKey(args.outdir, outdirPath, "ZAsmt.Main.txt", zipcode, "RowID")
            for f in asmtFiles:
                FilterDFByZipcode(args.outdir, outdirPath, "ZAsmt." + f + ".txt", "RowID", asmtKeyDF)
            tranKeyDF = Get_MainKey(args.outdir, outdirPath, "ZTrans.PropertyInfo.txt", zipcode, "TransId")
            zt_main = FilterDFByZipcode(args.outdir, outdirPath, "ZTrans.Main.txt", "TransId", tranKeyDF)

