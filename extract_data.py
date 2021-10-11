# -*- coding: utf-8 -*-
"""
Created on Thu Feb 15 16:57:59 2018

"""

# MACHINERY TO WORK WITH ZTRAX DATA
############################################################################################################
import pandas as pd
import os
import argparse
import logging
import pickle
import glob
import zipfile
import multiprocessing as mp

def Million(lsize):
    return lsize * 1000 * 1000

def LoadLayout(lPath):
    """
        Load the layout from the layout folder
    """
    logging.debug("Reading the layout from %s", lPath)
    xl = pd.ExcelFile(lPath)
    if 'ZTrans' not in xl.sheet_names or 'ZAsmt' not in xl.sheet_names:
        logging.debug("Cannot find layout data")
        return None, None

    ZTrans = pd.read_excel(lPath, sheet_name='ZTrans')
    ZAsmt = pd.read_excel(lPath, sheet_name='ZAsmt')
    return ZTrans, ZAsmt

def Read_ZTrans(archive, outdir, ZTrans, FileName, columns=None, cSizeInMillions=1):
    logging.debug("Reading ZTrans data from %s", FileName)
    table_name = FileName.split('.')[0]
    layout_temp = ZTrans.loc[ZTrans.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName']
    # the field name is a typo, it should be DataType
    dtype=layout_temp['DateType'].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    for d in dtype:
        if dtype[d] == 'bigint':
            dtype[d] = 'int64'
        if dtype[d] == 'smallint':
            dtype[d] = 'int64'
        else:
            dtype[d] = 'object'
    chunk_size = Million(cSizeInMillions)  # read 5 million rows
    reader = pd.read_csv(archive.open("ZTrans\\"+FileName), quoting=quoting, names=names, dtype=dtype,
                         encoding=encoding, sep=sep, header=header,
                         chunksize=chunk_size)
#                        usecols=columns)
    data_p_files=[]
    j = 0
    for i, chunk in enumerate(reader):
        ofile = os.path.join(outdir, "ZTrans." + FileName + str(i) + ".pkl")
        logging.debug("Dumping file %s, %d", ofile, j)
        j = j + len(chunk)
        data_p_files.append(ofile)
        with open(ofile, "wb") as f:
            pickle.dump(chunk,f,pickle.HIGHEST_PROTOCOL)

def Read_ZAsmt(archive, outdir, ZAsmt, FilePrefix, FileName, retDataFrame=False, columns=None, cSizeInMillions=1):
    logging.debug("Reading ZAsmt data from %s", FileName)
    table_name = FileName.split('.')[0]
    layout_temp = ZAsmt.loc[ZAsmt.TableName=='ut{}'.format(table_name), :].reset_index()
    names=layout_temp['FieldName']
    # the field name is a typo, it should be DataType
    dtype=layout_temp['DateType'].to_dict()
    encoding='ISO-8859-1'
    sep = '|'
    header=None
    quoting=3
    for d in dtype:
        if dtype[d] == 'bigint':
            dtype[d] = 'int64'
        if dtype[d] == 'smallint':
            dtype[d] = 'int64'
        else:
            dtype[d] = 'object'
    chunk_size =  Million(cSizeInMillions) # read 5 million rows

    reader = pd.read_csv(archive.open("ZAsmt\\"+FileName), quoting=quoting, names=names, dtype=dtype,
                         encoding=encoding, sep=sep, header=header,
                         chunksize=chunk_size,usecols=columns)
    data_p_files=[]
    j = 0
    for i, chunk in enumerate(reader):
        ofile = os.path.join(outdir, "ZAsmt." + FilePrefix +  FileName + str(i) + ".pkl")
        logging.debug("Dumping file %s, %d", ofile, j)
        data_p_files.append(ofile)
        with open(ofile, "wb") as f:
            pickle.dump(chunk,f,pickle.HIGHEST_PROTOCOL)
        j = j + len(chunk)
        # if j > chunk_size or (j > 0 and len(chunk) == 0):
        #     break
    if retDataFrame is False:
        return
    df = pd.DataFrame([])
    for i in range(len(data_p_files)):
            logging.debug("Merging file %s", data_p_files[i])
            df = df.append(pd.read_pickle(data_p_files[i]),ignore_index=True)
    return df

def Get_AllZipCodes(archive, outdir, ZA, FileName):
    # Property Zipcode in ZAssmt is 29 (indexed at 0)
    ofile = os.path.join(outdir, "Zipcodes.pkl")
    logging.debug("Generating All Zipcodes")
    if os.path.exists(ofile):
        logging.debug("Found " + ofile + "Skipping reading the archive, loading the pickle file")
        df = pd.read_pickle(ofile)
        return list(df)

    df = Read_ZAsmt(archive, outdir, ZA, "Zipcode", "Main.txt", True, [29])
    # df_ZA = Read_ZAsmt(archive, outdir_ZA, ZA, "Zipcode", "Main.txt", True, [29])
    df = df['PropertyZip'].unique()
    # ZAsmt
    # df_ZA = Read_ZAsmt(archive, outdir_ZA, ZA, "Zipcode", "Main.txt", True, [29])
    # # df_hist = Read_ZAsmt(archive_hist, outdir_hist, ZA, "Zipcode", "Main.txt", True, [29])
    # # df = df_ZA.append(df_hist)
    # df = df_ZA['PropertyZip'].unique()

    with open(ofile, "wb") as f:
        pickle.dump(df,f,pickle.HIGHEST_PROTOCOL)
    return list(df)


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                        level=logging.DEBUG,
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.basicConfig(filename='./extract_data.py.log',level=logging.DEBUG)
    logging.debug('Processing the extraction of data')
    parser = argparse.ArgumentParser(description='Extract data from zillow')
    parser.add_argument("--layout", "-l", default="./META/Layout.xlsx",
                        help="Excel file that has the data layout")
    parser.add_argument("--datadir", "-d", default="./DATADIR/",
                        help= "Path to the folder containing Zillow data")
    parser.add_argument("--zipfile", "-z", default="12.zip",
                        help= "Zip file to process, default Florida")
    parser.add_argument("--outdir", "-o", default="./OUTDIR/",
                        help= "Path to the output folder for extracted data")
    parser.add_argument("--chunksizeInMillion", "-c", default=1,type=int,
                        help= "chunk size in millions")
    args = parser.parse_args()
    zipPath = os.path.join(args.datadir, args.zipfile)
    archive  = zipfile.ZipFile(zipPath, 'r')
    outdir_ZA = os.path.join(args.outdir, "ZA")
    outdir_ZT = os.path.join(args.outdir, "ZT")

    # ZASMT HISTORICAL DATA
    # zipPath_hist = os.path.join("./DATADIR/Hist/", "12.zip")
    # archive_hist = zipfile.ZipFile(zipPath_hist, 'r')
    # outdir_hist = "./OUTDIR/Hist/"

    # Load the layout data from excel sheet
    ZT, ZA = LoadLayout(args.layout)
    # STEP 1 : Get all Zipcodes
    zipcodes = Get_AllZipCodes(archive, args.outdir, ZA, "Main.txt")
    # STEP 2: Generate the pickle files
    #ZAFiles = ["Main.txt", "Building.txt", "SaleData.txt", "Pool.txt", "Garage.txt"]
    #for files in ZAFiles:
    #    p= mp.Process(target=Read_ZAsmt, args=(archive, args.outdir, ZA, "", files))
    #    p.start()
    #    p.join()
    Read_ZAsmt(archive, args.outdir, ZA,"", "Main.txt", False, None, args.chunksizeInMillion)
    Read_ZAsmt(archive, args.outdir, ZA,"", "SaleData.txt", False, None, args.chunksizeInMillion)
    Read_ZAsmt(archive, args.outdir, ZA,"", "Building.txt", False, None, args.chunksizeInMillion)
    Read_ZAsmt(archive, args.outdir, ZA,"", "Pool.txt", False, None, args.chunksizeInMillion)
    Read_ZAsmt(archive, args.outdir, ZA,"", "Garage.txt", False, None, args.chunksizeInMillion)
    Read_ZTrans(archive, args.outdir, ZT, "PropertyInfo.txt", args.chunksizeInMillion)
    Read_ZTrans(archive, args.outdir, ZT, "Main.txt", args.chunksizeInMillion)
