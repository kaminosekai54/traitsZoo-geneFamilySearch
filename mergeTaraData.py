# imports
import os, sys, time
import pandas as pd
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.SeqRecord import SeqRecord
from multiprocessing import Pool
import settings

################################################################################
settings = settings.getSettings()


# function getFastaRecords
# this function juste return the fasta records as list
#@param
# @fastaFile, the fasta file to read
def getFastaRecords(fastaFile):
    return list(SeqIO.parse(fastaFile, "fasta"))

# function mergeAll,
# this function merge all the tara data file into a two big one, one for each methode (MAG, MGT)
def mergeAll():
    magList=[]
    mgtList= []
    t1 = time.time()
    with Pool() as p:
        magList = p.map(getFastaRecords, [settings["path"]["magData"] + file for file in os.listdir(settings["path"]["magData"])])
        mgtList = p.map(getFastaRecords, [settings["path"]["mgtData"] + file for file in os.listdir(settings["path"]["mgtData"])])

    magData= [seq for fileData in magList for seq in fileData]
    mgtData= [seq for fileData in mgtList for seq in fileData]
    SeqIO.write(magData, settings["path"]["rawData"] + "taraDataMAG_pep.fasta", "fasta-2line")
    SeqIO.write(mgtData, settings["path"]["rawData"] + "taraDataMGT_pep.fasta", "fasta-2line")
    t2 = time.time()
    print("all tara data  merged in " + str((t2-t1)/60) + " sec")
    

if __name__ == '__main__':
    mergeAll()