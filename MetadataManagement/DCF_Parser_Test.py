#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      zool1301
#
# Created:     26/04/2016
# Copyright:   (c) zool1301 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
from DCF_Parser_Main import parseDCF
import os

inFile = r'\\129.67.26.176\map_data\DHS_Automation\Acquisition\All\287\287.BDIR51.DCF'
outDir = r'\\129.67.26.176\map_data\DHS_Automation\DataExtraction\All\287\Test'

reqfieldnames = ['ItemType', 'FileCode','RecordName','RecordTypeValue','RecordLabel','Name','Label',
                 'Start','Len','Occurrences','ZeroFill', 'DecimalChar', 'Decimal', 'FMETYPE']
valfieldnames = ['FileCode','Name','Value','ValueDesc', 'ValueType']
relfieldnames = ['FileCode', 'RelName', 'PrimaryTable', 'PrimaryLink', 'SecondaryTable', 'SecondaryLink']

def main():
    if not (os.path.exists(outDir)):
        os.makedirs(outDir)
    parsedDCFItems, parsedDCFRelations = parseDCF(inFile, expandRanges="None")



if __name__ == '__main__':
    main()
