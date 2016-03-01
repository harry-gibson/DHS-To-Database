import re
import os
from difflib import SequenceMatcher as SM

def parseDCF(dcfFile):
    '''
    Parse a .DCF file (CSPro dictionary specification) into a structured object
    
    The result is a list where each item is a dictionary that represents a "DHS Recode" 
    i.e. column name in a given table.
    This dictionary is suitable for writing out to a CSV file.
    The item dictionary may have a "Values" key, whose value is itself a list of 
    dictionaries. Each of these subdictionaries represents a value that the associated 
    column can have (its value domain). The contents of the values key, if present, should be 
    written out to a separate CSV file.
    
    For example:
        schemafields = ['FileCode','RecordName','RecordTypeValue',
                    'RecordLabel','Name','Label','Start','Len',
                    'Occurrences', 'ZeroFill', 'DecimalChar', 'Decimal']
        valfields = ['FileCode','Name','Value','ValueDesc', 'ValueType']
        res = parseDCF (dcfFileName)
        for item in res:
            outWriter.writerow([item[k]] if item.has_key[k] else '' for k in schemafields)
            if 'Values' in item and len(item['Values'] > 0):
                vals = item['Values']
                for val in vals:
                    valWriter.writerow([item['FileCode'], item['Name'], 
                                        val[0], val[1], val[2]])
    '''
    # We simply read the .DCF format file in line order, imputing the hierarchical structure from the order 
    # of things, i.e. the level and record of an item is the previous found level or record 
    # We also take advantage of the blank line between chunks as a line delimiter.
    global parsedLines
    parsedLines = 0
    
    # within-survey "globals" i.e. things we need to keep track of between items
    currentRecordName = 'N/A'
    currentRecordLabel = 'N/A'
    currentRecordType = 'N/A'
    currentLevelName = ''
    currentLevelLabel = ''
    currentSurveyDecChar = False
    currentSurveyZeroFill = False
    
    currentValues = []
    skippingChunk = False
    
    currentlyParsing = "None"
    currentIds = []
    
    myRecords = {}
    myLevels = {}
    myItems = []
    myCountries = {}
    mySkippedChunks = [] 
    
    # Get a unique reference code for this dcf file that should be entered into the output data. 
    # Would be cleaner / more general to accept this as a method parameter.
    currentSurveyCode = os.path.extsep.join(os.path.basename(dcfFile).split(os.path.extsep)[:-1])
    chunkInfo = {
        'FileCode':currentSurveyCode
    }
    
    with open(dcfFile) as fileIn:
        # We read through the dcf line-by-line. The structure of the file is given by the order in 
        # which sections occur, and the sections ("chunks") are delimited by blank lines. We take 
        # advantage of these facts to build the output record specification and value specification 
        # tables.
        for line in fileIn:
            parsedLines +=1
            # Are we on a chunk start (a line with something in [Brackets])?
            # If so reset the chunkInfo global, and anything else as appropriate
            if line.find('[Level]') != -1:
                currentChunkType = "Level"
                skippingChunk = False
                chunkInfo = {}
            elif line.find('[Record]') != -1:
                currentChunkType = "Record"
                currentlyParsing = "Records"
                skippingChunk = False
                chunkInfo = {}
            elif line.find('[Item]') != -1:
                currentChunkType = "Item"
                skippingChunk = False
                chunkInfo = {}
            elif line.find('[ValueSet]') != -1:
                currentChunkType = "ValueSet"
                skippingChunk = False
                chunkInfo = {}
            elif line.find('[IdItems]') != -1:
                # Reset the iditems global as well
                currentChunkType = "IdItems"
                skippingChunk = False
                currentlyParsing = "IdItems"
                currentIds = []
                chunkInfo = {}
            elif line.find('[Dictionary]') != -1:
                currentChunkType = "Dictionary"
                currentlyParsing = "Dictionary"
                skippingChunk = False
                recordTypeStart = 0
                recordTypeLen = 0
                
            elif line.find('[') != -1 and line.find(']') != -1:
                # This is some chunk we don't know and/or care about. 
                skippingChunk = True
                mySkippedChunks.append(line)

            # Or are we on the end of a chunk, marked by a blank line? This is the point at which 
            # we may want to do something with the previous lines of info
            elif line == '\n':
                if skippingChunk:
                    # this was a bunch of lines we skip (e.g. those following '[Dictionary]')
                    skippingChunk = False
                else:
                    if currentChunkType == 'Dictionary':
                        # This will be the first item in the file and will be written to the first 
                        # row of the output. It's an item that describes for all lines of the data file 
                        # where the record type can be found (start/len). It's a fudge to make this fit 
                        # the overall row format which is designed to describe an item
                        chunkInfo['RecordName'] = '*'
                        chunkInfo['RecordLabel'] = '*'
                        chunkInfo['RecordTypeValue'] = '*'
                        # the record type positioning is labelled in the DCF as "RecordTypeStart"(Len) 
                        # but we want to record it into the standard Start/Len columns of the output CSV
                        # so just copy it over
                        chunkInfo['Start'] = chunkInfo['RecordTypeStart']
                        chunkInfo['Len'] = chunkInfo['RecordTypeLen']
                        chunkInfo['ItemType'] = 'RecordDesciption'
                        myItems.append(chunkInfo)
                        # set the default values for the item parsing info
                        currentSurveyZeroFill = chunkInfo['ZeroFill']
                        currentSurveyDecChar = chunkInfo['DecimalChar']
                    
                    # If we're at the end of a chunk defining a level or record then place the 
                    # info into the globals so that the item parser will read them for items
                    # that FOLLOW afterward
                    elif currentChunkType == 'Level':
                        currentLevelName = chunkInfo['Name']
                        currentLevelLabel = chunkInfo['Label']
                        if myLevels.has_key(currentLevelName):
                            if myLevels[currentLevelName] == currentLevelLabel:
                                print "Warning, duplicate level name/label encountered at line "+str(parsedLines)
                            else:
                                raise ValueError("Duplicate level name encountered at line {0!s} with non-matched label".
                                                 format(parsedLines))
                        myLevels[currentLevelName] = currentLevelLabel

                    elif currentChunkType == 'Record':
                        # save into dirty globals so the subsequent items know what record they belong to
                        currentRecordName = chunkInfo['Name']
                        currentRecordLabel = chunkInfo['Label']
                        currentRecordType = chunkInfo['RecordTypeValue']
                        
                        # At the end of a record chunk, we save an "item" with the new record name/type/label 
                        # reflecting the id item for this record. In other words the first output row of each record 
                        # will describe the record itself and in particular the start/len of the id item(s)
                        chunkInfo['FileCode']=currentSurveyCode
                        # apply the parent hierarchical labels, just stored in simple globals
                        chunkInfo['RecordName'] = currentRecordName
                        chunkInfo['RecordLabel'] = currentRecordLabel
                        chunkInfo['RecordTypeValue'] = currentRecordType.strip("'")
                        chunkInfo['LevelName'] = currentLevelName
                        chunkInfo['LevelLabel'] = currentLevelLabel
                        chunkInfo['ItemType'] = 'IdItem'
                        for iditem in currentIds:
                            # add a row for each id item 
                            # Normally there will only be one (which may implicitly code more than one within it e.g. 
                            # caseid includes HHID and another number), but sometimes (looking at you, HIV datasets)
                            # there may be several encoded as several items
                            newItem = {}
                            for i in chunkInfo:
                                # copy the common record-related stuff, careful not to just modify chunkInfo and add it repeatedly
                                # as that wouldn't work (reference types and all that jazz)
                                newItem[i] = chunkInfo[i]
                            newItem['Name'] = iditem['Name']
                            newItem['Label'] = iditem['Label']
                            newItem['Start'] = iditem['Start']
                            newItem['Len'] = iditem['Len']
                            myItems.append(newItem)

                        if myRecords.has_key(currentRecordName):
                            if myRecords[currentRecordName] == currentRecordLabel:
                                print "Warning, duplicate record name/label encountered at line "+str(parsedLines)
                            else:
                                raise ValueError("Duplicate record name encountered at line {0!s} with non-matched label".
                                                 format(parsedLines))
                        myRecords[currentRecordName] = currentRecordLabel
                    
                    #elif currentChunkType == 'Dictionary'

                    # If we're at the end of a chunk defining a valueset then place the info
                    # into the last-processed item - valueset comes AFTER the item and a blank line,
                    # so the item will have already been added to the output list myItems
                    elif currentChunkType == 'ValueSet':
                        # check it matches-ish. Either starts the same or text similarity is high
                        # - sometimes they abbreviate the valueset but not the previous label
                        s1 = chunkInfo['Label']
                        s2 = myItems[-1]['Label']
                        simRatio = SM(None, s1, s2).ratio()
                        if not (simRatio > 0.7 or chunkInfo['Label'].find(myItems[-1]['Label']) == 0):
                            print ("Warning, valueset did not seem to match item at line {0!s} of file {1!s} - please check!".
                                   format(parsedLines,dcfFile))
                        
                        if 'ValueRanges' in chunkInfo:
                            # If there was more than one range then expand each out to the individual values
                            # This occurs with something like 1:12=age in months, 13:112 = (age in years +12)
                            if len(chunkInfo['ValueRanges']) > 1:
                                for rangeInfo in chunkInfo['ValueRanges']:
                                    thisRangeMin = int(rangeInfo[0])
                                    thisRangeMax = int(rangeInfo[1])
                                    thisRangeDesc = rangeInfo[2]
                                    assert thisRangeMax > thisRangeMin
                                    for expandedVal in range(thisRangeMin, thisRangeMax+1):
                                        currentValues.append((expandedVal, thisRangeDesc, "ExpandedRange"))
                            else:
                                rangeInfo = chunkInfo['ValueRanges'][0]
                                currentValues.append((rangeInfo[0], rangeInfo[2], "RangeMin"))
                                currentValues.append((rangeInfo[1], rangeInfo[2], "RangeMax"))
                               
                        if myItems[-1].has_key('Values'):
                                # occasionally items have two valueset chunks!
                            myItems[-1]['Values'].extend(currentValues)
                        else:
                            myItems[-1]['Values'] = currentValues
                        currentValues = []
                        #else:
                        #    raise ValueError("Error parsing valueset at line "+str(parsedLines))

                    # Otherwise we are at the end of a chunk defining an actual item (recode)
                    elif currentChunkType == 'Item':
                        if currentlyParsing == "Records":
                            # This is a "normal" line of the file, i.e. one recode or column of a table.
                            # Apply the parent hierarchical labels, just stored in simple globals
                            chunkInfo['RecordName'] = currentRecordName
                            chunkInfo['RecordLabel'] = currentRecordLabel
                            chunkInfo['RecordTypeValue'] = currentRecordType.strip("'")
                            chunkInfo['LevelName'] = currentLevelName
                            chunkInfo['LevelLabel'] = currentLevelLabel
                            chunkInfo['FileCode']=currentSurveyCode
                            if not 'ZeroFill' in chunkInfo:
                                chunkInfo['ZeroFill'] = currentSurveyZeroFill
                            if not 'DecimalChar' in chunkInfo:
                                chunkInfo['DecimalChar'] = currentSurveyDecChar
                            # "save" the information to the output list
                            chunkInfo['ItemType'] = 'Item'
                            myItems.append(chunkInfo)
                        elif currentlyParsing == "IdItems":
                            # this is a special case; it needs to be written out as an "item" for 
                            # each record. In the .dcf, IdItems comes after level info but before record 
                            # info. So save the info into dirty globals so that when we parse the record 
                            # info that follows we have access to it.
                            currentIds.append({
                                'Name':     chunkInfo['Name'],
                                'Label':    chunkInfo['Label'],
                                'Start':    chunkInfo['Start'],
                                'Len':      chunkInfo['Len']
                            })
            else:
                # We are "within" a chunk of information
                # add item key / value to the current chunk dictionary
                # There are sometimes lines with more than one equals sign in (as it can appear in the 
                # description) so split at the FIRST = position only and clear up a bit (carriage return)
                splitPos = line.find('=')
                fieldName = line[0:splitPos].strip()
                fieldVal = line[splitPos+1:].strip()
                #fieldName,fieldVal = line.split('=')
                
                if fieldName == 'Value':
                    # we don't explicitly check that we're in a valueset chunk, but we will be(?)
                    
                    # Look for a description first. Because if a description contains a time
                    # then this would be seen as a range below
                    # e.g. Value=1;Yes: between 2:00 and 6:00 pm
                    descMatch = re.split(';(.*)$', fieldVal)
                    if len(descMatch) > 1:
                        valDesc = descMatch[1]
                        fieldVal = descMatch[0]
                    else:
                        valDesc = ''
                        
                    # match value ranges based on pattern "digits-colon-digits"
                    # Add these to a separate list of valueranges, because we will write them out differently
                    # depending on whether there is one or more than one range specified
                    match = re.search('\d+:\d+', fieldVal)
                    if match:
                        try:
                            # the right hand side sometimes contains a description of the range values
                            # after a semicolon 
                            #descMatch = re.search('^(.*);(\w+)$', fieldVal)
                            #if descMatch:
                            #    valDesc = descMatch.group(1)
                            #else:
                            #    valDesc = ''
                            
                            # again don't just split and unpack, in case there is a colon in the description too
                            # also sometimes we see multiple ranges on one line e.g. line 35629 of COIR53.DCF:
                            # 100:101 102:198;Days
                            rangesOnLine = re.findall('\d+:\d+', fieldVal)
                            for minmax in rangesOnLine:
                                splitPos = minmax.find(':')
                                vMin = minmax[0:splitPos].strip()
                                vMax = minmax[splitPos+1:].strip()
                                if not 'ValueRanges' in chunkInfo:
                                    chunkInfo['ValueRanges'] = []
                                chunkInfo['ValueRanges'].append((vMin, vMax, valDesc.strip()))    
                        
                            #if re.match('\d+;', vMax):
                            #    splitPos = vMax.find(';')
                            #    vMaxNew = vMax[0:splitPos]
                            #    valDesc = vMax[splitPos+1:]
                            #    vMax = vMaxNew
                            # if it doesn't then use the valueset label instead
                            #else:
                            #    valDesc = chunkInfo['Label']
                            #    valDesc = ''
                        except:
                            print "uhoh!"
                            print fieldVal
                            print chunkInfo
                            
                            valRange, otherCrap = fieldVal.split(';')
                            vMin,vMax = valRange.split(':')
                            if not 'ValueRanges' in chunkInfo:
                                chunkInfo['ValueRanges'] = []
                            chunkInfo['ValueRanges'].append((vMin, vMax, valDesc.strip()))    
                        
                    # match "normal" value/description pairs based on digits-semicolon
                    #elif re.match('\d+', fieldVal):
                        # split at pos of first semicolon
                        #scPos = fieldVal.find(';')
                        #val = fieldVal[0:scPos]
                        #valDesc = fieldVal[scPos+1:]
                        # do not just split at semicolon because semicolon might also occur within the desc
                        #val, valDesc = fieldVal.split(';')
                    #    m = re.match('\d+', fieldVal)
                    #    val = m.group()
                    #    currentValues.append((val.strip(),valDesc.strip(), "ExplicitValue"))
                    
                    # match horrible lines with VALUE given as string with quotes (see VCAL_VS1)
                    #elif re.match('.+;', fieldVal):
                    #    scPos = fieldVal.find(';')
                    #    val = fieldVal[0:scPos]
                    #    valDesc = fieldVal[scPos+1:]
                        #val, valDesc = fieldVal.split(';')
                    #    currentValues.append((val.strip("'").strip(),valDesc.strip("'").strip(), "ExplicitValue"))
                    
                    # else save whatever we've got, presumably there is a value with no desc
                    else:
                        currentValues.append((fieldVal,valDesc.strip(), "ExplicitValue"))

                elif not chunkInfo.has_key(fieldName):
                    # append the first occurrence of other labels. Subsequent ones will be silently discarded
                    chunkInfo[fieldName] = fieldVal 
        print "Parsed {0!s} lines into {1!s} items".format(parsedLines,len(myItems))
        return myItems