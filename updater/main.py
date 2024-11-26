import statistics
from tqdm import tqdm

import SekitobaDataManage as dm
import SekitobaLibrary as lib
import SekitobaPsql as ps

def checkDataType( data ):
    dataType = type( data )
    
    if ( dataType is dict or data is list or data is str ) \
       and len( data ) == 0:
        return False
    elif ( dataType is int or dataType is float ) \
       and data == lib.escapeValue:
        return False

    return True

def main():
    checkData = {}
    raceKeyData = {}
    raceHorceKeyData = {}
    jockeyKeyData = {}
    testSuccess = True
    raceData = ps.RaceData()
    raceHorceData = ps.RaceHorceData()
    jockeyData = ps.JockeyData()
    #raceIdList = raceData.get_all_race_id()
    raceIdList = dm.pickle_load( "update_race_id_list.pickle" )
    raceLen = len( raceIdList )
    raceHorceLen = 0

    for raceId in tqdm( raceIdList ):
        raceData.get_all_data( raceId )
        raceHorceData.get_all_data( raceId )
        jockeyData.get_multi_data( raceHorceData.jockey_id_list )

        for k in raceData.data.keys():
            if not checkDataType( raceData.data[k] ):
                lib.dicAppend( checkData, k, [] )
                checkData[k].append( raceId )
                raceKeyData[k] = True

        for horceId in raceHorceData.data.keys():
            raceHorceLen += 1

            for k in raceHorceData.data[horceId].keys():
                if not checkDataType( raceHorceData.data[horceId][k] ):
                    lib.dicAppend( checkData, k, [] )
                    checkData[k].append( raceId )
                    raceHorceKeyData[k] = True

        for jockeyId in jockeyData.data.keys():
            for k in jockeyData.data[jockeyId].keys():
                if not checkDataType( jockeyData.data[jockeyId][k] ):
                    lib.dicAppend( checkData, k, [] )
                    checkData[k].append( raceId )
                    jockeyKeyData[k] = True
            

    for k in checkData.keys():
        test = {}

        if k in raceKeyData:
            if len( checkData[k] ) / raceLen < 0.01:
                continue
            elif 0.3 < len( checkData[k] ) / raceLen:
                print( "fail raceData {} {}".format( k, len( checkData[k] ) / raceLen ) )
                testSuccess = False
                continue
        elif k in raceHorceKeyData:
            if len( checkData[k] ) / raceHorceLen < 0.01:
                continue
            elif 0.3 < len( checkData[k] ) / raceHorceLen:
                print( "fail raceHorceData {} {}".format( k, len( checkData[k] ) / raceHorceLen ) )
                testSuccess = False
                continue
        elif k in jockeyKeyData:
            if len( checkData[k] ) / raceHorceLen < 0.01:
                continue
            elif 0.3 < len( checkData[k] ) / raceHorceLen:
                print( "fail jockeyData {} {}".format( k, len( checkData[k] ) / raceHorceLen ) )
                testSuccess = False
                continue

        for raceId in checkData[k]:
            year = raceId[0:4]
            lib.dicAppend( test, year, 0 )
            test[year] += 1

        medianData = statistics.median( list( test.values() ) )
        for year in sorted( test.keys() ):
            medianRate = test[year] / medianData

            if medianRate > 1.1:
                testSuccess = False
                print( k, year )

    if testSuccess:
        print( "Test Success!!!" )
    
if __name__ == "__main__":
    main()
