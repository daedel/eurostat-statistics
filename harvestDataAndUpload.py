import re
import requests
from pprint import pprint
import csv
import sqlite3
import pandas
import io
import subprocess
from urllib.request import urlopen
import py7zlib

MAIN_URL = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=comext%2FCOMEXT_DATA%2FPRODUCTS'



PARSE_URL_PATTERN = re.compile("href=[\'\"](https://ec.europa.eu/eurostat/estat-navtree-portlet-prod[^>]*?downfile[^>]*?full[^>]*?7z)[\'\"]>Download")
PARSE_DATE_PATTERN = re.compile("full(\d{6})\.") # 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&amp;downfile=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull201806.7z'}

dbConnection = None

def getPage(url):
    if url:
        response = requests.get(url)
    else:
        return False
    return response.text

def save_file(url,fileName):
    response = urlopen(url)
    out_file = open(fileName, "wb")
    out_file.write(response.read())
    out_file.close()

def getArchiveContent(fileName):

    fp = open(fileName, "rb")
    archive = py7zlib.Archive7z(fp)
    archiveMember = archive.getnames()[0]
    archiveContent = archive.getmember(archiveMember).read().decode()
    return archiveContent

def prepareDb():
    try:
        conn = sqlite3.connect("MyDb.db")
        c = conn.cursor()

        sql_create_Eurostat_table = """ CREATE TABLE IF NOT EXISTS Eurostat (
                                        id integer PRIMARY KEY,
                                        PERIOD VARCHAR(6) NOT NULL,
                                        DECLARANT_ISO VARCHAR(2) NOT NULL,
                                        TRADE_TYPE VARCHAR(1) NOT NULL,
                                        VALUE_IN_EUROS integer

                                    ); """

        c.execute(sql_create_Eurostat_table)
        return conn
    except Error as e:
        print(e)
    
    return 


def extractDates(parsedUrls, minimalVersion=True):
    dates = {}

    for url in parsedUrls:
        date = re.findall(PARSE_DATE_PATTERN, url)
        if date:
            if minimalVersion and date[0] < '201701':
                continue
            else:
                dates[date[0]] = url.replace("&amp;","&")
    return dates

if __name__ == "__main__":
    mainPage = getPage(MAIN_URL)
    parsedUrls = set(re.findall(PARSE_URL_PATTERN, mainPage))
    urlsDict = extractDates(parsedUrls)

    dbConn = prepareDb()
    dbCursor = dbConn.cursor()
    pprint(urlsDict)
    for key in urlsDict:
        url = urlsDict[key]

        print(url)
        save_file(url, "temp_zip.7z")
        
        archiveContent = getArchiveContent("temp_zip.7z")
     
        buffer = io.StringIO(archiveContent)
        columns = buffer.readline().split(',')  #first line contains columns name
        period_index = columns.index('PERIOD')
        declarant_iso_index = columns.index('DECLARANT_ISO')
        trade_type_index = columns.index('TRADE_TYPE')
        value_index = columns.index('VALUE_IN_EUROS')

        commitCounter = 0

        for line in buffer:
            
            valuesList = line.split(',')
            period = valuesList[period_index]
            if period[-2:] == '52':
                break # not sure if 52 is a week number, should I add it as December(12)? Now im just ommiting data as for example: '201052''
            declarant_iso = valuesList[declarant_iso_index]
            trade_type = valuesList[trade_type_index]
            value = valuesList[value_index]
            insert_sql = f"INSERT INTO Eurostat (PERIOD, DECLARANT_ISO, TRADE_TYPE, VALUE_IN_EUROS) VALUES (\'{period}\', \'{declarant_iso}\', \'{trade_type}\', {value})"
            dbCursor.execute(insert_sql)
            if commitCounter >= 10000: #commit to DB each 10000 rows (performance)
                dbConn.commit()
                commitCounter = 0
            commitCounter += 1