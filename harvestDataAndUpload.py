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
                                        period text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); """

        c.execute(create_table_sql)
    except Error as e:
        print(e)
    
    return 

if __name__ == "__main__":
    mainPage = getPage(MAIN_URL)
    parsedUrls = set(re.findall(PARSE_URL_PATTERN, mainPage))


    for url in parsedUrls:
        url = url.replace("&amp;","&")

        save_file(url, "temp_zip.7z")
        
        archiveContent = getArchiveContent("temp_zip.7z")
     
        buffer = io.StringIO(archiveContent)
        columns = buffer.readline().split(',')  #first line contains columns name
        print(columns)
        period_index = columns.index('PERIOD')
        declarant_iso_index = columns.index('DECLARANT_ISO')
        trade_type_index = columns.index('TRADE_TYPE')
        value_index = columns.index('VALUE_IN_EUROS')


        for line in buffer:
            
            valuesList = line.split(',')
            print(valuesList)
            period = valuesList[period_index]
            declarant_iso = valuesList[declarant_iso_index]
            trade_type = valuesList[trade_type_index]
            value = valuesList[value_index]

            print(declarant_iso,trade_type,value,period)
            break
        break