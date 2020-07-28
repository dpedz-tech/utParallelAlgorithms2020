#!/usr/bin/env python
import os
import zipfile
import threading
import requests


def download_url(url, save_path, chunk_size=128):
    # print(save_path)
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def unzipfile(zipfile_path, directory):
    with zipfile.ZipFile(zipfile_path, 'r') as zip_ref:
        zip_ref.extractall(directory)


def downloadAndUnZipAction(currentYearPath, y, q):
    # https: // transtats.bts.gov / PREZIP / Origin_and_Destination_Survey_DB1BMarket_2020_1.zip
    csvfile = "Origin_and_Destination_Survey_DB1BMarket_" + str(y) + "_" + str(q) + ".csv"
    zipfile = "Origin_and_Destination_Survey_DB1BMarket_" + str(y) + "_" + str(q) + ".zip"
    website = "https://transtats.bts.gov/PREZIP/" + zipfile

    if not os.path.exists(currentYearPath + "/" + zipfile):
        print("downloading " + str(y) + " " + str(q) )
        download_url(website, currentYearPath + "/" + zipfile, 256)
        print("downloaded " + str(y) + " " + str(q))
    if not os.path.exists(currentYearPath + "/" + csvfile):
        print("unzipping " + str(y) + " " + str(q))
        unzipfile(currentYearPath + "/" + zipfile, currentYearPath)
        print("unzipped " + str(y) + " " + str(q))


class downloadAndUnZipDataFileThread(threading.Thread):
    def __init__(self, currentYearPath, y, q):
        threading.Thread.__init__(self)
        self.currentYearPath = currentYearPath
        self.y = y
        self.q = q

    def run(self):
        print("Starting " + str(self.y) + " " + str(self.q)+"")
        downloadAndUnZipAction(self.currentYearPath, self.y, self.q)
        print("Exiting " + str(self.y) + " " + str(self.q)+"")



def initialize():
    threads = []
    # quarter = [1, 2, 3, 4]
    # year = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
    for y in range(2010, 2020):
        try:
            currentYearPath = os.path.join(os.path.dirname(os.path.abspath(__file__)) + "/../data/" + str(y))
            if not os.path.exists(currentYearPath):
                os.makedirs(currentYearPath)
                print('Created:', currentYearPath)
            for q in range(1,5):
                newThread = downloadAndUnZipDataFileThread(currentYearPath, y, q)
                newThread.start()
                threads.append(newThread)
        except OSError as error:
            print(error)

    for proc in threads:
        proc.join()
