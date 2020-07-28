#!/usr/bin/env python
import DownloadDataFiles
import ParseDataFiles


def main():
    DownloadDataFiles.initialize()
    ParseDataFiles.getCSVFiles()




if __name__ == "__main__":
    main()
