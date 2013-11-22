#!/usr/bin/env python

# This is the mapper of a Map-Reduce pair.
# Given the Wikipedia log file, extract the time stamp and view count of each
# article. The intermediate result has form "ARTICLE_NAME\tDATE\tVIEW_COUNT".

import string
import os
import sys

bannedStart = ["Media", "Special", "Talk", "User", "User_talk", "Project",
    "Project_talk", "File", "File_talk", "MediaWiki", "MediaWiki_talk",
    "Template", "Template_talk", "Help", "Help_talk", "Category",
    "Category_talk", "Portal", "Wikipedia",
    "Wikipedia_talk"] + list(string.ascii_lowercase)

bannedEnd = [".jpg", ".gif", ".png", ".JPG", ".GIF", ".PNG", ".txt", ".ico"]

bannedString = ["404_error/", "Main_Page", "Hypertext_Transfer_Protocol",
    "Favicon.ico", "Search"]

def isQualifiedName(name):
    for s in bannedStart:
        if name.startswith(s):
	    return False
    for s in bannedEnd:
        if name.endswith(s):
	    return False
    for s in bannedString:
        if name == s:
	    return False
    return True

def main():
    # Only need the "Date" info from the File Name.
    inputTimeStamp = (os.environ["map_input_file"])[-12:-10]
    for newLine in sys.stdin:
        newLine = newLine[:-1].split(" ")
        lang = newLine[0]
        name = newLine[1]
        count = int(newLine[2])
        if (lang == "en") and isQualifiedName(name):
            print name + "\t" + inputTimeStamp + "\t" + str(count)

if __name__ == "__main__":
    main()

