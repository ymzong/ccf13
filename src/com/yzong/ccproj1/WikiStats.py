# Running time: 5m13.056s

# Aggregates English wikipedia entries along with their view counts.
# Result is sorted descendingly by the view count.

import string

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

def sorter(arg1, arg2):
    return arg2[1] - arg1[1]

def doit():
    f = open('data', 'r')
    newLine = f.readline()
    englishWikiList = list()
    totalCount = 0
    while newLine != "":
        if True or newLine.startswith("en "):
            newLine = newLine[:-1].split(" ")
            lang = newLine[0]
            name = newLine[1]
            count = int(newLine[2])
            totalCount += count
            if (lang == "en") and isQualifiedName(name):
                englishWikiList.append((name, count))
        newLine = f.readline()
    f.close()
    # Sort by descending view frequency.
    englishWikiList = sorted(englishWikiList, sorter)
    f = open('result.txt', 'w')
    f.write("Total Count: " + str(totalCount) + "\n")
    for elem in englishWikiList:
        f.write(elem[0] + "\t" + str(elem[1]) + "\n")
    f.close()

doit()

