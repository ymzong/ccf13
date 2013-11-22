#!/usr/bin/env python

# This is the reducer of a Map-Reduce pair.
# Given the mapper's output, the reducer emits a line for each article entry:
# <total month views>\t<article name>\t{<date_i:page views for date_i>}...
# Only articles with at least 100000 total views are in the output.
# The result, as from mapper, is sorted descendingly by total view count.

import sys
import copy

# Threshold number of total views.
THRESHOLD = 100000

# List of all qualified titles, along with their information
outputData = list()

# Saves the given entry into the global list.
def saveEntry(currentTotal, currentTitle, currentDetails):
    outputData.append([currentTotal, currentTitle,
                         copy.deepcopy(currentDetails)])

# Sort, then output the list of entries.
def outputResults():
    for entry in outputData:
        currentTotal = entry[0]
        currentTitle = entry[1]
        currentDetails = entry[2]
        outputString = str(currentTotal) + "\t" + currentTitle + "\t"
        for i in xrange(1, 31):
            outputString += ("201306" + str(i).rjust(2, "0") +
                           ":" + str(currentDetails[i]) + "\t")
        print outputString[:-1]

def main():
    currentTitle = None
    currentTotal = None 
    currentDetails = [0] * 31
    for entry in sys.stdin:
        (entry_title, entry_date, entry_count) = entry.strip().split("\t")
        entry_date = int(entry_date)
        entry_count = int(entry_count)
        if entry_title == currentTitle:
            currentTotal += entry_count
            currentDetails[entry_date] += entry_count
        else:
            # New article has come! Output the current word.
            if currentTotal >= THRESHOLD:
                saveEntry(currentTotal, currentTitle, currentDetails)
            currentTitle = entry_title
            currentTotal = entry_count
            currentDetails = [0] * 31
            currentDetails[entry_date] = entry_count
    if currentTotal >= THRESHOLD:
        saveEntry(currentTotal, currentTitle, currentDetails)
    outputResults()

if __name__ == "__main__":
    main()

