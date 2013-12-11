#! /usr/bin/env python

## Required libraries
# System built-in
import sys

# External packages
from mococrawler import MocoCrawler


##
# Main function
#
def pprint(data):
    if data:
        print "callback: " + str(data)

def main():
    if len(sys.argv) > 1:
        crawler = MocoCrawler(sys.argv[1])
        crawler.crawling(None, pprint)
    return

if __name__ == '__main__':
    main()
