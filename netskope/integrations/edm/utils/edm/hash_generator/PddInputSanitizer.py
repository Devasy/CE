# flake8: noqa
"""
    PddInputSanitizer.py
    ~~~~~~~~~~~~~~~~~~~~

    Preprocess input csv file and split the good records
    and bad records into <output>.good and <output>.bad files.
    Then, user should use the <output>.good file for PDD hashing.

    :copyright: (c) 2019 Netskope, Inc. All rights reserved.
    ..author: Wai Yim<wyim@netskope.com> (Sept, 2019)
"""
import csv
import re
import argparse


class InvalidStats(object):
    """
        Invalid CSV row data class.
    """
    def __init__(self):
        self.totalEmpty = 0
        self.total1Digit = 0
        self.total1Letter = 0
        self.totalHasDigit = 0
        self.totalSingleAfterPunc = 0
        self.totalStopwords = 0

    def printStats(self, msg):
        """
            Print stats
            :param msg: Message to print along with stats
        """
        print ("==================")
        print ("%s: " % msg)
        print ("Total empty: %d " % self.totalEmpty)
        print ("Total single digit: %d " % self.total1Digit)
        print ("Total single letter: %d " % self.total1Letter)
        print ("Total has digit: %d " % self.totalHasDigit)
        print ("Total single letter - strip punc: %d " % self.totalSingleAfterPunc)
        print ("Total stopwords: %d " % self.totalStopwords)


lastName = InvalidStats()
firstName = InvalidStats()
totalBothInvalid = 0
totalRows = 0
totalGood = 0
totalModifiedGood = 0
totalBad = 0
totalTrimZeros = 0
curLine = None

def writeRowToFile(outfile, row, first, last):
    """
        Write a row to a file with replacement
        of first name and last name fields.
        :param outfile: CSV to write row data to
        :param row: row data
        :param first: first name
        :param last: last name
    """
    global totalGood
    global totalModifiedGood
    data = u''
    i = 0
    for cell in row:
        if i > 0:
            data += '~'

        if i == 4:
            data += last
        elif i == 5:
            data += first
        else:
            data += cell
        i += 1

    outfile.write(data.encode('iso-8859-1'))
    outfile.write(b'\n')
    totalGood += 1
    totalModifiedGood += 1


def stripEndChars(data):
    """
        String the ending now alphbet chars
        :param data: token data
        :return: return string with ending not alphbet stripped.
    """
    if not data:
        return None

    while len(data) > 0 and not data[-1].isalpha():
        data = data[:-1]

    if len(data) <= 1:
        return None

    return data

def getFirstLast(cellData, stopwords):
    """
        Get the first and last valid token from a cell.
        This filters out all invalidate elements from
        validateName() call.  And each valid token has
        to be at least 3 chars long.
        :param data: Cell data
        :param stopwords: list of stopwords
        :return: return the first and last valid tokens.
    """
    first = None
    last = None

    data = stripEndChars(cellData)
    if not data:
        return cellData, None

    tokens = data.split(' ')
    if len(tokens) <= 1:
        # onlyt 1 token, nothing to do
        return data, None

    for t in tokens:
        # if there is a '.' in a token, skip
        # assume some kind of abbrev
        if t.find('.') >= 0 and len(t) < 6:
            continue

        if not validateName(t, None, stopwords):
            continue

        t = stripEndChars(t)

        #token has to be at least 3 chars
        if not t or len(t) < 3:
            continue

        # found 1st valid token
        if not first:
            first = t
        else:
            last = t

    if not first:
        first = data

    return first, last


def validateName(data, stats, stopwords):
    """
        Given a row cell, we invalidate:
        - 1 char field
        - 1 char after removed all non-alpha-numeric chars
        - any digit in the cell
        - the whole cell matches in stopwords list
        :param data: Cell data
        :param stats: statsm class
        :param stopwords: list of stopwords
        :return: return True of passed
    """
    if not data:
        return False

    if len(data) == 0:
        # empty is OK
        if stats:
            stats.totalEmpty += 1
        return False

    # no single letter/digit
    if len(data) == 1:
        if stats:
            if data.isdigit():
                stats.total1Digit += 1
            else:
                stats.total1Letter += 1
        return False

    # no digit in name
    if any(i.isdigit() for i in data):
        if stats:
            stats.totalHasDigit += 1
        return False

    # single letter after stripping punctations
    tmp = re.sub(r'\W+', '', data)
    if len(tmp) == 1:
        if stats:
            stats.totalSingleAfterPunc += 1
        return False

    # words in stopwords list
    if data.upper() in stopwords:
        if stats:
            stats.totalStopwords += 1
        return False

    return True

def normalizeNumber(num):
    """
        remove all leading zeros from number fields
        :param num: number cell
        :return: None if there is no leading 0s. Else stripped num.
    """
    if num and len(num) > 5 and num[0] == '0':
        # strip padding 0s
        num = num.lstrip('0')
        return num

    return None

def utf8_encoder(unicode_csv_data):
    """
        Read a line from CSV file
        :param unicode_csv_data: CSV file data
        :return: CSV line
    """
    global curLine
    cursor = unicode_csv_data.tell()
    while True:
        line = unicode_csv_data.readline()
        if not line:
            return
        cursor = unicode_csv_data.tell()
        curLine = line
        yield line.decode('iso-8859-1')

def parseCSV(filename, stopwords, columns, verbose, outfile):
    """
        Parse the CSV file
        :param filename: CSV file name
        :param stopwords: stopwords file name
        :param columns: flag to treat first row as column names
        :param verbose: verbose
        :param outfile: output CSV file base name. Add .good and .bad
    """
    idx = 0
    goodfile = None
    badfile = None
    global totalRows
    global totalBothInvalid
    global totalGood
    global totalBad
    global totalTrimZeros

    if len(outfile) > 0:
        goodfile = open(outfile+".good", "wb")
        badfile = open(outfile+".bad", "wb")

    # failed to open good/bad files, return
    if not goodfile or not badfile:
        print ("Failed to create good or bad files.")
        if goodfile:
            goodfile.close()
        if badfile:
            badfile.close()
        return

    with open(filename, 'rb') as f:

        # if first row is column, remember and skip
        if columns > 0:
            for i in range(columns):
                hdr = f.readline()
                goodfile.write(hdr)
                badfile.write(hdr)

        data = csv.reader(utf8_encoder(f), delimiter="~", quoting=csv.QUOTE_NONE)

        #try:
        if data:
            for row in data:
                invalidf = False
                invalidl = False
                invalidn = False

                if len(row) < 1:
                    #empty row
                    continue

                # don't strip leading 0s from CSV file for now.
                #normNum = normalizeNumber(row[0])
                #if normNum:
                #    row[0] = normNum
                #    invalidn = True

                #if len(row) > 7:
                #    for i in range (8,len(row)):
                #        normNum = normalizeNumber(row[i])
                #        if normNum:
                #            row[i] = normNum
                #            invalidn = True

                if invalidn:
                    totalTrimZeros += 1

                if len(row) > 6:
                    idx += 1
                    totalRows += 1
                    if not validateName(row[4].strip(), lastName, stopwords):
                        invalidl = True
                        if verbose:
                            print ("Problem with row[%d]last name [%s]" % (idx, row[4]))
                            print (row)

                    if not validateName(row[5].strip(), firstName, stopwords):
                        invalidf = True
                        if verbose:
                            print ("Problem with row[%d]first name [%s]" % (idx, row[5]))
                            print (row)

                    if invalidl and invalidf:
                        badfile.write(curLine)
                        totalBothInvalid += 1
                    elif invalidl:
                        f, l = getFirstLast(row[5], stopwords)

                        #this means we don't have more than 1 valid toke
                        #write to bad.
                        if not l:
                            badfile.write(curLine)
                            totalBad += 1
                        else:
                            # we have enough for 2
                            writeRowToFile(goodfile, row, f, l)
                    elif invalidf:
                        f, l = getFirstLast(row[4], stopwords)

                        #this means we don't have more than 1 valid toke
                        #write to bad.
                        if not l:
                            badfile.write(curLine)
                            totalBad += 1
                        else:
                            # we have enough for 2
                            writeRowToFile(goodfile, row, f, l)
                    else:
                        # if both are good, trim down to only 1 element
                        rl = row[4]
                        rf = row[5]
                        lf, ll = getFirstLast(rl, stopwords)
                        ff, fl = getFirstLast(rf, stopwords)
                        #print ("org f:%s; l:%s" % (rf, rl))
                        #print ("last f:%s; l:%s" % (lf, ll))
                        #print ("first f:%s; l:%s" % (ff, fl))

                        # in case something went wrong, safety net
                        if not lf:
                            lf = rl

                        if not ff:
                            ff = rf

                        if ll:
                            writeRowToFile(goodfile, row, ff, ll)
                        else:
                            if len(ff) == len(rf) and len(lf) == len(rl):
                                goodfile.write(curLine)
                                totalGood += 1
                            else:
                                writeRowToFile(goodfile, row, ff, lf)

        #except Exception as e:
        #    print ("CSV error:%s" % str(e))
        #    print ("The last known row index before the issue: %d" % idx)


    goodfile.close()
    badfile.close()


def main():
    """
        Main
    """

    usage = "%(prog)s [options]"
    description = "Preprocess PDD CSV file"

    parser = argparse.ArgumentParser(usage=usage,
                                     description=description)

    parser.add_argument("-t", "--txt",
                        type=str,
                        default='',
                        help="CSV file")

    parser.add_argument("-s", "--stopwords",
                        type=str,
                        default='',
                        help="List of stop words")

    parser.add_argument("-o", "--outfile",
                        type=str,
                        default='',
                        help="Generate output record files - outfile.good and outfile.bad")

    parser.add_argument("-c", "--columns",
                        type=int,
                        default=0,
                        help="First row is column headers")

    parser.add_argument("-v", "--verbose",
                        default=False,
                        action='store_true',
                        help="Print invalid row's data.")

    args = parser.parse_args()

    stopwords = []
    if len(args.stopwords) > 0:
        with open(args.stopwords) as stopfile:
            line = stopfile.readline()
            while line:
                stopwords.append(line.strip().upper())
                line = stopfile.readline()


    parseCSV(args.txt, stopwords, args.columns, args.verbose, args.outfile)

    # print stats
    print ("Total Rows:%d" % totalRows)

    if len(args.outfile) > 0:
        print ("Total Good Rows Written:%d" % totalGood)
        print ("Total Bad Rows Written:%d" % totalBad)
        print ("Total Good modified Written:%d" % totalModifiedGood)
        print ("Total prefix 0s removed:%d" % totalTrimZeros)
    print ("Total both invalid:%d" % totalBothInvalid)
    lastName.printStats("Invalid Last name")
    firstName.printStats("Invalid First name")


if __name__== "__main__":
    main()
