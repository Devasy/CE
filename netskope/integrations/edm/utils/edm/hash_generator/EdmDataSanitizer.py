# flake8: noqa
"""
    EdmDataSanitizer.py
    ~~~~~~~~~~~~~~~~~~~~

    Preprocess input csv file and split the good records
    and bad records into <output>.good and <output>.bad files.
    Then, user should use the <output>.good file for PDD hashing.

    :copyright: (c) 2020 Netskope, Inc. All rights reserved.
    ..author: Wai Yim<wyim@netskope.com> (Oct, 2020)
"""
import csv
import re
import json
import argparse


class EdmSetting(object):
    """
        Edm settings from config json file
    """
    TAG_COLUMNS = 'columns'
    TAG_NAMES = 'names'
    TAG_DELIMITER = 'delimiter'
    TAG_ENCODING = 'encoding'
    TAG_HAS_COL_HDR = 'has-column-header'
    TAG_NAMES_LOWERCASE = 'names-lowercase'
    TAG_IDS_STRIP_ZERO = 'ids-strip-leading-zeros'
    TAG_NORM_IDS = 'normalize-ids'
    TAG_PRIM_2ND_IDS = 'primary-secondary-ids'
    TAG_STOPWORDS = 'stopwords'

    def __init__(self, jsonCfg):

        self.stopwords = []
        self.colmap = {}
        self.colNames = []
        self.jsonCfg = jsonCfg
        columns = self.getColumns()
        self.loadStopwords()
        index = 0
        # build map from column name to column index
        for c in columns:
            self.colNames.append(c)
            self.colmap[c] = index
            index += 1

    def getColumns(self):
        """
            get list of column names
        """
        if self.TAG_COLUMNS in self.jsonCfg:
            return self.jsonCfg[self.TAG_COLUMNS]
        return []

    def getDelimiter(self):
        """
            get delimiter
        """
        if self.TAG_DELIMITER in self.jsonCfg:
            return self.jsonCfg[self.TAG_DELIMITER]
        return ','

    def getEncoding(self):
        """
            get encoding string
        """
        if self.TAG_ENCODING in self.jsonCfg:
            return self.jsonCfg[self.TAG_ENCODING]
        return 'utf-8'

    def getHasColumnHeader(self):
        """
            get if csv file has column headers
        """
        if self.TAG_HAS_COL_HDR in self.jsonCfg:
            return self.jsonCfg[self.TAG_HAS_COL_HDR]
        return False

    def getLowerCaseNames(self):
        """
            get flag for normalize names to lower case
        """
        if self.TAG_NAMES_LOWERCASE in self.jsonCfg:
            return self.jsonCfg[self.TAG_NAMES_LOWERCASE]
        return False

    def getStripLeadingZeros(self):
        """
            get flag for how leading 0s to strip
        """
        if self.TAG_IDS_STRIP_ZERO in self.jsonCfg:
            return self.jsonCfg[self.TAG_IDS_STRIP_ZERO]
        return 0

    def getStopwords(self):
        """
            get stopword file to use
        """
        return self.stopwords

    def getNameColumns(self):
        """
            get list of name columns
        """
        columns= []
        if self.TAG_NAMES in self.jsonCfg:
            names = self.jsonCfg[self.TAG_NAMES]
            for n in names:
                columns.append(self.colmap[n])
        return columns

    def geIdColumns(self):
        """
            get list of number/id columns
        """
        columns= []
        if self.TAG_NORM_IDS in self.jsonCfg:
            ids = self.jsonCfg[self.TAG_NORM_IDS]
            for n in ids:
                columns.append(self.colmap[n])
        return columns

    def lookupColumnName(self, idx):
        """
            get column name by column index - 0 based
        """
        if idx >= len(self.colNames):
            return ''
        return self.colNames[idx]

    def getPrimary2ndColumns(self):
        """
            get list of primary and secondary columns pairing
        """
        idGrps= []
        if self.TAG_PRIM_2ND_IDS in self.jsonCfg:
            grps = self.jsonCfg[self.TAG_PRIM_2ND_IDS]
            for grp in grps:
                idGrp = []
                for c in grp:
                    idGrp.append(self.colmap[c])
                idGrps.append(idGrp)
        return idGrps

    def loadStopwords(self):
        """
            get list of stop words
        """
        self.stopwords = []
        if self.TAG_STOPWORDS in self.jsonCfg:
            if len(self.jsonCfg[self.TAG_STOPWORDS]) > 0:
                with open(self.jsonCfg[self.TAG_STOPWORDS]) as stopfile:
                    line = stopfile.readline()
                    while line:
                        self.stopwords.append(line.strip().upper())
                        line = stopfile.readline()
        return self.stopwords


class NameDataStats(object):
    """
        Statistic CSV name column data class.
    """
    def __init__(self):
        self.empty = 0
        self.oneChar = 0
        self.hasDigit = 0
        self.singleAfterPunc = 0
        self.stopwords = 0
        self.maxChars = 0
        self.maxWords = 0

    def setMaxChars(self, name):
        """
            set the longest (in char and word count) data in a column
        """
        size = len(name)
        if size > self.maxChars:
            self.maxChars = size

        words = name.split()
        size = len(words)
        if size > self.maxWords:
            self.maxWords = size

    def printStats(self, msg):
        """
            Print stats
            :param msg: Message to print along with stats
        """
        print ("==================")
        print ("%s:" % msg)
        print ("Total empty: %d" % self.empty)
        print ("Total single char: %d" % self.oneChar)
        print ("Total has digit: %d" % self.hasDigit)
        print ("Total single letter - strip punc: %d" % self.singleAfterPunc)
        print ("Total stopwords: %d" % self.stopwords)
        print ("Total longest length: %d" % self.maxChars)
        print ("Total most words: %d" % self.maxWords)


class Primary2ndDataStats(object):
    """
        Statistic CSV number/ID data class.
    """
    def __init__(self):
        self.mismatched = 0
        self.empty = 0
        self.maxChars = 0

    def setMaxChars(self, name):
        """
            set the longest (in char) data in a column
        """
        size = len(name)
        if size > self.maxChars:
            self.maxChars = size

    def printStats(self, msg):
        """
            Print stats
            :param msg: Message to print along with stats
        """
        print ("==================")
        print ("%s:" % msg)
        print ("Total mismatched: %d" % self.mismatched)
        print ("Total empty: %d" % self.empty)
        print ("Total longest length: %d" % self.maxChars)


class RowStats(object):
    """
        Statistic CSV row data class.
    """
    def __init__(self):
        self.totalRows = 0
        self.goodRows = 0
        self.badRows = 0
        self.totalModified = 0
        self.totalZerosTrimmed = 0
        self.allNamesInvalid = 0

    def printStats(self, msg):
        """
            Print stats
            :param msg: Message to print along with stats
        """
        print ("==================")
        print ("%s:" % msg)
        print ("Total rows: %d" % self.totalRows)
        print ("Total good rows: %d" % self.goodRows)
        print ("Total bad rows: %d" % self.badRows)
        print ("Total modified rows: %d" % self.totalModified)
        #print ("Total zeros trimmed rows: %d" % self.totalZerosTrimmed)
        print ("Total all names invalid rows: %d" % self.allNamesInvalid)


def setupColumnsFromCsvFile(filename, jcfg):
    """
        In case column headers are in csv file,
        load the csv headers instead.
        :param filename: filename of csv file
        :param jcfg: config json settings
    """
    delim = ','
    if EdmSetting.TAG_HAS_COL_HDR in jcfg:
        if jcfg[EdmSetting.TAG_HAS_COL_HDR]:
            if EdmSetting.TAG_DELIMITER in jcfg:
                delim = jcfg[EdmSetting.TAG_DELIMITER]

            with open(filename, 'r') as f:
                hdr = f.readline()
                data = csv.reader([hdr],
                                delimiter=delim,
                                quoting=csv.QUOTE_NONE)

                if data:
                    for row in data:
                        jcfg[EdmSetting.TAG_COLUMNS] = row


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
        if stats:
            stats.empty += 1
        return False

    if len(data) == 0:
        # skip empty
        if stats:
            stats.empty += 1
        return False

    # no single letter/digit
    if len(data) == 1:
        if stats:
            stats.oneChar += 1
        return False

    # no digit in name
    if any(i.isdigit() for i in data):
        if stats:
            stats.hasDigit += 1
        return False

    # single letter after stripping punctations
    tmp = re.sub(r'\W+', '', data)
    if len(tmp) < 2:
        if stats:
            stats.singleAfterPunc += 1
        return False

    # words in stopwords list
    if data.upper() in stopwords:
        if stats:
            stats.stopwords += 1
        return False

    return True


def examPrim2ndNumber(data1, data2, stats, stripLeadingZeros):
    """
        Given 2 cells, data1 is primary and
        data2 is secondary.
        :param data1: the cell from primary column
        :param data2: the cell from secondary column
        :param stats: Primary2ndDataStats stats object
        :param stripLeadingZeros: How many leading zeros to strip.
        :return: return True of passed
    """
    if not data1 or not data2:
        stats.empty += 1
        return False

    normData2 = normalizeNumber(data2, stripLeadingZeros)
    if data1 != normData2:
        stats.mismatched += 1
        return False

    return True


def normalizeNumber(num, stripLeadingZeros):
    """
        remove all leading zeros from number fields
        :param num: number cell
        :param stripLeadingZeros: How many leading zeros to strip.
        :return: None if there is no leading 0s. Else stripped num.
    """
    modified = False
    if num:
        if stripLeadingZeros > 0 and stripLeadingZeros < len(num):
            # strip padding 0
            count = 0
            for i in range(stripLeadingZeros):
                if num[i] != '0':
                    break
                count += 1

            if count > 0:
                num = num[count:]
                modified = True

        norm = num.replace('-', '').replace('.', '').replace(' ', '')

        if len(norm) != len(num):
            num = norm
            modified = True


    return num, modified

CURLINE = None

def char_encoder(unicode_csv_data, encoding):
    """
        Read a line from CSV file
        :param unicode_csv_data: CSV file data
        :param encoding: char encoding of CSV file data
        :return: CSV line
    """
    global CURLINE
    cursor = unicode_csv_data.tell()
    while True:
        line = unicode_csv_data.readline()
        if not line:
            return
        cursor = unicode_csv_data.tell()
        CURLINE = line
        yield line.decode(encoding)


class EdmInputChecker(object):
    """
        EDM check sub command class
    """

    def __init__(self):
        self.setting = None
        self.NameStats = dict()
        self.NumberStats = dict()
        self.RowStats = RowStats()


    def setArgsParser(self, subparsers):
        """
            Set subcommand argument parser
            :param subparsers: argumeent subparser
        """
        description = 'Assess quality of input data <data.csv> \
                      and generate a report with no other changes \
                      or impact to any data.'

        check = subparsers.add_parser('check',
                                      description=description,
                                      help=description)

        check.add_argument("file",
                            type=str,
                            default='',
                            help="CSV file")

        check.add_argument("-c", "--config",
                            type=str,
                            default='',
                            required=True,
                            help="Configuration json file")

        check.set_defaults(func=runChecker)


    def getConfig(self):
        """
            Get config json setting
            :return: config setting
        """
        return self.setting


    def printStats(self):
        """
            Print stats results for checker object
        """
        for idx, stats in self.NameStats.items():
            colname = self.setting.lookupColumnName(idx)
            stats.printStats("Name stats for column[%s]:" % colname)
        for idx, stats in self.NumberStats.items():
            colname = self.setting.lookupColumnName(idx)
            stats.printStats("Primary and Secondary stats for column[%s]:" % colname)
        self.RowStats.printStats("Overall Stats")


    def normalizeID(self, row, idx, stripLeadingZeros):
        """
            Normalize Number/ID column data
            :param row: data for the entire row
            :param idx: zero based index to number/id column
            :param stripLeadingZeros: number of leading zeros to strip.
            :return: True if cell is modified, else False
        """
        # for a given Number cell, strip leading 0s
        num, modified = normalizeNumber(row[idx], stripLeadingZeros)

        if modified:
            row[idx] = num

        return modified


    def parseCSV(self, filename):
        """
            Process data CSV file
            :param filename: CSV data filename
        """
        idx = 0
        stripLeadingZeros = self.setting.getStripLeadingZeros()
        delim = self.setting.getDelimiter()
        encoding = self.setting.getEncoding()
        nameCols = self.setting.getNameColumns()
        numCols = self.setting.geIdColumns()
        prim2ndCols = self.setting.getPrimary2ndColumns()

        with open(filename, 'rb') as f:

            # if first row is column, remember and skip
            if self.setting.getHasColumnHeader():
                hdr = f.readline()

            numOfCols = len(self.setting.getColumns())

            data = csv.reader(char_encoder(f, encoding),
                              delimiter=delim,
                              quoting=csv.QUOTE_NONE)

            if data:
                for row in data:

                    if len(row) < 1:
                        #empty row
                        continue

                    # make sure each row has the same number of columns
                    if len(row) != numOfCols:
                        print("Mismatched number of columns at %d" % self.RowStats.totalRows)
                        continue

                    self.RowStats.totalRows += 1

                    # check name columns
                    allNamesInvalid = True
                    invalidName = False
                    for name in nameCols:
                        if not name in self.NameStats:
                            self.NameStats[name] = NameDataStats()
                        stats = self.NameStats[name]
                        stats.setMaxChars(row[name])
                        if validateName(row[name], stats, self.setting.stopwords):
                            allNamesInvalid = False
                        elif not invalidName:
                            invalidName = True

                    if allNamesInvalid:
                        self.RowStats.allNamesInvalid += 1

                    if invalidName:
                        self.RowStats.badRows += 1
                    else:
                        self.RowStats.goodRows += 1

                    # check primary and secondary columns
                    for numGrp in prim2ndCols:
                        if len(numGrp) > 1:
                            if not numGrp[1] in self.NumberStats:
                                self.NumberStats[numGrp[1]] = Primary2ndDataStats()
                            stats = self.NumberStats[numGrp[1]]
                            stats.setMaxChars(row[numGrp[1]])
                            examPrim2ndNumber(row[numGrp[0]], row[numGrp[1]], stats, stripLeadingZeros)

                    # check primary and secondary columns
                    modified = False
                    for numColumn in numCols:
                        if self.normalizeID(row, numColumn, stripLeadingZeros):
                            modified = True

                    if modified:
                        self.RowStats.totalModified += 1


def runChecker(args):
    """
        Process check sub command's program arguments
        :param args: input arguments
    """
    with open(args.config) as fp:
        jcfg = json.load(fp)
        setupColumnsFromCsvFile(args.file, jcfg)
        gChecker.setting = EdmSetting(jcfg)

    print("==================")
    print("Columns:", gChecker.getConfig().getColumns())
    print("Names:", gChecker.getConfig().getNameColumns())
    print("Num groups:", gChecker.getConfig().getPrimary2ndColumns())
    print("==================")

    gChecker.parseCSV(args.file)
    gChecker.printStats()


class EdmInputProcessor(object):
    """
        EDM sanitize sub command class
    """

    def __init__(self):
        self.setting = None
        self.NameStats = dict()
        self.RowStats = RowStats()


    def setArgsParser(self, subparsers):
        """
            Set subcommand argument parser
            :param subparsers: argumeent subparser
        """
        description = 'Execute sanitization code and \
                      generate <data.csv>.good and \
                      <data.csv>.bad files.'

        sanitize = subparsers.add_parser('sanitize',
                    description=description,
                    help=description)

        sanitize.add_argument("file",
                            type=str,
                            default='',
                            help="CSV file")

        sanitize.add_argument("-c", "--config",
                            type=str,
                            default='',
                            required=True,
                            help="Configuration json file")

        sanitize.add_argument("-o", "--outfile",
                            type=str,
                            default='',
                            required=True,
                            help="Generate output record files - outfile.good and outfile.bad")

        sanitize.set_defaults(func=runSanitizer)


    def getConfig(self):
        """
            Get config json setting
            :return: config setting
        """
        return self.setting


    def printStats(self):
        """
            Print stats results for sanitizer object
        """
        for idx, stats in self.NameStats.items():
            colname = self.setting.lookupColumnName(idx)
            stats.printStats("Name stats for column[%s]:" % colname)
        self.RowStats.printStats("Overall Stats")


    def writeRowToFile(self, outfile, delim, row):
        """
            Write a row to a file with replacement row data.
            :param outfile: CSV to write row data to
            :param delim: csv file delimiter to use
            :param row: row data
        """
        data = u''
        i = 0
        for cell in row:
            if i > 0:
                data += delim

            data += cell
            i += 1

        outfile.write(data.encode('utf-8'))
        outfile.write(b'\n')
        self.RowStats.goodRows += 1

    def normalizeID(self, row, idx, stripLeadingZeros):
        """
            Normalize Number/ID column data
            :param row: data for the entire row
            :param idx: zero based index to number/id column
            :param stripLeadingZeros: number of leading zeros to strip.
            :return: True if cell is modifiex, else False
        """
        num, modified = normalizeNumber(row[idx], stripLeadingZeros)

        if modified:
            row[idx] = num

        return modified

    def normalizeNames(self, row, idx):
        """
            Normalize name column data
            :param row: data for the entire row
            :param idx: zero based index to number/id column
        """
        name = row[idx].lower()
        row[idx] = name


    def parseCSV(self, filename, outfile):
        """
            Process data CSV file
            :param filename: CSV data filename
            :param outfile: prefix for output files.
        """
        idx = 0
        goodfile = None
        badfile = None
        lowercaseName = self.setting.getLowerCaseNames()
        stripLeadingZeros = self.setting.getStripLeadingZeros()
        delim = self.setting.getDelimiter()
        encoding = self.setting.getEncoding()
        nameCols = self.setting.getNameColumns()
        numCols = self.setting.geIdColumns()

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
            if self.setting.getHasColumnHeader():
                hdr = f.readline()
                goodfile.write(hdr)
                badfile.write(hdr)

            numOfCols = len(self.setting.getColumns())

            data = csv.reader(char_encoder(f, encoding),
                              delimiter=delim,
                              quoting=csv.QUOTE_NONE)

            if data:
                for row in data:

                    if len(row) < 1:
                        #empty row
                        continue

                    # make sure each row has the same number of columns
                    if len(row) != numOfCols:
                        print("Mismatched number of columns at %d" % self.RowStats.totalRows)
                        continue

                    self.RowStats.totalRows += 1

                    # check name columns
                    invalidName = False
                    for name in nameCols:
                        if not name in self.NameStats:
                            self.NameStats[name] = NameDataStats()
                        stats = self.NameStats[name]
                        stats.setMaxChars(row[name])

                        if not validateName(row[name], stats, self.setting.stopwords):
                            invalidName = True
                            break

                        if lowercaseName:
                            self.normalizeNames(row, name)

                    if invalidName:
                        self.RowStats.badRows += 1
                        badfile.write(CURLINE)
                        continue

                    # check primary and secondary columns
                    modified = False
                    for numColumn in numCols:
                        if self.normalizeID(row, numColumn, stripLeadingZeros):
                            modified = True

                    if modified:
                        self.RowStats.totalModified += 1

                    # now write to good file
                    self.writeRowToFile(goodfile, delim, row)

        goodfile.close()
        badfile.close()


def runSanitizer(args):
    """
        Process sanitize sub command's program arguments
        :param args: input arguments
    """
    with open(args.config) as fp:
        jcfg = json.load(fp)
        setupColumnsFromCsvFile(args.file, jcfg)
        gSanitizer.setting = EdmSetting(jcfg)

    print("==================")
    print("Columns:", gSanitizer.getConfig().getColumns())
    print("Names:", gSanitizer.getConfig().getNameColumns())
    print("Num groups:", gSanitizer.getConfig().getPrimary2ndColumns())
    print("==================")

    gSanitizer.parseCSV(args.file, args.outfile)
    gSanitizer.printStats()


gChecker = EdmInputChecker()
gSanitizer = EdmInputProcessor()


def main():
    """
        Main
    """
    usage = "%(prog)s <command> [options] <data.csv>"
    description = "Preprocess EDM CSV file"

    parser = argparse.ArgumentParser(usage=usage,
                                     description=description)

    subparsers = parser.add_subparsers(title='Commands',
                                       description='EDM CSV file Commands',)
    gChecker.setArgsParser(subparsers)
    gSanitizer.setArgsParser(subparsers)

    args = parser.parse_args()
    args.func(args)


if __name__== "__main__":
    main()
