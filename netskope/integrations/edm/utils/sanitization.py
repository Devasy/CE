"""Contain Methods for generating .good and .bad file and data sanitization."""
import csv
import re


class EdmSetting(object):
    """Edm settings from config json file."""

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

    def __init__(self, json_cfg):
        """Init method."""
        self.stopwords = []
        self.colmap = {}
        self.col_names = []
        self.json_cfg = json_cfg
        columns = self.get_columns()
        self.load_stopwords()
        index = 0
        # build map from column name to column index
        for column in columns:
            self.col_names.append(column)
            self.colmap[column] = index
            index += 1

    def get_columns(self):
        """Get list of column names."""
        if self.TAG_COLUMNS in self.json_cfg:
            return self.json_cfg[self.TAG_COLUMNS]
        return []

    def get_delimiter(self):
        """Get delimiter."""
        if self.TAG_DELIMITER in self.json_cfg:
            return self.json_cfg[self.TAG_DELIMITER]
        return ','

    def get_encoding(self):
        """Get encoding string."""
        if self.TAG_ENCODING in self.json_cfg:
            return self.json_cfg[self.TAG_ENCODING]
        return 'utf-8'

    def get_has_column_header(self):
        """Get if csv file has column headers."""
        if self.TAG_HAS_COL_HDR in self.json_cfg:
            return self.json_cfg[self.TAG_HAS_COL_HDR]
        return False

    def get_lower_case_names(self):
        """Get flag for normalize names to lower case."""
        if self.TAG_NAMES_LOWERCASE in self.json_cfg:
            return self.json_cfg[self.TAG_NAMES_LOWERCASE]
        return False

    def get_strip_leading_zeros(self):
        """Get flag for how leading 0s to strip."""
        if self.TAG_IDS_STRIP_ZERO in self.json_cfg:
            return self.json_cfg[self.TAG_IDS_STRIP_ZERO]
        return 0

    def get_stopwords(self):
        """Get stopword file to use."""
        return self.stopwords

    def get_name_columns(self):
        """Get list of name columns."""
        columns = []
        if self.TAG_NAMES in self.json_cfg:
            names = self.json_cfg[self.TAG_NAMES]
            for name in names:
                columns.append(self.colmap[name])
        return columns

    def get_id_columns(self):
        """Get list of number/id columns."""
        columns = []
        if self.TAG_NORM_IDS in self.json_cfg:
            ids = self.json_cfg[self.TAG_NORM_IDS]
            for column_id in ids:
                columns.append(self.colmap[column_id])
        return columns

    def lookup_column_name(self, idx):
        """Get column name by column index - 0 based."""
        if idx >= len(self.col_names):
            return ''
        return self.col_names[idx]

    def get_primary_2nd_columns(self):
        """Get list of primary and secondary columns pairing."""
        id_grps = []
        if self.TAG_PRIM_2ND_IDS in self.json_cfg:
            grps = self.json_cfg[self.TAG_PRIM_2ND_IDS]
            for grp in grps:
                id_grp = []
                for column in grp:
                    id_grp.append(self.colmap[column])
                id_grps.append(id_grp)
        return id_grps

    def load_stopwords(self):
        """Get list of stop words."""
        self.stopwords = []
        if self.TAG_STOPWORDS in self.json_cfg:
            if len(self.json_cfg[self.TAG_STOPWORDS]) > 0:
                with open(self.json_cfg[self.TAG_STOPWORDS], encoding="UTF-8") as stopfile:
                    line = stopfile.readline()
                    while line:
                        self.stopwords.append(line.strip().upper())
                        line = stopfile.readline()
        return self.stopwords


class NameDataStats(object):
    """Statistic CSV name column data class."""

    def __init__(self):
        """Init Method."""
        self.empty = 0
        self.one_char = 0
        self.has_digit = 0
        self.single_after_punc = 0
        self.stopwords = 0
        self.max_chars = 0
        self.max_words = 0

    def setmax_chars(self, name):
        """Set the longest (in char and word count) data in a column."""
        size = len(name)
        if size > self.max_chars:
            self.max_chars = size

        words = name.split()
        size = len(words)
        if size > self.max_words:
            self.max_words = size


class Primary2ndDataStats(object):
    """Statistic CSV number/ID data class."""

    def __init__(self):
        """Init method."""
        self.mismatched = 0
        self.empty = 0
        self.max_chars = 0

    def setmax_chars(self, name):
        """Set the longest (in char) data in a column."""
        size = len(name)
        if size > self.max_chars:
            self.max_chars = size


class RowStatus(object):
    """Statistic CSV row data class."""

    def __init__(self):
        """Init method."""
        self.total_rows = 0
        self.good_rows = 0
        self.bad_rows = 0
        self.total_modified = 0
        self.total_zeros_trimmed = 0
        self.all_names_invalid = 0


def setup_columns_from_csv_file(filename, jcfg):
    """In case column headers are in csv file, load the csv headers instead.

    :param filename: filename of csv file
    :param jcfg: config json settings
    """
    delim = ','
    if EdmSetting.TAG_HAS_COL_HDR in jcfg:
        if jcfg[EdmSetting.TAG_HAS_COL_HDR]:
            if EdmSetting.TAG_DELIMITER in jcfg:
                delim = jcfg[EdmSetting.TAG_DELIMITER]

            with open(filename, 'r', encoding="UTF-8") as file_pointer:
                hdr = file_pointer.readline()
                data = csv.reader([hdr],
                                  delimiter=delim,
                                  quoting=csv.QUOTE_NONE)

                if data:
                    for row in data:
                        jcfg[EdmSetting.TAG_COLUMNS] = row


def validate_name(data, stats, stopwords):
    """
    Invalidate a row cell based on the following criteria.

    - Remove 1 character field.
    - Remove 1 character after removing all non-alphanumeric characters.
    - Remove any digit in the cell.
    - Check if the entire cell matches any stopword in the provided list.

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
            stats.one_char += 1
        return False

    # no digit in name
    if any(i.isdigit() for i in data):
        if stats:
            stats.has_digit += 1
        return False

    # single letter after stripping punctations
    tmp = re.sub(r'\W+', '', data)
    if len(tmp) < 2:
        if stats:
            stats.single_after_punc += 1
        return False

    # words in stopwords list
    if data.upper() in stopwords:
        if stats:
            stats.stopwords += 1
        return False

    return True


def exam_prim_2nd_number(data1, data2, stats, strip_leading_zeros):
    """
    Given 2 cells, data1 is primary and data2 is secondary.

    :param data1: the cell from primary column
    :param data2: the cell from secondary column
    :param stats: Primary2ndDataStats stats object
    :param strip_leading_zeros: How many leading zeros to strip.

    :return: return True of passed
    """
    if not data1 or not data2:
        stats.empty += 1
        return False

    norm_data_2 = normalize_number(data2, strip_leading_zeros)
    if data1 != norm_data_2:
        stats.mismatched += 1
        return False

    return True


def normalize_number(num, strip_leading_zeros):
    """
    Remove all leading zeros from number fields.

    :param num: number cell
    :param strip_leading_zeros: How many leading zeros to strip.

    :return: None if there is no leading 0s. Else stripped num.
    """
    modified = False
    if num:
        if strip_leading_zeros > 0 and strip_leading_zeros < len(num):
            # strip padding 0
            count = 0
            for i in range(strip_leading_zeros):
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
    Read a line from CSV file.

    :param unicode_csv_data: CSV file data
    :param encoding: char encoding of CSV file data

    :return: CSV line
    """
    global CURLINE
    _ = unicode_csv_data.tell()
    while True:
        line = unicode_csv_data.readline()
        if not line:
            return
        _ = unicode_csv_data.tell()
        CURLINE = line
        yield line.decode(encoding)


class EdmInputProcessor(object):
    """EDM sanitize sub command class."""

    def __init__(self):
        """Init Method."""
        self.setting = None
        self.name_stats = dict()
        self.row_stats = RowStatus()

    def get_config(self):
        """
        Get config json setting.

        :return: config setting
        """
        return self.setting

    def write_row_to_file(self, outfile, delim, row):
        """
        Write a row to a file with replacement row data.

        :param outfile: CSV to write row data to
        :param delim: csv file delimiter to use
        :param row: row data
        """
        data = ''
        i = 0
        for cell in row:
            if i > 0:
                data += delim

            data += cell
            i += 1

        outfile.write(data.encode('utf-8'))
        outfile.write(b'\n')
        self.row_stats.good_rows += 1

    def normalize_id(self, row, idx, strip_leading_zeros):
        """
        Normalize Number/ID column data.

        :param row: data for the entire row
        :param idx: zero based index to number/id column
        :param strip_leading_zeros: number of leading zeros to strip.

        :return: True if cell is modifiex, else False
        """
        num, modified = normalize_number(row[idx], strip_leading_zeros)

        if modified:
            row[idx] = num

        return modified

    def normalize_names(self, row, idx):
        """
        Normalize name column data.

        :param row: data for the entire row
        :param idx: zero based index to number/id column
        """
        name = row[idx].lower()
        row[idx] = name

    def parse_csv(self, filename, outfile):
        """
        Process data CSV file.

        :param filename: CSV data filename
        :param outfile: prefix for output files.
        """
        goodfile = None
        badfile = None
        lowercase_name = self.setting.get_lower_case_names()
        strip_leading_zeros = self.setting.get_strip_leading_zeros()
        delim = self.setting.get_delimiter()
        encoding = self.setting.get_encoding()
        name_cols = self.setting.get_name_columns()
        num_cols = self.setting.get_id_columns()

        if len(outfile) > 0:
            goodfile = open(outfile+".good", "wb")
            badfile = open(outfile+".bad", "wb")

        # failed to open good/bad files, return
        if not goodfile or not badfile:
            print("Failed to create good or bad files.")
            if goodfile:
                goodfile.close()
            if badfile:
                badfile.close()
            return

        with open(filename, 'rb') as file_pointer:

            # if first row is column, remember and skip
            if self.setting.get_has_column_header():
                hdr = file_pointer.readline()
                goodfile.write(hdr)
                badfile.write(hdr)

            num_of_cols = len(self.setting.get_columns())

            data = csv.reader(char_encoder(file_pointer, encoding),
                              delimiter=delim,
                              quoting=csv.QUOTE_NONE)

            if data:
                for row in data:

                    if len(row) < 1:
                        # empty row
                        continue

                    # make sure each row has the same number of columns
                    if len(row) != num_of_cols:
                        print(f"Mismatched number of columns at {self.row_stats.total_rows}")
                        continue

                    self.row_stats.total_rows += 1

                    # check name columns
                    invalid_name = False
                    for name in name_cols:
                        if name not in self.name_stats:
                            self.name_stats[name] = NameDataStats()
                        stats = self.name_stats[name]
                        stats.setmax_chars(row[name])

                        if not validate_name(row[name], stats, self.setting.stopwords):
                            invalid_name = True
                            break

                        if lowercase_name:
                            self.normalize_names(row, name)

                    if invalid_name:
                        self.row_stats.bad_rows += 1
                        badfile.write(CURLINE)
                        continue

                    # check primary and secondary columns
                    modified = False
                    for num_column in num_cols:
                        if self.normalize_id(row, num_column, strip_leading_zeros):
                            modified = True

                    if modified:
                        self.row_stats.total_modified += 1

                    # now write to good file
                    self.write_row_to_file(goodfile, delim, row)

        goodfile.close()
        badfile.close()


gSanitizer = EdmInputProcessor()


def run_sanitizer(csv_path, output_path, emd_data_config):
    """
    Process sanitize sub command's program arguments.

    :param args: input arguments
    """
    setup_columns_from_csv_file(csv_path, emd_data_config)
    gSanitizer.setting = EdmSetting(emd_data_config)

    gSanitizer.parse_csv(filename=csv_path, outfile=output_path)
