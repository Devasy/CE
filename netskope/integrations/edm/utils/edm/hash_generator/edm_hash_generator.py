# flake8: noqa
#! /usr/bin/env python
"""
    edm_hash_generator.py
    ~~~~~~~~~~~~~~~~~~~~

    Parse the input csv file and generate the bin files (one per column)

    The processing of this file will be done in parallel and
    the output will be concatenated together in the same order.

    The number of columns is variable and this program will emit as many
    files as the number of columns it encounters. Callers may optionally set
    column names via the cmdline params. If none are specified create dummy
    column names.

    :copyright: (c) 2015-2019 Netskope, Inc. All rights reserved.
..author: Arjun Sambamoorthy<arjuns@netskope.com> (July 31, 2015)
"""
from __future__ import print_function

from builtins import input
from builtins import str
from builtins import range
from builtins import object
import shutil
import hashlib
import json
import uuid
import os
import sys
import csv
import signal
import re
from pathlib import Path
import multiprocessing as mp
import traceback
import tarfile
from io import open

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from netskope.common.utils.logger import Logger
from .dictmgr import DictMgr, DictMgrException

SUPPORT_DICT = True
logger = Logger()

FILE_ENCODING = 'utf-8'
CSV_DELIM = ','
FILE_TMP_PREFIX = 'pdd_tmp_data_'
FILE_PREFIX = 'pdd_data_'

ENCRYPTED_PREFIX = 'enc-'
SALT_PREFIX = 'salt-'

PBKDF2_ITER = 10000
PBKDF2_KEY_LEN = 20

STATUS_MODULO = 100000
MAX_FILENAME_LEN = 240

class PddHashException(Exception):
    """
        A generic exception raise during hash generation
    """
    pass

def _get_line_split(file_size):
    """
        Estimate the file split count for parallel jobs based on the
        file size.
        - 100KB <= 1K lines
        - 10MB <= 10K lines
        - 100MB <= 100K lines
        - Larger than 100MB split by 1M lines
    """
    k_lines = 1000
    if file_size <= 100 * 1024:
        return 1 * k_lines
    elif file_size <= 10 * 1024 * 1024:
        return 10 * k_lines
    elif file_size <= 100 * 1024 * 1024:
        return 100 * k_lines
    else:
        return 1000 * k_lines

def make_int_array(str_array, is_one_based=True):
    """
        Given an array of string numbers, return an array of ints (optionally
        making them 0 based instead of 1 based)
    """
    ret = []
    for item in str_array:
        num = int(item)
        if is_one_based:
            num = num - 1
        ret.append(num)
    return ret

def normalize_num(num):
    """
        Normalize the strings as title (starts with uppercase and
        rest as lowercase) and normalize the numbers by removing
        the dashes, dots and spaces.
    """
    norm = num.replace('-', '').replace('.', '').replace(' ', '')
    return norm

def utf8_encoder(unicode_csv_data, stop_position=None):
    # type: (_io.BufferedReader, int) -> str
    """
        Read data from the given fp till the stop_pos and parse it as utf-8.
        stop_position is the byte offset at which to stop iteration (default
        behavior is to read till end of file)

        :param unicode_csv_data: CSV file opened with binary mode
        :type unicode_csv_data: _io.BufferedReader
        :param stop_position: Position in file where to stop, position is relative
                              to begin and a 0 value means end of file
        :type stop_position: int
        :return: (str) Line from CSV file as a unicode string.
        :rtype: str
    """
    cursor = unicode_csv_data.tell()
    while True:
        line = unicode_csv_data.readline()
        if not line:
            return
        cursor = unicode_csv_data.tell()
        if stop_position and cursor > stop_position:
            return
        yield line.decode(FILE_ENCODING)

def unicode_csv_reader(unicode_csv_data, stop=None, use_quoting=False):
    """
        Copied from python docs. csv module doesn't handle Unicode so
        converting it to UTF-8 and back.
    """
    quoting_mode = csv.QUOTE_NONE
    if use_quoting:
        quoting_mode = csv.QUOTE_ALL
    csv_reader = csv.reader(utf8_encoder(unicode_csv_data, stop), \
                            delimiter=CSV_DELIM, \
                            quoting=quoting_mode)

    for row in csv_reader:
        yield row

def _process_row(row, row_num, hash_salt, out_fps, dict_objs=None,
                 dict_cins=[], norm_nums=[], norm_strs=[],
                 skip_hash=False, remove_quotes=False):
    """
        Process the row data (salt, hash) and emit to the output file(s)

        This code was written to support out_fps as both an array as well as a
        single fp. However we should deprecate use of the latter form. When we
        do, this code can be greatly simplified.
        :type row: list[str]
        :type row_num: int
        :type hash_salt: str
        :type out_fps: list[io._io.BufferedReader] | _io.BufferedReader
        :type dict_objs: dict[int, DictMgr]
        :type dict_cins: list[int]
        :type norm_nums: list[int]
        :type norm_strs: list[int]
        :type skip_hash: bool
        :type remove_quotes: bool
    """
    # For rows where we are generating a case insensitive dictionary we need to
    # normalize the text. For now we are using title casing since older versions
    # of this script used title casing but we can change to other casing
    # patterns as long as we are consistent within a single file generation
    for i in dict_cins:
        row[i] = row[i].title()

    if dict_objs:
        for i, entry in enumerate(row):

            # strip leading and trailing spaces.
            if entry:
                entry = entry.strip()

            if not entry:
                continue

            #if normalize string
            if i in norm_strs:
                entry = entry.title()
            #if normalize number
            elif i in norm_nums:
                entry = normalize_num(entry)

            if i in dict_objs:
                dict_obj = dict_objs[i]
                dict_obj.write_record(entry.encode("utf-8"))

    if skip_hash:
        return

    if isinstance(out_fps, list):
        for i, entry in enumerate(row):

            # strip leading and trailing spaces.
            if entry:
                entry = entry.strip()

            if not entry:
                continue

            #if normalize string
            if i in norm_strs:
                entry = entry.title()
            #if normalize number
            elif i in norm_nums:
                entry = normalize_num(entry)

            salted_entry = entry.encode("utf-8") + hash_salt.encode("utf-8")
            hash_entry = hashlib.sha256(salted_entry).digest()
            final_entry = str(row_num).encode("utf-8") + b':' + hash_entry
            out_fps[i].write(final_entry)
    else:
        output = CSV_DELIM.join(row)
        out_fps.write(output.encode("utf-8") + '\n')

def _close_fps(out_fp):
    """
        Close any open temporary file descriptors
    """
    for fp in out_fp:
        fp.close()

def _dict_cleanup(dict_objs):
    """
        Cleanup the dictionary xml and ecr files
    """
    for dict_obj in dict_objs.values():
        dict_obj.cleanup()

def _dict_cleanup_tmp(dict_objs):
    """
        Cleanup the dictionary xml files
    """
    for dict_obj in dict_objs.values():
        dict_obj.cleanup_tmp()

def _gen_dict(dict_objs):
    """
        Compile the dictionary ecr files
    """
    total = len(dict_objs)
    for i, dict_obj in enumerate(dict_objs.values()):
        print("Compiling dictionary %d/%d" % (i+1, total))
        dict_obj.compile_dict()

def _get_dict_json(dict_objs):
    """
        Get a list of dictionary objects as json
    """
    dict_list = []
    for dict_obj in dict_objs.values():
        dict_data = dict_obj.get_meta_json()
        dict_upload = dict_obj.get_upload_json()
        obj = {}
        obj = dict_data
        obj ['md5sum'] = dict_upload['md5sum']
        obj ['size'] = dict_upload['size']
        dict_list.append(obj)
    return dict_list

def _setup_dict_obj(out_dir, file_name, dict_objs, col_names, col_nums, case, args):
    """
        Setup the dictionary objects
    """
    #cur_path = os.getcwd()
    edk_lic = os.path.join(args.get('edk_lic_dir', ''), "licensekey.dat")
    edk_tool = os.path.join(args.get('edk_tool_dir', ''), "edktool.exe")

    for col_num in col_nums:
        col_name = col_names[col_num]
        dict_obj = DictMgr(out_dir, file_name,
                            elem_name=col_name,
                            elem_id=str(col_num+1), # user facing: 1-based
                            edk_lic=edk_lic,
                            edk_tool=edk_tool,
                            case_sensitive=case)
        dict_objs[col_num] = dict_obj

def _create_genid():
    """
        use Openssl's RAND_bytes(16) to generate salt.
        :return: string of RAND_bytes in UUID format.
    """
    rand_val = os.urandom(16)
    genid = str(uuid.UUID(bytes=rand_val))
    return genid


def process_chunk(inp_file, out_dir, hash_salt, start_row, chunknum,
                start, stop, dict_cs, dict_cins, norm_nums, norm_strs, show_status, colnames, args):
    """
        Parse the csv input file from specified start
        cursor to the stop cursor and write the processed
        output to a temporary file and return the result
        in a JSON format.
    """
    line_count = 0
    skip_count = 0
    row_count = start_row
    out_files = []
    out_fp = []
    log_fp = None
    dict_objs = {}
    numcols = len(colnames)

    try:
        # open all the output files
        prefix_uuid = _create_genid()
        if not args.get('skip_hash'):
            for i in range(1, numcols + 1):
                file_name = FILE_TMP_PREFIX + prefix_uuid + '_' + str(i)
                file_path = os.path.join(out_dir, file_name)
                out_files.append(file_path)
                out_fp.append(open(file_path, 'wb+'))

        # also setup the objects for dict creation
        if SUPPORT_DICT:
            bname = os.path.basename(inp_file).split('.')[0]
            if dict_cs:
                _setup_dict_obj(out_dir, bname, dict_objs, colnames,
                                dict_cs, True, args)
            if dict_cins:
                _setup_dict_obj(out_dir, bname, dict_objs, colnames,
                                dict_cins, False, args)

        with open(inp_file, 'rb') as input_fp:
            input_fp.seek(start)
            for idx, row in enumerate(unicode_csv_reader(input_fp, stop, use_quoting=args.get("remove_quotes", False)), start=1):
                # Exclude lines of invalid length
                if len(row) != numcols:
                    # Note that the row # may not be the row # in the file
                    log_line = "Skipping line [%d] due to wrong element "\
                        "count: \"%s\" (expected: %d)" % (idx, row, numcols)
                    logger.warn(log_line + "\n")
                    skip_count += 1
                    continue
                try:
                    _process_row(row, row_count, hash_salt, out_fp, dict_objs,
                                 dict_cins, norm_nums, norm_strs,
                                 skip_hash=args.get("skip_hash"),
                                 remove_quotes=args.get("remove_quotes"))
                except Exception as err:
                    logger.error(
                        message=f"An error occurred while processing csv rows: {err}",
                        details=traceback.format_exc()
                    )
                    # Note that the row # may not be the row # in the file
                    log_line = "Error: %s processing row number %d; " \
                                "row %r\n" % (str(err), row_count, row)
                    logger.warn(log_line + "\n")
                    skip_count += 1

                # Track the written rows
                row_count += 1
                line_count += 1
                if show_status and not line_count % STATUS_MODULO:
                    logger.debug("Processed and hashed %d records" % (line_count))

            if SUPPORT_DICT:
                _gen_dict(dict_objs)
                _dict_cleanup_tmp(dict_objs)

            out_obj = {}
            out_obj['status'] = 'success'
            out_obj['chunk'] = chunknum
            out_obj['files'] = out_files
            out_obj['lines'] = line_count
            out_obj['skipped'] = skip_count
            out_obj['dict'] = _get_dict_json(dict_objs)
            return json.dumps(out_obj)
    except Exception as err:
        _dict_cleanup(dict_objs)
        logger.error(
            message=f"An error occurred while generating the EDM hash {err}",
            details=traceback.format_exc()
        )
        return '{ \"status\": \"fail\", \"error\": \"%s\"}' % (str(err))
    finally:
        if log_fp:
            log_fp.close()
        _close_fps(out_fp)

class PddHash(object):

    def __init__(self, mode, input_file, out_dir,
                 dict_cs, dict_cins, norm_nums, norm_strs,
                 args, column_names=[],
                 parse_column_names=False,
                 public_key=None):
        self.mode = mode
        self.input_file = input_file
        self.out_dir = out_dir
        self.out_files = []
        self.out_fps = []
        self.cpu_count = mp.cpu_count() - 1
        self.global_gen_id = None
        self.global_edm_id = None
        self.salt = None
        self.proc_results = []
        self.status_line_count = 0
        self.dict_result = []
        self.column_names = []
        self.dict_cs = dict_cs
        self.dict_cins = dict_cins
        self.norm_nums = norm_nums
        self.norm_strs = norm_strs
        self.skip_first_line = False
        self.public_key = public_key
        self.args = args

        if parse_column_names:
            self._init_column_names()
            self.skip_first_line = True
        else:
            self.column_names = column_names

        if len(self.column_names) <= 0:
            raise PddHashException("No column names inferred/specified")

        for idx in dict_cs:
            if idx > len(self.column_names):
                raise Exception("Invalid column number specified for dict_cs")
        for idx in dict_cins:
            if idx > len(self.column_names):
                raise Exception("Invalid column number specified for dict_cins")
        for idx in norm_nums:
            if idx > len(self.column_names):
                raise Exception("Invalid column number specified for normalized number")
        for idx in norm_strs:
            if idx > len(self.column_names):
                raise Exception("Invalid column number specified for normalized string")


        self._init_salt()
        self._setup_signals()

    @staticmethod
    def _signal_handler(signum, stack):
        """
            Signal handler
        """
        logger.debug("Performing cleanup before terminating")
        raise PddHashException("user interruption")

    def _init_column_names(self):
        """
            Initialize the column names from the first line of the input file.
        """
        with open(self.input_file, 'rb') as input_fp:
            cols = next(unicode_csv_reader(input_fp, use_quoting=self.args.get("remove_quotes", False)))
            for c in cols:
                self.column_names.append(c.strip())

    def _init_salt(self):
        """
            Create generation id using openssl's RAND_bytes and setup the
            salt for sha256 hash.
        """
        self.global_gen_id = _create_genid()
        self.global_edm_id = _create_genid()
        # Use the following to use a predictable UUID for comparisons in tests.
        #self.global_gen_id = str(uuid.UUID('{12345678-1234-5678-1234-567812345678}'))
        global_gen_id_bytes = self.global_gen_id.encode('utf-8')
        pbkdf_salt = hashlib.sha256(global_gen_id_bytes).hexdigest()

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA1(),
            length=PBKDF2_KEY_LEN,
            salt=pbkdf_salt.encode('utf-8'),
            iterations=PBKDF2_ITER)
        key = kdf.derive(global_gen_id_bytes)

        if hasattr(key, 'hex'):
            self.salt = key.hex()
        else:
            self.salt = ''.join([ key[i].encode("hex") for i in range(len(key)) ])

    def _setup_signals(self):
        """
            Register the signal handlers
        """
        # signal.signal(signal.SIGTERM, self._signal_handler)
        # signal.signal(signal.SIGINT, self._signal_handler)
        # signal.signal(signal.SIGQUIT, self._signal_handler)
        # signal.signal(signal.SIGPIPE, self._signal_handler)

        try:
            if not self.args.get("skip_hash"):
                fname = os.path.basename(self.input_file).split('.')[0]
                for i in range(1, len(self.column_names) + 1):
                    file_name = FILE_PREFIX + fname + '_' + str(i) + '.bin'
                    file_path = os.path.join(self.out_dir, file_name)
                    self.out_files.append(file_path)
                    self.out_fps.append(open(file_path, 'wb+'))
        except IOError:
            raise PddHashException("Failed opening final output file " \
                                    "at dir %s" % (self.out_dir))

    def _cleanup_files(self):
        """
            Cleanup the generated temporary files that may be left behind by
            dead (multiprocess) children.
        """
        files = [ f for f in os.listdir(self.out_dir) \
                    if os.path.isfile(os.path.join(self.out_dir, f)) ]
        for fname in files:
            file_path = os.path.join(self.out_dir, fname)
            prefix = os.path.join(self.out_dir, FILE_TMP_PREFIX)
            if file_path.find(prefix) == 0:
                try:
                    os.remove(file_path)
                except Exception as rmError:
                    raise rmError

    def _fill_empty_hash_files(self):
        empty_cols = []
        for out_fps in self.out_fps:
            org_pos = out_fps.tell()
            out_fps.seek(0, os.SEEK_END)
            fsize = out_fps.tell()
            # restore back to original pos
            out_fps.seek(org_pos)
            if fsize == 0:
                empty_cols.append(out_fps)

        if empty_cols:
            logger.warn(
                f"Warning: {len(empty_cols)} hash columns were empty. "
                "Please check the input data if this is unexpected."
            )
            # if all columns are empty, error
            if len(empty_cols) == len(self.out_fps):
                # cleanup the empty out files
                for outfile in self.out_files:
                    try:
                        os.unlink(outfile)
                    except Exception as rmError:
                        pass
                raise PddHashException("All columns were empty. Please check the input data")

            # not all columns are empty
            for out_fps in empty_cols:
                # add a dummy record
                dummy_data = "(@&...====&&+!!!####(^^^^@@@&&&&&&&!!!!!====&&&&((([[[["
                salted_entry = dummy_data.encode("utf-8") + self.salt.encode("utf-8")
                hash_entry = hashlib.sha256(salted_entry).digest()
                final_entry = b'1:' + hash_entry
                out_fps.write(final_entry)

    def _collect_hash_output(self):
        """
            Concatenate the hash outputs from the children and write them
            to the final output file.
        """
        for proc_result in self.proc_results:
            for i, files in enumerate(proc_result['files']):
                with open(files, 'rb') as src_fp:
                    shutil.copyfileobj(src_fp, self.out_fps[i])
                    try:
                        os.remove(files)
                    except Exception as rmError:
                        raise rmError

        # check if any hash column is empty, add dummy record
        self._fill_empty_hash_files()
        # Closing file objects immidiately
        _close_fps(self.out_fps)

    def _generate_metadata(self):
        """
            Generate metadata json file that describes the column names,
            generation id and the generated file information.
        """
        out_json = {}
        column_data = []
        for out_file in self.out_files:
            column_data.append(os.path.basename(out_file))
        out_json['column_names'] = self.column_names
        out_json['column_data'] = column_data
        out_json['filename'] = os.path.basename(self.input_file)
        out_json['dict_data'] = self.dict_result

        # encrypt generation_id fs there is public_key
        if self.public_key:
            with open(self.public_key, "rb") as cert_file:
                cert_data = cert_file.read()
            cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            serial_num = str(cert.serial_number)
            raw_salt = SALT_PREFIX + self.global_gen_id

            # encrypt the salt
            pub_key = cert.public_key()
            encrypted = pub_key.encrypt(
                    raw_salt.encode('utf-8'),
                    padding.OAEP(
                        mgf=padding.MGF1(algorithm=hashes.SHA1()),
                        algorithm=hashes.SHA1(),
                        label=None
                ))
            encoded_genid = encrypted.hex()

            self.global_gen_id = self.global_edm_id +\
                '+' + serial_num +\
                '+' + encoded_genid

            self.global_gen_id = ENCRYPTED_PREFIX + self.global_gen_id

        # set generation_id
        out_json['generation_id'] = self.global_gen_id

        out_str = json.dumps(out_json, indent=4)
        input_fname = os.path.basename(self.input_file).split('.')[0]
        meta_file = 'pdd_metadata_' + input_fname + '.json'
        output_folder = self.out_dir.replace('/temp_edm_hashes', '')
        zip_file = output_folder + '/pdd_metadata_' + input_fname + '.tgz'
        source = self.out_dir
        logger.debug(f'zip file name: {zip_file} and source file name: {source}')
        output_file = os.path.join(self.out_dir, meta_file)
        with open(output_file, 'wb+') as fp:
            fp.write(out_str.encode("utf-8"))
        # Write the tgz file
        try:
            with tarfile.open(zip_file, "w:gz") as tar:
                tar.add(source, arcname=os.path.basename(source))
        except FileNotFoundError as fnf_error:
            logger.error(f"Error: {fnf_error}")
        except PermissionError as perm_error:
            logger.error(f"Error: {perm_error} - Check file permissions.")
        except Exception as eee:
            logger.error(f"Error in tar generation: {eee}")
        return 'pdd_metadata_' + input_fname + '.tgz'

    def _finish_process(self):
        """
            Collect the generated data from multiple sources into one
            file and generate the metadata for uploading.
        """
        self._collect_hash_output()
        meta_file = self._generate_metadata()

        print("Copy the generated metadata, hash and dictionary files " \
                "to Netskope Secure Forwarder for uploading")

        print("")
        print("Result summary:")
        for result_block in self.proc_results:
            print("    Chunk: " + str(result_block["chunk"]))
            print("        Status: " + str(result_block["status"]))
            print("        Lines processed: " + str(result_block["lines"]))
            print("        Lines skipped: " + str(result_block["skipped"]))
            print("        Files written: ")
        print("")
        return meta_file

    def _parallel_hashing(self):
        """
            Use multiprocessing module for hashing the data in parallel based
            on the number of cpus available.
        """
        cursor = 0
        chunk = 0
        line_count = 0
        start_row = 1
        results = []
        pool = mp.Pool(self.cpu_count)
        line_split_count = _get_line_split(os.path.getsize(self.input_file))

        print("Starting parallel hashing of data and this may take a while " \
                "depending on the input size")

        with open(self.input_file,'rb') as fp:
            if self.skip_first_line:
                fp.readline()
            cursor = fp.tell()
            while fp.readline():
                line_count += 1
                if not line_count % line_split_count:
                    end = fp.tell()
                    proc = pool.apply_async(process_chunk,
                                        args = [self.input_file, self.out_dir,
                                                self.salt, start_row, chunk,
                                                cursor, end,
                                                self.dict_cs, self.dict_cins,
                                                self.norm_nums, self.norm_strs,
                                                False, self.column_names,
                                                self.args])
                    chunk += 1
                    cursor = end
                    start_row = line_count + 1
                    results.append(proc)

            end = fp.tell()
            proc = pool.apply_async(process_chunk,
                                args = [self.input_file, self.out_dir,
                                        self.salt, start_row, chunk,
                                        cursor, end,
                                        self.dict_cs, self.dict_cins,
                                        self.norm_nums, self.norm_strs,
                                        False, self.column_names,
                                        self.args])
            results.append(proc)

        sys.stdout.flush()
        print("%d records scanned (into %d chunks) and queued for hashing" % \
                (line_count, chunk+1))
        print('Waiting for hash generation to complete this may take a while')

        pool.close()
        pool.join()

        fail = False
        error = ''
        self.proc_results = [{}] * (chunk + 1)
        for proc in results:
            proc_result = proc.get()
            result_json = json.loads(proc_result)
            if result_json['status'] == 'fail':
                fail = True
                error = result_json['error']
            else:
                self.status_line_count += result_json['lines']
                self.proc_results[result_json['chunk']] = result_json

        if fail:
            raise PddHashException("PDD permutation processing failed " \
                                    "due to %s" % (error))

        print(self.status_line_count, "records hashed. Gathering the data.")

        metadata_file = self._finish_process()
        return metadata_file

    def _sequential_hashing(self):
        """
            Hash the data sequentially
        """
        fsize = os.path.getsize(self.input_file)

        print("Starting sequential hashing of data and this may take a while " \
                "depending on the input size")

        with open(self.input_file, 'rb') as fp:
            if self.skip_first_line:
                fp.readline()
            cursor = fp.tell()
            result = process_chunk(self.input_file, self.out_dir, self.salt,
                                    1, 0, cursor, fsize,
                                    self.dict_cs, self.dict_cins,
                                    self.norm_nums, self.norm_strs,
                                    True, self.column_names,
                                    self.args)
            result_json = json.loads(result)
            if result_json['status'] == 'fail':
                raise PddHashException("Post processing failed due to %s" % \
                                    result_json['error'])

            self.dict_result = result_json['dict']
            self.proc_results.append(result_json)
            metadata_file = self._finish_process()
            return metadata_file

    def cleanup(self):
        """
            Close the open file descriptors and cleanup the temporary files.
        """
        self._cleanup_files()

    def run(self):
        """
            Perform the hash operation
        """
        # if self.cpu_count > 1 and not (self.dict_cs or self.dict_cins):
        #     metadata_file = self._parallel_hashing()
        # else:
        metadata_file = self._sequential_hashing()
        return metadata_file


def enable_fips():
    """
        Enable OpenSSL's fips mode
        :return: True is Openssl's FIPS mode is on, else False
    """
    try:
        import ctypes
        libcrypto = ctypes.CDLL("libcrypto.so")
        fips_mode = libcrypto.FIPS_mode
        fips_mode.argtypes = []
        fips_mode.restype = ctypes.c_int

        fips_mode_set = libcrypto.FIPS_mode_set
        fips_mode_set.argtypes = [ctypes.c_int]
        fips_mode_set.restype = ctypes.c_int

        # check of FIPS is already enabled
        if fips_mode():
            return True

        # not enabled, try to enable it
        fips_mode_set(1)

        if fips_mode():
            return True
    except Exception as ex:
        print(f"Loading Openssl FIPS mode error: {str(ex)}")

    return False


def get_options():
    """
        Parse the command line options using optparse
    """
    usage = "usage: %(prog)s [options] <mode> <input_csv> <output_dir>"
    desc = get_desc()
    cur_path = os.getcwd()
    parser = ArgumentParser(usage=usage, description=desc)

    parser.add_argument("-l", "--log",
                    dest="log_file",
                    type=str,
                    help="Log file")
    parser.add_argument("-d", "--delimiter",
                    dest="delimiter",
                    type=str,
                    help="Delimiter for the input file.  Default is comma")
    parser.add_argument("-c", "--column-names",
                    dest="column_names",
                    type=str,
                    help="Comma separated column names. Useful when the input "\
                    "file does not have column names in the first line.")
    parser.add_argument("-p", "--parse-column-names",
                    dest="parse_column_names",
                    action="store_true",
                    help="First line of the input file contains column names. "\
                    "If specified, this line is used to infer column names "\
                    "and the -c/--column-names option is ignored.")
    parser.add_argument("--dict-cs",
                    dest="dict_cs",
                    type=str,
                    help="Case sensitive dictionary column list. Ex: 1,2")
    parser.add_argument("--dict-cins",
                    dest="dict_cins",
                    type=str,
                    help="Case insensitive dictionary column list. Ex: 3,4")
    parser.add_argument("--norm_num",
                    dest="norm_num",
                    type=str,
                    help="Normalize number column list. Ex: 5,6")
    parser.add_argument("--norm_str",
                    dest="norm_str",
                    type=str,
                    help="Normalize string column list. Ex: 7,8")
    parser.add_argument("--skip-hash",
                    dest="skip_hash",
                    action="store_true",
                    help="Skip hash generation, Requires one of --dict-cs/--dict-cins option.")
    parser.add_argument("--edk-lic-dir",
                    dest="edk_lic_dir",
                    type=str,
                    default=cur_path,
                    help="Directory where EDK license file (licensekey.dat) is present [default . (current_dir)]")
    parser.add_argument("--edk-tool-dir",
                    dest="edk_tool_dir",
                    type=str,
                    default=cur_path,
                    help="Directory where EDK tool (edktool.exe) is present [default . (current_dir)]")
    parser.add_argument("--input-encoding",
                    dest="input_encoding",
                    type=str,
                    default='utf-8',
                    help="Set input csv file's character encoding like iso-8859-1.  Default is utf-8")
    parser.add_argument("--public-key",
                    dest="public_key",
                    type=str,
                    default='',
                    help="Set public key file to use for encryption")
    parser.add_argument("--fips",
                    dest="ssl_fips",
                    action="store_true",
                    default=False,
                    help="Enforce Openssl FIPS mode only")
    parser.add_argument("--remove-quotes",
                    dest="remove_quotes",
                    action="store_true",
                    default=False,
                    help="Remove leading and trailing double quotes")
    # It is a hidden argument for Windows support.
    parser.add_argument("--win",
                    dest="windows",
                    action="store_true",
                    default=False,
                    help=SUPPRESS)
    parser.add_argument("mode",
                    type=str,
                    default="hash",
                    help=("Mode (hash) generates secure hashes for each column "
                         "of the input csv file. Example - "
                         "edm_hash_generator.py hash <input_csv> <output_dir>."))
    parser.add_argument("input_csv",
                    type=str,
                    help="Input CSV file")
    parser.add_argument("output_dir",
                    type=str,
                    help="Output directory")

    args = parser.parse_args()


    return (parser, args)

def get_desc():
    """
        return a string that describes how to run this program
    """
    desc = "Mode (hash) generates secure hashes for each column " \
            "of the input csv file. Example - " \
            "edm_hash_generator.py hash <input_csv> <output_dir>."
    return desc

def eula_check():
    """
        Get a confirmation from the user that they agree to the
        EULA.
    """
    inputs = ["YES", "Yes", "yes", "NO", "No", "no"]

    print("Please read the associated License.txt and " \
          "by entering yes you agree to the terms and " \
          "conditions mentioned in the License.txt.")

    inp = "yes"

    while inp not in inputs:
        inp = input("Please enter yes or no: ")

    if inp == "yes" or inp == "Yes" or inp == "YES":
        return True
    else:
        return False

def generate_edm_hash(conf_name, edm_conf):
    global CSV_DELIM
    global FILE_ENCODING

    mode = edm_conf.get("mode")
    input_file = edm_conf.get("input_csv")
    output_dir = edm_conf.get("output_dir")

    logger.debug(f"EDM Hash generation is started for configuration: {conf_name}.")

    if mode != 'hash':
        logger.error("Invalid mode %s passed. Only 'hash' is supported")

    if not os.path.isfile(input_file):
        error_msg = "File %s doesn't exist" % (input_file)
        logger.error(error_msg)


    # check if file size is 0
    if os.path.getsize(input_file) == 0:
        error_msg = "File %s is empty" % (input_file)
        logger.error(error_msg)


    if edm_conf.get("ssl_fips"):
        if not enable_fips():
            error_msg = "Failed to enable Openssl FIPS mode"
            logger.error(error_msg)


    if edm_conf.get("public_key"):
        if not os.path.isfile(edm_conf.get("public_key")):
            error_msg = "Public key file %s doesn't exist" % (input_file)
            logger.error(error_msg)


        if os.path.getsize(edm_conf.get("public_key")) == 0:
            error_msg = "Public key file %s is empty" % (input_file)
            logger.error(error_msg)


    # if csv file has column headers, make sure we have 1 data row
    with open(input_file, 'r') as fp:
        if edm_conf.get("parse_column_names"):
            hdr = fp.readline()
            if hdr:
                hdr = hdr.strip()
            if not hdr:
                error_msg = "Can't find column headers in file %s" % (input_file)
                logger.error(error_msg)

        fline = fp.readline()
        if fline:
            fline = fline.strip()
        if not fline:
            error_msg = "No data in file %s" % (input_file)
            logger.error(error_msg)


    base_name = os.path.basename(input_file)
    if len(base_name) > MAX_FILENAME_LEN:
        error_msg = f"Filename too long (support {MAX_FILENAME_LEN} chars)"
        logger.error(error_msg)
        raise PddHashException(error_msg)

    # Check if base filename (without extension) contains only alphanumeric and underscore
    base_name_no_ext = os.path.splitext(base_name)[0]
    if not re.match(r'^[a-zA-Z0-9_]+$', base_name_no_ext):
        error_msg = f"Invalid filename '{base_name}'. Filename must contain only alphanumeric characters and underscores [a-zA-Z0-9_]"
        logger.error(error_msg)
        raise PddHashException(error_msg)


    col_names = []
    if edm_conf.get("column_names"):
        for cn in edm_conf.get("column_names").split(","):
            col_names.append(cn.strip())

    if len(col_names) <= 0 and not edm_conf.get("parse_column_names"):
        error_msg = "Column names must be present in the file or specified as a parameter"
        logger.error(error_msg)  # noqa


    if edm_conf.get("skip_hash") and not edm_conf.get("dict_cs") and not edm_conf.get("dict_cins"):
        error_msg = "--skip-hash option requires one of --dict-cs/--dict-cins option"
        logger.error(error_msg)


    if edm_conf.get("input_encoding"):
        FILE_ENCODING = edm_conf.get("input_encoding")

    dict_cs = []
    if edm_conf.get("dict_cs"):
        dict_cs = edm_conf.get("dict_cs").split(",")

    dict_cins = []
    if edm_conf.get("dict_cins"):
        dict_cins = edm_conf.get("dict_cins").split(",")

    norm_nums = []
    if edm_conf.get("norm_num"):
        norm_nums = edm_conf.get("norm_num").split(",")

    norm_strs = []
    if edm_conf.get("norm_str"):
        norm_strs = edm_conf.get("norm_str").split(",")

    if set(dict_cs).intersection(set(dict_cins)):
        logger.error("Dictionary columns shouldn't overlap")

    dict_cs = make_int_array(dict_cs, is_one_based=False)
    dict_cins = make_int_array(dict_cins, is_one_based=False)
    norm_nums = make_int_array(norm_nums, is_one_based=False)
    norm_strs = make_int_array(norm_strs, is_one_based=False)

    if SUPPORT_DICT:
        if not eula_check():
            error_msg = "EULA not agreed"
            logger.error(error_msg)


    if edm_conf.get("delimiter"):
        CSV_DELIM = edm_conf.get("delimiter")
        logger.debug("Set input file delimiter to " + CSV_DELIM)

    pdd_hash = None
    edm_conf['skip_hash'] = False
    try:
        pdd_hash = PddHash(mode, input_file, output_dir,
                           dict_cs, dict_cins, norm_nums, norm_strs,
                           edm_conf, column_names=col_names,
                           parse_column_names=True,
                           public_key=None)

        metadata_file = pdd_hash.run()
        logger.info(f"EDM Hash generation completed successfully for configuration: {conf_name}.")
        return True, metadata_file
    except Exception as e:
        logger.error(
            message="Error occurred while generating EDM hashes "
            + f"for configuration: {conf_name}",
            details=traceback.format_exc(),
            error_code="EDM_1029"
        )
        return False, None
    finally:
        if pdd_hash is not None:
            pdd_hash.cleanup()
