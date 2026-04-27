# flake8: noqa
#! /usr/bin/env python
"""
    dictmgr.py
    ~~~~~~~~~~

    Dictionary manager for creating and managing the dictionary xml
    and ecr grammar files.

    Please note this file should be able to run without any
    dependency of common.ns or any other Netskope package as is
    shared with customers (KP).

    :copyright: (c) 2016-2019 Netskope Inc. All rights reserved.
..author:: Arjun Sambamoorthy<arjuns@netskope.com> (April 8, 2016)
"""
from __future__ import print_function

from builtins import str
from builtins import object
import sys, traceback
import os
import hashlib
from io import open
from .dictgen import DictGen, DictGenException

class DictMgrException(Exception):
    """
        Exception raised while handling dictionary objects
    """
    pass

class DictMgr(object):
    """
        Dictionary manager
    """

    DICT_NAME_PREFIX = "Auto"
    DICT_EDK_NAME_PREFIX = "autodict"

    def __init__(self, out_dir, object_name, object_id='', elem_name='',
                elem_id='', edk_lic='', edk_tool='',
                case_sensitive=True, charset='utf-8'):
        """
            The object name is mandatory and it will be used as
            dictionary name in WebUI. If present object id will be
            used in the ecr and xml file names. The name and id
            should be unique for a tenant.

            The element name and id are optional and should be used
            if multiple dictionaries are created for the same object.
            Example: PDD columns.

            Raises DictMgrException on error.
        """
        self.out_dir = out_dir
        self.object_name = object_name
        self.object_id = str(object_id)
        self.elem_name = elem_name
        self.elem_id = str(elem_id)
        self.edk_lic = edk_lic
        self.edk_tool = edk_tool
        self.case_sensitive = case_sensitive
        self.charset = charset

        self.dict_name = None
        self.dict_edk_name = None
        self.dict_entity = None
        self.dict_ecr_path = None
        self.dict_xml = None
        self.dict_gen = None
        self.md5sum = None
        self.size = 0

        self._check_inputs()
        self._setup_dict_names()
        self._create_dictgen_obj()

    def cleanup(self):
        """
            Remove the dictionary ecr files
        """
        if os.path.exists(self.dict_ecr_path):
            os.remove(self.dict_ecr_path)
        if self.dict_gen:
            self.dict_gen.cleanup()

    def cleanup_tmp(self):
        """
            Remove the dictionary tmp xml files
        """
        if os.path.exists(self.dict_xml):
            os.remove(self.dict_xml)

    @staticmethod
    def get_md5sum(filename):
        """
            Compute the MD5 of the file.
        """
        md5 = hashlib.md5()
        with open(filename, 'rb') as fp:
            for data in iter(lambda: fp.read(8192), b""):
                md5.update(data)
        return md5.hexdigest()

    def _check_inputs(self):
        """
            Check the inputs
        """
        if not self.object_name:
            raise DictMgrException("Invalid object name")

        if not self.edk_lic or not os.path.isfile(self.edk_lic):
            raise DictMgrException("Invalid EDK license path: %s" \
                                    % self.edk_lic)

        if not self.edk_tool or not os.path.isfile(self.edk_tool):
            raise DictMgrException("Invalid EDK tool path: %s" \
                                    % self.edk_tool)

    def _setup_dict_names(self):
        """
            Create dictionary name, dictionary grammar file name and
            dictionary entity name.
        """
        object_id = self.object_id
        if not object_id:
            object_id = self.object_name

        elem_id = self.elem_id
        if self.elem_name and not self.elem_id:
            elem_id = self.elem_name

        # This is the name that will appear while using it in a DLP
        # rule. Example: AUTO_Patient Records_v1_MRN
        self.dict_name = DictMgr.DICT_NAME_PREFIX + "_" + self.object_name
        if self.elem_name:
            self.dict_name += "_" + self.elem_name

        # Edk grammar xml file path
        xml_name = DictMgr.DICT_EDK_NAME_PREFIX + "_" + object_id.lower()
        if elem_id:
            xml_name += "_" + elem_id.lower() + ".xml"
        else:
            xml_name += ".xml"
        self.dict_xml = os.path.join(self.out_dir, xml_name)

        # Name of the edk grammar file. Example: dictpdd_1_1.ecr
        self.dict_edk_name = DictMgr.DICT_EDK_NAME_PREFIX + "_" + \
                                object_id.lower()
        if elem_id:
            self.dict_edk_name += "_" + elem_id.lower() + ".ecr"
        else:
            self.dict_edk_name += ".ecr"

        self.dict_ecr_path = os.path.join(self.out_dir, self.dict_edk_name)

        # Dictionary entity name. Example: autodict/id_<id>_<elem>
        self.dict_entity = "autodict/id_{}".format(object_id.lower())
        if elem_id:
            self.dict_entity += "_{}".format(elem_id.lower())

    def _create_dictgen_obj(self):
        """
            Create dictionary generator object.
        """
        try:
            self.dict_gen = DictGen(self.dict_xml, self.dict_ecr_path,
                                    self.edk_lic, self.edk_tool,
                                    self.dict_entity,
                                    self.case_sensitive,
                                    self.charset)
        except DictGenException as err:
            print(traceback.print_exc())
            raise DictMgrException("Failed creating DictGen object." \
                                    "Error: %s" % (str(err)))

    def write_record(self, data):
        """
            Write the data into the grammar xml file.
        """
        try:
            self.dict_gen.write_record(data)
        except DictGenException as err:
            raise DictMgrException("Dict mgr write failed. Error: %s" \
                                    % (str(err)))

    def compile_dict(self):
        """
            All the data is written now start compiling the
            dictionary grammar file.
        """
        try:
            self.dict_gen.compile_records()
            self.md5sum = self.get_md5sum(self.dict_ecr_path)
            self.size = os.path.getsize(self.dict_ecr_path)
        except DictGenException as err:
            print(traceback.print_exc())
            raise DictMgrException("Dict compilaton failed. Error: %s" \
                                    % (str(err)))

    def get_meta_json(self):
        """
            Get the dictionary meta data as json
        """
        dict_json = {}
        dict_json['name'] = self.dict_name
        dict_json['edk_entity'] = self.dict_entity
        dict_json['edk_file'] = os.path.basename(self.dict_ecr_path)
        dict_json['case_sensitive'] = 'false'
        if self.case_sensitive:
            dict_json['case_sensitive'] = 'true'
        return dict_json

    def get_upload_json(self, add_subfile=False):
        """
            Get the json for uploading the dictionary data.
        """

        dict_json = {}
        dict_json['filename'] = self.dict_ecr_path
        dict_json['md5sum'] = self.md5sum
        dict_json['size'] = self.size

        if not add_subfile:
            return dict_json

        subfile = {}
        subfile['name'] = self.dict_ecr_path
        subfile['size'] = self.size
        dict_json['subfiles'] = [ subfile ]
        return dict_json

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("usage: dictmgr.py <input file>\n")
        sys.exit(-1)

    if not os.path.isfile(sys.argv[1]):
        print("File not found %s" % (sys.argv[1]))
        sys.exit(-1)

    try:
        dict_mgr = DictMgr("./", 'test', elem_name='name', elem_id='1',
                        edk_lic="./licensekey.dat",
                        edk_tool="./edktool.exe",
                        case_sensitive=False,
                        charset='iso-8859-1')

        with open(sys.argv[1], 'rb') as fp:
            for line in fp:
                dict_mgr.write_record(line)

        dict_mgr.compile_dict()
    except Exception:
        print(traceback.print_exc())
