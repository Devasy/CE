# flake8: noqa
#! /usr/bin/env python
"""
    dictgen.py
    ~~~~~~~~~~

    Generate a dictionary grammar file for a streaming
    input. The creation of the input grammar xml will be
    done using sax.

    Please note this file should be able to run without any
    dependency of common.ns or any other Netskope package as is
    shared with customers (KP).

    :copyright: (c) 2016-2019 Netskope, Inc. All rights reserved.
..author: Arjun Sambamoorthy<arjuns@netskope.com> (April 6, 2016)
"""
from __future__ import print_function

from builtins import str, object, chr
import sys, os
import re
import subprocess
from xml.sax import SAXException
from xml.sax.saxutils import XMLGenerator
import traceback


class DictXMLException(Exception):
    """
        This exception will be raised while processing the XML
        data.
    """
    pass

class DictGenException(Exception):
    """
        A generic exception raised while generating the dictionary.
        Catch this exception if you aren't looking for the specifics.
    """
    pass

class DictXMLGenerator(object):
    """
        A wrapper class for XMLGenerator for creating xml
        elements. This wrapper has the ability to indent
        xml elements and add custom data directly into the
        xml file.

        Call start() after creating the object and before
        writing anything into the file and call done()
        to indicate document done and to perform the
        necessary cleanups.

        Raises DictXMLException on errors and cleanups the
        local states on error so no need to call done() on
        error handling.
    """

    def __init__(self, out_file, indent=True):
        """
            Init routine
        """
        self.out_file = out_file
        self.out_fp = None
        self.current_index = 0
        self.indent = indent
        self.xml_writer = None

    def cleanup(self):
        """
            Close the file descriptor.
        """
        if self.out_fp:
            self.out_fp.close()
            self.out_fp = None

    def write(self, data):
        """
            Write data directly into xml file. Be wary of
            what is written as it can corrupt the xml
            syntax.
        """
        self.out_fp.write(data)

    def startElement(self, element, attr):
        """
            Wrapper for XMLGenerator startElement function
            with indentation.
        """
        if self.indent:
            tab_spaces = "\t" * self.current_index
            self.out_fp.write(tab_spaces)
            self.current_index += 1

        try:
            self.xml_writer.startElement(element, attr)
        except SAXException as saxerr:
            self.cleanup()
            raise DictXMLException(str(saxerr))

        if self.indent:
            self.out_fp.write("\n")

    def endElement(self, element):
        """
            Wrapper for XMLGenerator endElement function with
            indentation.
        """
        if self.indent:
            self.current_index -= 1
            tab_spaces = "\t" * self.current_index
            self.out_fp.write(tab_spaces)

        try:
            self.xml_writer.endElement(element)
        except SAXException as saxerr:
            self.cleanup()
            raise DictXMLException(str(saxerr))

        if self.indent:
            self.out_fp.write("\n")

    def characters(self, data):
        """
            Wrapper for XMLGenerator character.
        """
        # self.characters(data)
        pass # due to sonarqube quality bug and I don't see any use of this method

    def start(self):
        """
            Create XMLGenerator object for the output xml file
            and starts the document. Call this function before
            creating elements for writing data.
        """
        try:
            self.out_fp = open(self.out_file, "w+")
        except IOError as err:
            self.cleanup()
            raise DictXMLException(str(err))

        try:
            self.xml_writer = XMLGenerator(self.out_fp, "utf-8")
            self.xml_writer.startDocument()
        except SAXException as saxerr:
            self.cleanup()
            raise DictXMLException(str(saxerr))

    def done(self):
        """
            End writing the document and perform the necessary
            cleanups.
        """
        try:
            self.xml_writer.endDocument()
        except SAXException as saxerr:
            raise DictXMLException(str(saxerr))
        finally:
            self.cleanup()

class DictGen(object):
    """
        Generate a dictionary ecr grammar file from streaming input.
        Raises DictGenException on error.
    """

    def __init__(self, out_xml, out_grammar, edk_lic,
                edk_tool_path, entity, case_sensitive,
                charset='utf-8'):
        """
            Init routine that sets up the xml file.
        """
        self.out_xml = out_xml
        self.out_grammar = out_grammar
        self.edk_lic = edk_lic
        self.edk_tool_path = edk_tool_path
        self.entity = entity
        self.case_sensitive = case_sensitive
        self.charset = charset
        self.xml_gen = None
        self.record_count = 0

        self._validate_input()
        self._setup_xml_generator()

    def cleanup(self):
        """
            Cleanup the output xml files
        """
        if self.out_xml and os.path.exists(self.out_xml):
            os.remove(self.out_xml)
            self.out_xml = None

    def _validate_input(self):
        """
            Validate the inputs. Check the entity name format and
            validate the edk license and edktool files.
        """

        split_entity = self.entity.split('/')
        if len(split_entity) != 2:
            raise DictGenException("Invalid entity %s" % (self.entity))

        if re.match(r'\d', split_entity[1][0]):
            raise DictGenException("Invalid entity %s" % (self.entity))

        if not os.path.isfile(self.edk_lic):
            raise DictGenException("License file %s not found" \
                                     % (self.edk_lic))

        if not os.path.isfile(self.edk_tool_path):
            raise DictGenException("EDK tool %s not found" \
                                    % (self.edk_tool_path))

    def _setup_xml_generator(self):
        """
            Setup the XML generator for streaming the inputs
            to the generator. Setup the xml with doctype and
            grammar names.
        """
        try:
            split_entity = self.entity.split('/')
            self.xml_gen = DictXMLGenerator(self.out_xml, indent=True)
            self.xml_gen.start()
            self.xml_gen.write(u"<!DOCTYPE grammars\n")
            self.xml_gen.write(u" SYSTEM 'edk.dtd'>\n")
            self.xml_gen.startElement(u"grammars", {u"version": u"4.0"})
            self.xml_gen.startElement(u"grammar", {u"name": split_entity[0]})
            self.xml_gen.startElement(u"entity", {u"name": split_entity[1],
                                                 u"type": u"public"})
        except DictXMLException as err:
            raise DictGenException("XML generation error: %s" % (str(err)))
        except Exception as err:
            print(traceback.print_exc())
            raise DictGenException(str(err))

    def strip_illegal_xml_characters(self, input_str):
        """
        Strip XML restricted chars from input string
        :param input_str: Input string
        :return: modified string
        """
        if input_str:
            # unicode invalid characters
            RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + \
                             u'|' + \
                             u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
                             (chr(0xd800),chr(0xdbff),chr(0xdc00),chr(0xdfff),
                              chr(0xd800),chr(0xdbff),chr(0xdc00),chr(0xdfff),
                              chr(0xd800),chr(0xdbff),chr(0xdc00),chr(0xdfff),
                              )
            input_str = re.sub(RE_XML_ILLEGAL, "", input_str)

            # ascii control characters
            input_str = re.sub(r"[\x01-\x1F\x7F]", "", input_str)
        return input_str

    def write_record(self, record):
        """
            Write the record to grammar xml file.
        """
        try:
            if self.charset == 'iso-8859-1':
                entry = record.rstrip().decode('iso-8859-1', 'ignore')
            else:
                entry = record.rstrip().decode('utf-8', 'ignore')
            entry = self.strip_illegal_xml_characters(entry)
            entry_dict = {"headword": entry}
            if self.case_sensitive:
                entry_dict["case"] = "sensitive"
            else:
                entry_dict["case"] = "insensitive"
            self.xml_gen.startElement("entry", entry_dict)
            self.xml_gen.endElement("entry")
            self.record_count += 1
        except DictXMLException as err:
            raise DictGenException("XML write error: %s" % (str(err)))
        except Exception as err:
            self.xml_gen.cleanup()
            raise DictGenException(str(err))

    def compile_records(self):
        """
            Notify XML generator about record completion and compile the xml
            into ecr.
        """
        if self.record_count == 0:
            # need to add a dummy entry, something that EDK can never detect
            dummy_entry = {"headword": "(@&...====&&+!!!####(^^^^@@@&&&&&&&!!!!!====&&&&((([[[["}
            self.xml_gen.startElement("entry", dummy_entry)
            self.xml_gen.endElement("entry")

        self.xml_gen.endElement("entity")
        self.xml_gen.endElement("grammar")
        self.xml_gen.endElement("grammars")
        self.xml_gen.done()

        try:
            cmd = [ self.edk_tool_path,
                    'compile',
                    '-o',
                    self.out_grammar,
                    '-l',
                    self.edk_lic,
                    '-i',
                    self.out_xml ]
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as suberr:
            print('-'*64, '\nCommand output')
            print('-'*64, suberr.output.decode(), '-'*64)
            raise DictGenException("Dict ecr compilation failed. cmd %s. Err %s" \
                                    % (cmd, str(suberr)))
        '''
        finally:
            if self.out_xml and os.path.exists(self.out_xml):
                os.remove(self.out_xml)
                self.out_xml = None
        '''

class DictUTException(Exception):
    """
        Generic unit test exception
    """
    pass

class DictUTAssessException(Exception):
    """
        Dictionary assessment unit test exception
    """
    pass

def test_create_dict(out_xml, out_grammar, edk_lic, edk_tool,
                    entity, case, inputs):
    """
        Create a dictionary grammar file
    """
    dict_gen = DictGen(out_xml, out_grammar, edk_lic, edk_tool,
                        entity, case)
    for entry in inputs:
        dict_gen.write_record(entry.encode('utf-8'))

    dict_gen.compile_records()

def test_assess_dict(out_grammar, edk_lic, edk_tool, inputs):
    """
        Assess the compiled grammar file
    """
    test_input = "/tmp/dictgen_test.txt"
    with open(test_input, "w+") as test_fp:
        for entry in inputs:
            test_fp.write(entry + u'\n')

    cmd = [ edk_tool, 'assess', '-l', edk_lic, '-g',
            out_grammar, '-v', test_input, '-a' ]

    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        regex = re.compile(r'(?:(FAIL)|(PASS)):\s+valid input\s+'\
                            '\(\"(.*)\"\)\s+is matched$', re.MULTILINE)
        result_list = regex.findall(output.decode('utf-8'))
        for result in result_list:
            if result[1] != 'PASS':
                raise DictUTAssessException("Unit test match failed for %s" \
                                            % (result[2]))
    except subprocess.CalledProcessError as suberr:
        raise DictUTAssessException(str(err))
    except Exception as err:
        raise DictUTAssessException(str(err))
    finally:
        os.remove(test_input)

def test_case_sensitive(out_xml, out_grammar, edk_lic, edk_tool,
                        entity):
    """
        Case sensitive grammar tests
    """
    try:
        inputs = [u"Carbon", u"Silicon", u"Hemoglobin"]
        test_create_dict(out_xml, out_grammar, edk_lic, edk_tool,
                        entity, True, inputs)
        test_assess_dict(out_grammar, edk_lic, edk_tool, inputs)
    except DictGenException as dicterr:
        raise DictUTException("Failed generating case sensitive dictionary")
    except DictUTAssessException as dicterr:
        raise DictUTException("Assessment failed")

def test_case_insensitive(out_xml, out_grammar, edk_lic, edk_tool,
                        entity):
    """
        Case insensitive grammar tests
    """
    try:
        inputs = [u"carbon", u"Silicon", u"hemoGlobin"]
        test_inputs = [u"CARBON", u"sIlIcOn", u"Hemoglobin"]
        test_create_dict(out_xml, out_grammar, edk_lic, edk_tool,
                        entity, False, inputs)
        test_assess_dict(out_grammar, edk_lic, edk_tool, test_inputs)
    except DictGenException as dicterr:
        raise DictUTException("Failed generating case insensitive dictionary")
    except DictUTAssessException as dicterr:
        raise DictUTException(str(dicterr))

def test_entity_name(out_xml):
    """
        Entity name negative tests
    """
    inputs = [u"testcustom", u"test/123", u"test/123/123"]
    for entity in inputs:
        try:
            DictGen(out_xml, None, None, None, entity, False)
        except DictGenException as dicterr:
            continue

        raise DictUTException("Entity name test failed")

def run_test():
    """
        Run the unit tests
    """
    out_xml = "/tmp/dictgen_test.xml"
    out_grammar = "/tmp/dictgen_grammar.ecr"
    edk_lic = "licensekey.dat"
    edk_tool = "./edktool.exe"
    #edk_lic = "/opt/autonomy/EductionSDK/lic/licensekey.dat"
    #edk_tool = "/opt/autonomy/EductionSDK/bin/edktool.exe"
    entity = "custom/test"

    try:
        test_case_sensitive(out_xml, out_grammar, edk_lic, edk_tool,
                            entity)
        test_case_insensitive(out_xml, out_grammar, edk_lic, edk_tool,
                            entity)
        test_entity_name(out_xml)
    except DictUTException as uterr:
        print(traceback.print_exc())
        print("Unit test FAILED - %s" % (str(uterr)))
        return
    except Exception as err:
        print(traceback.print_exc())
        print("Unit test FAILED - %s" % (str(err)))
        return
    finally:
        if os.path.isfile(out_grammar):
            os.remove(out_grammar)

    print("Unit tests PASSED")

if __name__ == "__main__":
    sys.exit(run_test())
