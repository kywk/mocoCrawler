#! /usr/bin/env python

## Required libraries / packages
# System built-in
import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
import unicodedata
import datetime
import time
import re
import json
import urllib

# External packages
from pyquery import PyQuery

class DataWarehouse(object):
    '''
    '''

    # Object variable
    data_warehouse = None

    def __init__(self):
        self.data_warehouse = {}
        return

    def append(self, dw_name, data):
        ''' append(dw_name, data)

        Args:
            dw_name:
            data:

        Returns: None

        '''
        data_cursor = self.data_warehouse
        for name in re.split(':', dw_name):
            if data_cursor.has_key(name):
                data_cursor = data_cursor[name]
            else:
                data_cursor[name] = {}
                data_cursor = data_cursor[name]
                data_cursor['_DW_'] = []

        data_cursor['_DW_'].append(data)


class MocoCrawler(object):
    '''

    '''

    # Configuration / Constant / ...
    HTTP_HEADDER = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel \
                       Mac OS X 10.8; rv:24.0) Gecko/20100101 Firefox/24.0'}

    # Class variable

    # Object variable
    _site_desc = None

    data_warehouse = None


    def __init__(self, file_site_ds, target_data_warehouse=None):
        '''
        '''
        with open(file_site_ds) as data_file:
            self._site_desc = json.load(data_file)

        if target_data_warehouse is None:
            self.data_warehouse = DataWarehouse()
        else:
            self.data_warehouse = target_data_warehouse

        return


    def _parse_html(self, pqobj, parser_name, callback):
        ''' _paesr_html(pqobj, parser_name, callback) -> (list)parsed data

        Args:
            parser_name
            callback: pass callback to nested parsing

        Returns: Parsed data

        '''
        # TODO(kywk)
        # - error handling
        # - data list in same page
        # - apply multiple parser in same page

        data_content = {}

        for field in [parser for parser in self._site_desc['uri_parsers']
                if parser['parser'] == parser_name][0]['fields']:

            field_data = []
            for item in [obj for obj in pqobj.find(field['obj_selector'])]:
                if field['data_src'] == 'attr':
                    field_data.append(pqobj(item).attr(field['data_attr']))
                elif field['data_src'] == 'text':
                    field_data.append(pqobj(item).text())

            field_data = filter(None, field_data)
            for data in field_data:
                if field.has_key('filter'):
                    if field['filter']['method'] == 'split':
                        data = re.split(field['filter']['patten'], data) \
                                       [field['filter']['index']]

                # TODO(kywk): data formating

                if field['field_type'] == 'uri':
                    self.parse_uri({
                        'uri': self._site_desc['config']['uri_prefix'] + data,
                        'type': field['type'],
                        'parser': field['parser']
                        }, callback)
                elif field['field_type'] == 'data':
                    data_content[field['name']] = data

        if data_content:
            self.data_warehouse.append(parser['data_warehouse'], data_content)

        return data_content


    def parse_uri(self, target, callback, cb_trigger=None):
        ''' parse_uri(target, callback) -> callback(parse_TYPE(parser))

        URI dispatcher: loads URI content, then dispatchs to type paser

        Args:
            target: {uri, type, parser}
            callback:

        Returns: Parsed data

        '''
        print 'Parsing: ' + target['uri'] + '...'

        if target['type'] == 'html':
            callback(
                self._parse_html(
                    PyQuery(target['uri'].encode('ascii','ignore'),
                        header=MocoCrawler.HTTP_HEADDER,
                        parser='soup'),
                    target['parser'], callback))

        return


    def crawling(self, target, callback, cb_trigger=None):
        ''' crawling({uri, type, parser}, callback) -> callback(data)

        Args:
            target: {uri, type, parser}
                type: html | json | xml | list
            callback: callback function

        Returns: None

        '''

        if target is None:
            self.parse_uri(self._site_desc['entry'], callback, cb_trigger)
        else:
            self.parse_uri(target, callback, cb_trigger)

        return
