#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files. Your task is to complete the shape_element function that will transform each
element into the correct format. To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the 
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

#OSM_PATH = "example5.osm"
OSM_PATH = "magdeburg_harz.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER = re.compile(r'^[\da-z]+$')
LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""
 

    #print("shape_element")
    #print("element.tag:{0}".format(element.tag) )

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    nd_pos = 0
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE
    tag_dict = {}
    tag_count = 0
    for tag in element.iter('tag'):
        tag_k = tag.attrib['k']
        if not problem_chars.search(tag_k):
            key_ok = True
            tag_k_split_list = re.split('[:]', tag_k)
            for tag_k_split in tag_k_split_list:
                # Check if all lowercase letters or digits in key
                if not LOWER.match(tag_k_split):
                    key_ok = False
            # In case the key is ok:
            if key_ok:
                tag_key = tag_k
                tag_type = ""
                tag_k_split_list = re.split('[:]', tag_k, maxsplit=1)
                if len(tag_k_split_list) > 1:
                    tag_type = tag_k_split_list[0]
                    tag_key = tag_k_split_list[1]
                    #print(tag_k_colon.group(1))
                    tag_count += 1
                    #print("Tag number:{}".format(tag_count))
                    #print(tag.attrib)
                tag_dict = { \
                     'id': element.attrib['id'], \
                     'key': tag_key, \
                     'value': tag.attrib['v'], \
                     'type': tag_type \
                     }
                tags.append(tag_dict)
    
    # way_nodes
    nd_dict = {}
    for tag in element.iter('nd'):
        nd_dict = { \
                   'id': element.attrib['id'], \
                   'node_id': tag.attrib['ref'], \
                   'position': nd_pos \
                   }
        way_nodes.append(nd_dict)
        nd_pos += 1
        
    #for node_attr_key in node_attr_fields:
    #    node_attribs.update({ node_attr_key : element.attrib[node_attr_key] })
    
    if element.tag == 'node':
        for node_attr_key in node_attr_fields:
            node_attribs.update({ node_attr_key : element.attrib[node_attr_key] })
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        for way_attr_key in way_attr_fields:
            way_attribs.update({ way_attr_key : element.attrib[way_attr_key] })
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(iter(validator.errors.items())) # PY2->PY3: d.iteritems() -> iter(d.items())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.items()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)
#UnicodeDictWriter = csv.DictWriter # PY3

import sqlite3
import pandas as pd

class Sqlite3dbFile(object):
    """Base class just to open an SQLite3-DB file"""
    def __init__(self, fname="magdeburg.sqlite3"):
        self.connlite3 = sqlite3.connect(fname)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.connlite3.close()

class Sqlite3TableWriter(object):
    """Same thing as UnicodeDictWriter but using SQLite3 db"""
    def __init__(self, sqlite3db, table_name, table_columns):
        self.db = sqlite3db.connlite3
        self.tn = table_name
        self.tc = table_columns
        self.cache_pdf = pd.DataFrame(columns=table_columns)
        self.cache_list = [] # Faster than pd.DataFrame.append!
    def writerow(self, row):
        self.cache_list.append(row) # Pre-allocation would be faster, but this works...
    def writerows(self, rows):
        for row in rows: # TODO: store rows in dataframe
            self.writerow(row)
    def commit_db(self): # Some caching might be reasonable...
        self.cache_pdf = pd.DataFrame(self.cache_list, columns=self.tc)
        self.cache_pdf.to_sql(self.tn, self.db, if_exists="append")
    


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with Sqlite3dbFile("magdeburg_harz.sqlite3") as dbfile:        
        nodes_writer = Sqlite3TableWriter(dbfile, "nodes", NODE_FIELDS)
        node_tags_writer = Sqlite3TableWriter(dbfile, "nodes_tags", NODE_TAGS_FIELDS)
        ways_writer = Sqlite3TableWriter(dbfile, "ways", WAY_FIELDS)
        way_nodes_writer = Sqlite3TableWriter(dbfile, "ways_nodes", WAY_NODES_FIELDS)
        way_tags_writer = Sqlite3TableWriter(dbfile, "ways_tags", WAY_TAGS_FIELDS)

        # Brauchen wir nicht - oder an der Stelle neuen Table erzeugen.
        #nodes_writer.writeheader()

        validator = cerberus.Validator()
        print("Processing the Map...")
        
        counter = 0
        counter_disp = 0
        for element in get_element(file_in, tags=('node', 'way')):
            counter = counter + 1
            if counter > 100000: # Alive-ticker...
                counter = 0
                counter_disp = counter_disp + 1
                print("Parsing elements (in 100k):{0}".format(counter_disp))
            el = shape_element(element)
            
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
        
        print("Done!")
        print("Committing to SQLite-DB...")
        nodes_writer.commit_db()
        print("Committing to SQLite-DB...")
        node_tags_writer.commit_db()
        print("Committing to SQLite-DB...")
        ways_writer.commit_db()
        print("Committing to SQLite-DB...")
        way_nodes_writer.commit_db()
        print("Committing to SQLite-DB...")
        way_tags_writer.commit_db()


def run_process_map():
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    osm_file = open(OSM_PATH, "r", encoding='utf-8') # Set the correct encoding
    process_map(osm_file, validate=False) # validate=True

if __name__ == '__main__':
    run_process_map()
