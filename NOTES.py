"""
Code to get a small subset of the file at https://mapzen.com/data/metro-extracts/

is as followsi(Project Prep):
"""
######################################33
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "some_osm.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample.osm"

k = 10 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every kth top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % k == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')
####################################
#Example using the cleaning blueprint
####################################
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
from collections import defaultdict
import re

osm_file = open("chicago.osm", "r")

street_type_re = re.compile(r'\S+\.?$', re.IGNORECASE)
street_types = defaultdict(int)

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()

        street_types[street_type] += 1

def print_sorted_dict(d):
    keys = d.keys()
    keys = sorted(keys, key=lambda s: s.lower())
    for k in keys:
        v = d[k]
        print "%s: %d" % (k, v) 

def is_street_name(elem):
    return (elem.tag == "tag") and (elem.attrib['k'] == "addr:street")

def audit():
    for event, elem in ET.iterparse(osm_file):
        if is_street_name(elem):
            audit_street_type(street_types, elem.attrib['v'])    
    print_sorted_dict(street_types)    

if __name__ == '__main__':
    audit()

#####################################################
#CORRECTING VALIDITY
#####################################################
"""
Your task is to check the "productionStartYear" of the DBPedia autos datafile for valid values.
The following things should be done:
    - check if the field "productionStartYear" contains a year
    - check if the year is in range 1886-2014
    - convert the value of the field to be just a year (not full datetime)
    - the rest of the fields and values should stay the same
    - if the value of the field is a valid year in the range as described above,
      write that line to the output_good file
    - if the value of the field is not a valid year as described above, write that line to the output_bad file
    - discard rows (neither write to good nor bad) if the URI is not from dbpedia.org
    - you should use the provided way of reading and writing data (DictReader and DictWriter)
      They will take care of dealing with the header.

      You can write helper functions for checking the data and writing the files, but we will call only the 
      'process_file' with 3 arguments (inputfile, output_good, output_bad).
"""
import csv
import pprint

INPUT_FILE = 'autos.csv'
OUTPUT_GOOD = 'autos-valid.csv'
OUTPUT_BAD = 'FIXME-autos.csv'

def process_file(input_file, output_good, output_bad):

    with open(input_file, "r") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames

#COMPLETE THIS FUNCTION



                                              # This is just an example on how you can use csv.DictWriter
                                                  # Remember that you have to output 2 files
                                                      with open(output_good, "w") as g:
                                                                  writer = csv.DictWriter(g, delimiter=",", fieldnames= header)
                                                                          writer.writeheader()
                                                                                  for row in output_good:
                                                                                                  writer.writerow(row)
                                                                                                              
                                                                                                                  #writing output_bad
                                                                                                                      with open(output_bad,"w") as h:
                                                                                                                                  writer2 = csv.DictWriter(h, delimiter=",", fieldnames= header)
                                                                                                                                          writer2.writeheader()
                                                                                                                                                  for row2 in bad_data:
                                                                                                                                                                  writer2.writerow(row2)


                                                                                                                                                                  def test():

                                                                                                                                                                          process_file(INPUT_FILE, OUTPUT_GOOD, OUTPUT_BAD)


                                                                                                                                                                          if __name__ == "__main__":
                                                                                                                                                                                  test()
