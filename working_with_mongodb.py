"""
KICKING THE TIRES ON MONGODB
"""

"""
Your task is to sucessfully run the exercise to see how pymongo works
and how easy it is to start using it.
You don't actually have to change anything in this exercise,
but you can change the city name in the add_city function if you like.

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine,
you have to install MongoDB (see Instructor comments for link to installation information)
and uncomment the get_db function.
"""

"""
def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    # 'examples' here is the database name. It will be created if it does not exist.
    db = client.examples
    return db
"""

def add_city(db):
    db.cities.insert({"name" : "Chicago"})
    
def get_city(db):
    return db.cities.find_one()


if __name__ == "__main__":

    db = get_db() # uncomment this line if you want to run this locally
    add_city(db)
    print get_city(db)

#QUERYING USING FIELD SELECTION
from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://localhost:27017")

db = client.examples

def find():
    autos = db.autos.find({"manufacturer" : "Toyota"})
    for a in autos:
        pprint.pprint(a)
        
if __name__=='__main__'
    find()

#QUIZ:FINDING PORSCHE
#!/usr/bin/env python
"""
Your task is to complete the 'porsche_query' function and in particular the query
to find all autos where the manufacturer field matches "Porsche".
Please modify only 'porsche_query' function, as only that will be taken into account.

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine,
you have to install MongoDB and download and insert the dataset.
For instructions related to MongoDB setup and datasets please see Course Materials at
the following link:
https://www.udacity.com/wiki/ud032
"""


def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


def porsche_query():
    # Please fill in the query to find all autos manuafactured by Porsche
    query = {}
    return query


def find_porsche(db, query):
    return db.autos.find(query)


if __name__ == "__main__":

    db = get_db('examples')
    query = porsche_query()
    p = find_porsche(db, query)
    import pprint
    for car in results[:3]:
        pprint.pprint(car)

#PROJECTION QUERIES
def find():
    query = { "manufacturer" : "Toyota", "class": "mid-size car"}
    projection = {"_id" : 0, "name": 1}
    autos = db.autos.find(query, projection)

    for a in autos:
        pprint.pprint(a)

if __name__ == '__main__':
    find()

#GETTING DATA INTO MONGODB
for a in autos:
    db.myautos.insert(a)

num_autos = db.myautos.find().count()
print "num_autos after", num_autos

#EXAMPLES FOR INSERTING MULTIPLE DOCUMENTS
from pymongo import MongoClient
import csv
import json
import io
import re
import pprint


field_map = {
    "name" : "name",
    "bodyStyle_label" : "bodyStyle",
    "assembly_label" : "assembly",
    "class_label" : "class",
    "designer_label" : "designer",
    "engine_label" : "engine",
    "length" : "length",
    "height" : "height",
    "width" : "width",
    "weight" : "weight",
    "wheelbase" : "wheelbase",
    "layout_label" : "layout",
    "manufacturer_label" : "manufacturer",
    "modelEndYear" : "modelEndYear",
    "modelStartYear" : "modelStartYear",
    "predecessorLabel" : "predecessorLabel",
    "productionStartYear" : "productionStartYear",
    "productionEndYear" : "productionEndYear",
    "transmission" : "transmission"
}
fields = field_map.keys()


def skip_lines(input_file, skip):
    for i in range(0, skip):
        next(input_file)

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def strip_automobile(v):
    return re.sub(r"\s*\(automobile\)\s*", " ", v)

def strip_city(v):
    return re.sub(r"\s*\(city\)\s*", " ", v)

def parse_array(v):
    if (v[0] == "{") and (v[-1] == "}"):
        v = v.lstrip("{")
        v = v.rstrip("}")
        v_array = v.split("|")
        v_array = [i.strip() for i in v_array]
        return v_array
    return v

def mm_to_meters(v):
    if v < 0.01:
        return v * 1000
    return v

def clean_dimension(d, field, v):
    if is_number(v):
        if field == "weight":
            d[field] = float(v) / 1000.0
        else:
            d[field] = mm_to_meters(float(v))
    
def clean_year(d, field, v):
    d[field] = v[0:4]

def parse_array2(v):
    if (v[0] == "{") and (v[-1] == "}"):
        v = v.lstrip("{")
        v = v.rstrip("}")
        v_array = v.split("|")
        v_array = [i.strip() for i in v_array]
        return (True, v_array)
    return (False, v)

def ensure_not_array(v):
    (is_array, v) = parse_array(v)
    if is_array:
        return v[0]
    return v

def ensure_array(v):
    (is_array, v) = parse_array2(v)
    if is_array:
        return v
    return [v]

def ensure_float(v):
    if is_number(v):
        return float(v)

def ensure_int(v):
    if is_number(v):
        return int(v)

def ensure_year_array(val):
    #print "val:", val
    vals = ensure_array(val)
    year_vals = []
    for v in vals:
        v = v[0:4]
        v = int(v)
        if v:
            year_vals.append(v)
    return year_vals

def empty_val(val):
    val = val.strip()
    return (val == "NULL") or (val == "")

def years(row, start_field, end_field):
    start_val = row[start_field]
    end_val = row[end_field]

    if empty_val(start_val) or empty_val(end_val):
        return []

    start_years = ensure_year_array(start_val)
    if start_years:
        start_years = sorted(start_years)
    end_years = ensure_year_array(end_val)
    if end_years:
        end_years = sorted(end_years)
    all_years = []
    if start_years and end_years:
        #print start_years
        #print end_years
        for i in range(0, min(len(start_years), len(end_years))):
            for y in range(start_years[i], end_years[i]+1):
                all_years.append(y)
    return all_years


def process_file(input_file):
    input_data = csv.DictReader(open(input_file))
    autos = []
    skip_lines(input_data, 3)
    for row in input_data:
        auto = {}
        model_years = {}
        production_years = {}
        dimensions = {}
        for field, val in row.iteritems():
            if field not in fields or empty_val(val):
                continue
            if field in ["bodyStyle_label", "class_label", "layout_label"]:
                val = val.lower()
            val = strip_automobile(val)
            val = strip_city(val)
            val = val.strip()
            val = parse_array(val)
            if field in ["length", "width", "height", "weight", "wheelbase"]:
                clean_dimension(dimensions, field_map[field], val)
            elif field in ["modelStartYear", "modelEndYear"]:
                clean_year(model_years, field_map[field], val)
            elif field in ["productionStartYear", "productionEndYear"]:
                clean_year(production_years, field_map[field], val)
            else:
                auto[field_map[field]] = val
        if dimensions:
            auto['dimensions'] = dimensions
        auto['modelYears'] = years(row, 'modelStartYear', 'modelEndYear')
        auto['productionYears'] = years(row, 'productionStartYear', 'productionEndYear')
        autos.append(auto)
    return autos

#USING MONGOIMPORT
#documentation at https://docs.mongodb.com/manual/reference/program/mongoimport/

mongoimport -d examples -c myautos2 --file autos.json

###############################################
#OPERATORS
##############################################
#-SAME IDEA AS IN PROGRAMMING LANGUAGES
#-SAME SYNTAX AS FIELD NAMES
#-DISTINGUISHED USING $

#RANGE QUERIES
#-INEQUALITY OPERATORS => $gt, $lt, $gte, $lte, $ne

def find()

    query = {"population" : {"$gt" : 250000, "$lte" : 500000}}
    cities = db.cities.find(query)

    num_cities = 0
    for c in cities:
        pprint.pprint(c)
        num_cities += 1

    print "\nNumber of cities matching: %d\n" % num_cities

def find():

    query = {"name" : {"$gte" : "X", "$lt" : "Y"}}
    cities = db.cities.find(query)

def find():

    query = {"foundingDate" : {"$gte" : datetime(1837, 1, 1),
                               "$lte" : datetime(1837, 12, 31)}
    cities = db.cities.find(query)

    def find2():

             query = {"country" : {"$ne" : "United States"}}
             cities = db.cities.find(query)

    num_cities = 0
    for c in cities:
             pprint.pprint(c)
             num_cities += 1

    print "\nNumber of cities matching: %d\n" % num_cities

if __name__ == '__main__':
    find()

########################################
#Quiz:Range Queries
########################################
#!/usr/bin/env python
"""
Your task is to write a query that will return all cities
that are founded in 21st century.
Please modify only 'range_query' function, as only that will be taken into account.

Your code will be run against a MongoDB instance that we have provided.
If you want to run this code locally on your machine,
you have to install MongoDB, download and insert the dataset.
For instructions related to MongoDB setup and datasets please see Course Materials.
"""

from datetime import datetime
    
def range_query():
    # Modify the below line with your query.
    # You can use datetime(year, month, day) to specify date in the query
    query = {"foundingDate":{"$gte":datetime(2001,1,1,0,0)}}
    return query

# Do not edit code below this line in the online code editor.
# Code here is for local use on your own computer.
def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.examples
    return db

if __name__ == "__main__":
    # For local use
    db = get_db()
    query = range_query()
    cities = db.cities.find(query)

    print "Found cities:", cities.count()
    import pprint
    pprint.pprint(cities[0])
#MongoDB query reference at https://docs.mongodb.com/manual/reference/operator/query/
"""
example_city.txt
{
 'areaCode': ['916'],
 'areaLand': 109271000.0,
 'country': 'United States',
 'elevation': 13.716,
 'foundingDate': datetime.datetime(2000, 7, 1, 0, 0),
 'governmentType': ['Council\u2013manager government'],
 'homepage': ['http://elkgrovecity.org/'],
 'isPartOf': ['California', u'Sacramento County California'],
 'lat': 38.4383,
 'leaderTitle': 'Chief Of Police',
 'lon': -121.382,
 'motto': 'Proud Heritage Bright Future',
 'name': 'City of Elk Grove',
 'population': 155937,
 'postalCode': '95624 95757 95758 95759',
 'timeZone': ['Pacific Time Zone'],
 'utcOffset': ['-7', '-8']
}
"""
#############################
#Exists
############################
#to start mongo shell locally:Type following command in your terminal

#mongo
#examples for the mongo shell below
"""
use examples [switches to db examples]
db.cities.find() [all the records in  db cities]
db.cities.find({"governmentType" : {"$exists" : 1}}) [all the documents where the field governmentType exists]
db.cities.find({"governmentType" : {"$exists" : 1}}).count() 
db.cities.find({"governmentType" : {"$exists" : 0}}) [all the documents where the field governmentType does not exist]
db.cities.find({"governmentType" : {"$exists" : 1}}).pretty() [pretty prints the output]

"""
######################
#REGEX OPERATOR
#####################
#-MongoDB regex is compatible with PCRE [Perl Compatible Regex Operator]
#-enables regular expression queries

#examples with mongo shell below
"""
db.cities.find({"motto" : {"$regex" : "friendship"}}).pretty
db.cities.find({"motto" : {"$regex" : "[Ff]riendship"}}).count()
db.cities.find({"motto" : {"$regex" : "[Ff]riendship|[Hh]appiness"}}).count()
db.cities.find({"motto" : {"$regex" : "[Ff]riendship|[Pp]ride"}}).count()
db.cities.find({"motto" : {"$regex" : "[Ff]riendship|[Pp]ride"}},{"motto" : 1,"_id" : 0}).pretty() [using a projection to print only the motto from the retrieved documents]
"""

########################
#QUERYING ARRAYS USING SCALARS
#######################

#examples with mongo shell below
"""
db.autos.find({"modelYears" : 1980}).pretty() [this prints all the documents in which 1980 is one of the array value for "model_years". By indexing this array field, it is  possible to make these queries very fast.
"""


########################
#IN OPERATOR
#######################

#examples with mongo shell below
"""
db.autos.find({"modelYears" : {"$in" : [1965, 1966, 1967]}}).count()
"""
