#MongoDB M102 course is very good for beginners
client = MongoClient("mongodb://localhost:27017")
db = client.examples

def skip_lines(input_file, skip):
    for i in range(0, skip):
        next(input_file)

def audit_country(input_file):
    for row in input_file:
        country = row['country_label']
        country = country.strip()
        if (country == "NULL") or (country == ""):
            continue
        if db.countries.find({"name":country}).count() != 1:
            print "Not found:", country

if  __name__ == '__main__':
    input_file = csv.DictReader(open("cities.csv"))
    
############################
#Auditing Accuracy 2
#PETL is a good module for cleaning data/transforming it
#PROBLEMS WITH COUNTRY
############################
#-SOME VALUES ARE ARRAYS
#-COLUMN SHIFT
#-REGEX TO THE RESCUE
#-POSSIBLY VALID COUNTRIES


############################
#Auditing Completeness
############################
#-YOU DON'T KNOW WHAT YOU DON'T KNOW
#-MISSING RECORDS
#-SIMILAR SOLUTION TO ACCURACY
#-NEED REFERENCE DATA


############################
#Auditing Consistency
############################
#-WHICH DATA SOURCE TO TRUST?
#-which entry was collected most recently?
#-which collection method is most reliable?

#Auditing for uniformity in locations in the database
import csv
import pprint

fieldname = "wgs84_pso#lat"
minval = -90
maxval = 90

#the is_array function is missing
#I think it can be written by looking at the data file and finding the delimiter.
#If the delimiter is ',' then split by comma and then check if the length of the list is more than 1 

def skip_lines(input_file, skip):
    for i in range(0, skip):
        next(input_file)

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def audit_float_field(v, counts):
    v = v.strip()
    if v == "NULL":
        counts['nulls'] += 1
    elif v == "":
        counts['empties'] += 1
    elif is_array(v):
        counts['arrays'] += 1
    elif not is_number(v):
        print "Found non number:",v
    else:
        v = float(v)
        if not ((minval < v) and (v < maxval)):
            print "Found out of range value:",v

if __name__=="__main__":
    input_file = csv.DictReader(open("cities3.csv"))
    skip_lines(input_file, 3)
    counts = {"nulls" :  0, "empties" : 0, "arrays" : 0}
    nrows = 0
    for row in input_file:
        audit_float_field(row[fieldname], counts)
        nrows += 1
    print "num cities:", nrows
    print "nulls:", counts['nulls']
    print "empties:", counts['empties']
    print "arrays:", counts['arrays']


####################################
#A LITTLE MORE ABOUT CORRECTING DATA
####################################
#-REMOVING TYPOGRAPHICAL ERRORS
#-VALIDATING AGAINST KNOWN ENTRIES
#-CROSS CHECKING WITH OTHER DATASETS
#-DATA ENHANCEMENT (integrating other data into database to enhance the value)
#-DATA HARMONIZATION (abbreviating certain words etc.)
#-CHANGING REFERENCE DATA
