"""takes a simple csv file with no headers:
 [address, city, state, zip] as first 4 fields.
 Geocodes with Google,
 outputs to csv file with original row + geocoded elements:
 [address, lat, lng, type, match]
"""
import urllib2
import json
import csv
import sys
import datetime
import time
import argparse

def do_geocode(address):
    start = datetime.datetime.now()
    url="https://maps.googleapis.com/maps/api/geocode/json?address=%s" % address
    response = urllib2.urlopen(url)
    # print(datetime.datetime.now() - start)
    jsongeocode = response.read()
    return jsongeocode

def geocode(address):
    global retry_count
    res = [""]
    if retry_count < retry_limit:
        result = json.loads(do_geocode(address))

        if result["status"] == "OK":
            retry_count = 0
            top_result = result["results"][0]
            addr = top_result["formatted_address"].encode('ascii', 'ignore')
            lat = top_result["geometry"]["location"]["lat"]
            lng = top_result["geometry"]["location"]["lng"]
            ltype = top_result["geometry"]["location_type"]
            if hasattr(top_result, "partial_match"):
                matchtype = top_result["partial_match"]
            else:
                matchtype = ""
            res = [addr, lat, lng, ltype, matchtype]
        elif result["status"] in ("ZERO_RESULTS", "INVALID_REQUEST"):
            res = ["","","","","NO_RESULT"]
        elif result["status"] in ("OVER_QUERY_LIMIT", "REQUEST_DENIED", "UNKNOWN_ERROR"):
            print("Trying again because {0}".format(result["status"]))
            if result["status"] == "OVER_QUERY_LIMIT":
                retry_count += 1
            time.sleep(1)
            res = geocode(address) #recursion!
        else:
            res = [""]
            raise ValueError("A Problem Occurred: {0}".format(result["status"]))

    else: #retry_count over threshold
        raise ValueError("Too Many Re-Tries {0} - may have hit daily limit {}".format(retry_count, daily_limit))

    return res

#---------------------------------------------------------
# get arguments
parser = argparse.ArgumentParser(description='Google geocode some addresses. Google limits this to 2500 per day. Output file is placed in same folder as input csv.')
parser.add_argument('csv_file', nargs='?', help='{string} Full path to the csv file')
parser.add_argument('num_cols', type=int, nargs='?', help='{integer} Number of columns from left to use for address')
parser.add_argument('ignore_header', type=bool, nargs='?', default=False, help='{Boolean} True to ignore first line (header), default is False')
args = parser.parse_args()

csv_file = args.csv_file
num_cols = args.num_cols
ignore_header = args.ignore_header

# globals
row_count = 0
retry_count = 0
retry_limit = 5 # max number of retries
daily_limit = 2500 # max number of requests under API
initial = datetime.datetime.now()

# file output
out_file_path, f_name = csv_file.rsplit("\\",1)
out_file = "{}\\{}_geocode_result_{}".format(out_file_path, initial.strftime('%Y%m%d_%H%M%S%p'), f_name)

try:
    with open(out_file, 'wb') as output:
        out_writer = csv.writer(output, delimiter=',')

        with open (csv_file, 'rb') as csvfile:
            addr_reader = csv.reader(csvfile, delimiter=',')
            for row in addr_reader:
                if row_count < daily_limit:
                    if ignore_header and row_count == 0:
                        pass
                    else:
                        # get rows from left to num_cols
                        addr = []
                        for i in range(0,num_cols):
                            addr.append(row[i])
                        #create address
                        address = ",".join(addr)
                        address = address.replace(" ", "+")
                        address = address.replace(",",",+")
                        print("Geocode attempt for: ".format(address))
                        result = geocode(address)
                        print(result)
                        out_writer.writerow(row + result)

                    row_count += 1
                else:
                    msg = "Daily limit reached: {}".format(daily_limit)
                    print(msg)
                    out_writer.writerow(row + msg)

except ValueError as err:
    print(err)

finally:
    print("Total Time: {0}".format(datetime.datetime.now() - initial))
    print("Completed")
