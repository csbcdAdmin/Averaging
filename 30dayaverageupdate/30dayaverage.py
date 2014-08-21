""" 
    File: 30dayaverage.py
    By  : Reed Shinsato
    Date: August 5, 2014
    Desc:
        This script does a simple average for 30 days worth of data.
"""
import csv
import sys
import datetime
import os

# Define Constants
# VALUEAFTER is the amount of samples you want to wait before writing the row
# MN is the amount of samples that the queue should contain
# MAGICNUMBER is the amount of samples that the queue should use
# MAGICMINI is the amount of samples required before averaging will occur
# OUTPUTFILE is the name of the final output file
# METAFILENAME is the name of the processing file
# FREQUENCY (in seconds) is the time difference in the timestamps in the 
#     final output file
# DATAINDEX is the column for the data values from the input file
# INPUTFILENAME is the default input file that the script will search for
VALUEAFTER = 1
MN = 5
MAGICNUMBER = MN + VALUEAFTER
MAGICMINI = 2
OUTPUTFILENAME = "30dayaverage.csv"
METAFILENAME = "meta_file.csv"
FREQUENCY = 3600
DATAINDEX = 2
INPUTFILENAME = "testfile.csv"

# Simple Queue Class implementation from coursera course:
# RICE University Principles of Computing
class Queue():
    def __init__(self):
        # Initialize the array for storing items
        self._items = []
        
    def __len__(self):
        # Return the lenght of the Queue
        return len(self._items)
        
    def __iter__(self):
        # Iterate over the items in the Queue
        for item in self._items:
            yield item
            
    def __str__(self):
        # Return the list of items as a string
        return str(self._items)
        
    def enqueue(self, item):
        # Add an item to the Queue
        self._items.append(item)
        
    def dequeue(self):
        # Remove the first item from the Queue
        return self._items.pop(0)
        
    def clear(self):
        # Remove all the items from the Queue
        self._items = []

# Average Queue class that takes the average of the queue values
class AverageQueue(Queue):
    def __init__(self):
        # Initialize the Queue class
        Queue.__init__(self)
    
    def __return_valueafter(self):
        # Create a storage list
        valueafter_items = []
        
        # Store the valueafter items of the queue
        # The valueafter items are the last items in the queue that will not be 
        # used for averaging
        for dummy_valueafter in range(VALUEAFTER):
            valueafter_items.append(self._items.pop(-1))
        return valueafter_items
        
    def return_average(self):
        # Take the average of all the elements in the Queue
        # Initialize the numerator and denominator for the average
        temp_items = self.__return_valueafter()
        numerator = 0
        denominator = self.__len__()
        # For each item in the Queue,
        #     Add the item to the numerator if it is valid
        #     Subtrace the denominator by 1 if the item is not valid
        for item in self.__iter__():
            # Subtract 1 from the denominator if the item is not valid
            if item == None or item == "" or item == " ":
                if denominator != 0:
                    denominator -= 1
            # Add the item to the numerator if it is valid
            else:
                if self.__is_valid_entry(item):
                    numerator += float(item)

        for item in temp_items:
            self._items.append(item)
            
        # Take the numerator / denominator to get the average
        # Only return the average if there are more samples than the MAGICMINI
        # If the denominator is 0, then return None
        if denominator == 0:
            return None
        else:
            if denominator >= MAGICMINI:
                return round((float(numerator) / float(denominator)), 2)
            else:
                return None
            
    def return_valid_len(self):
        # Return the length of the Queue for valid items
        length = self.__len__()
        for item in self._items:
            if item == None or item == "" or item == " ":
                length -= 1
        length -= VALUEAFTER
        if length >= MAGICMINI:
            return length
        else:
            return None
        
    def __is_valid_entry(self, item):
        # Check if the item is a valid entry (a number)
        try:
            float(item)
            return True
        except ValueError:
            return False

# Check for the input file name otherwise use a default name            
try:
    input_file_name = sys.argv[1]
except:
    input_file_name = INPUTFILENAME
print "Processing Data ..."

# Open the input file and create the reader
try:
    input_file = open(input_file_name, "r")
except:
    print "Could not find input file\n"
    quit()
input_reader = csv.reader(input_file, delimiter = ",")

# Open the output file and create the writer
output_file = open(METAFILENAME, "wb")
output_writer = csv.writer(output_file, delimiter = ",")

# Get the first line of the input file
input_row = []
input_row = input_reader.next()


header = True
while header == True:
    try:
        timestamp = datetime.datetime.strptime(input_row[0],\
            "%m/%d/%Y %H:%M:%S")
        input_row = input_reader.next()
        header = True
    except:
        header = False
	input_row = input_reader.next()
      
# Create the previous timestamp that will be used for comparison
prev_timestamp = datetime.datetime.strptime(input_row[0],\
    "%m/%d/%Y %H:%M:%S")
output_row = [str(prev_timestamp), input_row[DATAINDEX]]
output_writer.writerow(output_row)

del input_row

# Create a meta file with null values where there are missing points of data
for input_row in input_reader:
    output_row = []
    
    # Get the most recent timestamp from the reader
    current_timestamp = datetime.datetime.strptime(input_row[0],\
        "%m/%d/%Y %H:%M:%S")
    # Check the time difference between the current and previous timestamps
    time_difference = current_timestamp - prev_timestamp
    
    # If the time difference is correct (equal to the FREQUENCY), 
    #     Write the input row
    if int(time_difference.total_seconds()) == FREQUENCY:
        output_row = [str(current_timestamp), input_row[DATAINDEX]]
        output_writer.writerow(output_row)
    # If the time difference is not correct,
    #     Write a new row with a blank value for every timestamp that is 
    #     evenly divisible by the FREQUENCY
    # Update the previous timestamp to the current timestamp of this iteration
    else:
        for second in range(FREQUENCY,\
            int(time_difference.total_seconds())):
            if second % FREQUENCY == 0:
                temporary_timestamp = prev_timestamp +\
                    datetime.timedelta(seconds = second)
                output_row = [str(temporary_timestamp), ""]
                output_writer.writerow(output_row) 
        output_row = [str(current_timestamp), input_row[DATAINDEX]]
        output_writer.writerow(output_row)
    prev_timestamp = current_timestamp    

# Close all the open files
input_file.close()
output_file.close()

# Open the meta file and create the reader
input_file = open(METAFILENAME, "r")
input_reader = csv.reader(input_file, delimiter = ",")

# Open the output file and create the writer
output_file = open(OUTPUTFILENAME, "wb")
output_writer = csv.writer(output_file, delimiter = ",")

# Write the header for the output file
output_row = ["timestamp", "value", "30dayhourlyaverage", "samples"]
output_writer.writerow(output_row)

# Create the AverageQueue 
average_queue = AverageQueue()

# For each input row in the input reader,
#     Get the timestamp, value, average, and number of samples
#     Write the retrieved data
for input_row in input_reader:
    output_row = []
    # Get the timestamp
    timestamp = datetime.datetime.strptime(input_row[0],\
        "%Y-%m-%d %H:%M:%S")
        
    # Get the value
    value = input_row[1]
    
    # Add new data to the averae queue and delete the old
    if len(average_queue) == MAGICNUMBER:
        average_queue.dequeue()
    average_queue.enqueue(value)
    
    # Recheck that the queue is the correct size before averaging
    assert average_queue <= MAGICNUMBER, "The average_queue is too long"
    
    # Get the average of the data in the average queue
    output_average = average_queue.return_average()
    
    # Get the number of valid samples in the average queue
    samples = average_queue.return_valid_len()
    
    # Write the new row of data
    output_row = [timestamp, value, output_average, samples]
    output_writer.writerow(output_row)

# Close all the files
input_file.close()
output_file.close()
# Delete the class
del average_queue
# Remove the meta file
os.remove(METAFILENAME)
print "... Finished"

    