#Calculate future fire spatial statistics
#Process step 1
#Copy ... spatial outputs to local drive for fast processing
#Copies only spatial outputs relevant for step 2, and which havent been processed yet.
#Deletes tifs if already in biomass folder, therefore those should be processed in step 2 first.
#Paulina Marczak December 8 2019

#Organization:
	#Must have all scripts (1_,2_,3_) saved in same directory to copy files to right location.

# Import system modules
import os
from os import listdir
from os.path import isfile, isdir, join
import time
import shutil
import fnmatch
import re
import distutils
from distutils import dir_util

import csv
import sys
import glob
import numpy as np
import setuptools
import subprocess
import shutil
import errno

print "Starting at:", (time.strftime('%a %H:%M:%S'))
print "Importing modules."

#set workspace
script_dir = os.path.dirname(os.path.realpath(__file__))
#set copy folder directories
get_biomass_dir= os.path.join(script_dir, "biomass")
base_dir= os.path.join(get_biomass_dir, "base")
miti_dir= os.path.join(get_biomass_dir, "miti")
salvage_dir= os.path.join(script_dir, "salvage_zones")


######################
# #Process: if relevant stem snag rasters and salvage masks exist copy them and run statistical calculations script
#check_folder= "M:\\..."
check_folder= "O:\\...."

print "Accessing network folder to copy biomass data from", check_folder
#Years to ignore not relevant in this process- exclude those rasters in copying
print "Creating list of files to exclude from copying for biomass layers- these folders should already be processed in step 2"

#we are dealing with future fire statistics, therefore only 2020+ rasters are needed
def createList(r1, r2): 
    return list(range(r1, r2+1)) 
r1, r2 = 1990, 2019
years_to_ignore= createList(r1,r2)
extension= ".tif"
wildcard= "*"
#must assign twice because https://stackoverflow.com/questions/9724356/python-list-extension-and-variable-assignment
values_to_ignore_base = [wildcard + str(x) + extension for x in years_to_ignore] 
values_to_ignore_miti = [wildcard + str(x) + extension for x in years_to_ignore] 

#different conditions for base and miti runs
mylist_base = ['config',
'abs_AG*',
'abs_All*',
'abs_StemSnags_to_Products*',
'*miti',
'Age*',
'compiled*',
'log_summary.txt',
'draft',
'ha*']

mylist_miti = ['config',
'abs_AG*',
'abs_All*',
'abs_Merch_to_StemSnags*',
'Age*',
'*base',
'compiled*',
'log_summary.txt',
'draft',
'ha*']

#Combine values to ignore depending on MC scenario
values_to_ignore_base.extend(mylist_base)
values_to_ignore_miti.extend(mylist_miti)

#Additionally, don't copy files if draw has already been processed according to contents in output directory
def createList(r1, r2): 
    return list(range(r1, r2+1)) 
r1, r2 = 1, 100
draws= createList(r1,r2)
wildcard= "*"
extension_list= ['_base', '_miti']
drawlist= []
for i in draws[:]:
    for e in extension_list[:]:
        #entry= wildcard + "0" + str(i) + 'high_'+ e
        #entry_max= wildcard + "high_" + "draw" + "0"+ str(i) + e
        if i <10:
            entry_max= wildcard + "high_" + "draw" + "00"+ str(i) + e
        else:
            entry_max= wildcard + "high_" + "draw" + "0"+ str(i) + e
        #drawlist.append(entry)
        drawlist.append(entry_max)

#Search output directory for instances of draw names (drawlist) and add those to the ignore list (values_to_ignore_base/miti)
#want names of subfolders, then if that subfolder matches x in drawlist, add to the values_ignore_list.
# for x in drawlist[:]:
#     for r,d,f in os.walk(get_biomass_dir):
#         for dir in d:
#             #print dir
#             if fnmatch.fnmatch(dir, x):
#                 print x, "already exists so files won't be copied"
#                 values_to_ignore_base.append(x)
#                 values_to_ignore_miti.append(x)

for x in drawlist[:]:
    for file in os.listdir(script_dir):
        if fnmatch.fnmatch(file, x):
            print x, "already exists so files won't be copied"
            values_to_ignore_base.append(x)
            values_to_ignore_miti.append(x)
        else
            print "Copying all available draws from src folder"
####THIS WILL HAVE TO BE FIXED TO SCAN FOR FILES IN SUBDIRECTORIES WHEN RERUN PART 2, and ignore folders ##############

#Make copy function that ignores cumulative list of values that we don't want
#Using the wildcard on the values_to_ignore list
def copy_files(src, dest, valuestoignore):
    try:
        if os.path.exists(dest):
            shutil.rmtree(dest)
        shutil.copytree(src, dest, ignore=shutil.ignore_patterns(*valuestoignore))
        print "Copied biomass files successfully from", src, "to", dest
        #and ignore matching file extensions
    except OSError as e:
        # If the error was caused because the source wasn't a directory
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            print('Directory not copied. Error: %s' % e)


#process wont copy things properly if folders exist but havent been spatially processed. 
print "Copying biomass files for processing from", check_folder, "to", base_dir
copy_files(check_folder, base_dir, values_to_ignore_base)
print "Copying biomass files for processing from", check_folder, "to", miti_dir
copy_files(check_folder, miti_dir, values_to_ignore_miti)

# #Repeat process for salvage zones

check_folder_salvage= "M...."
print "Accessing network folder to copy salvage layer data from", check_folder_salvage

#Remove unecessary folders
#REMOVE SALVAGE INSTEAD OF BURN
mylist_salvage = ['salvage*',
    '*.xml',
    '*.ovr']

#Additionally, don't copy files if draw has already been processed according to contents in output directory
#DONT DO JUST COPY ALL AT ONCE just keep all salvage layers in local dir
print "Creating list of files to exclude from copying for salvage layers"
def createList(r1, r2): 
    return list(range(r1, r2+1)) 
r1, r2 = 01, 100
draws= createList(r1,r2)
wildcard= "*"
drawlist= []
for i in draws[:]:
    if i <10:
        salvage_entry= wildcard + "draw" + "00"+ str(i)
    else:
        salvage_entry= wildcard+ "draw" + "0"+ str(i)
    #print salvage_entry
    #drawlist.append(entry)
    drawlist.append(salvage_entry)

#Search output directory for instances of draw names and add those to the ignore list 
for x in drawlist[:]:
    for file in os.listdir(script_dir):
        if fnmatch.fnmatch(file, x):
            #it only takes off 056 strangely.
            #cuz only reading files not folders
            #also need to match salvage layers to biomass layers via new dictionary.
            print x, "already exists so these burn zone masks aren't being copied"
            mylist_salvage.append(x)

#print "Copying salvage zone files for processing to local drive"
#copy_files(check_folder_salvage, salvage_dir, mylist_salvage)

##################
print "Finished at:", (time.strftime('%a %H:%M:%S'))
