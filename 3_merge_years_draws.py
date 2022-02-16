#Calculate future fire spatial statistics
#Process step 3
#Merge statistics for years and draws
#Paulina Marczak December 8 2019


# Import system modules
import os
from os import listdir
from os.path import isfile, isdir, join
import time
import csv
import pandas as pd

print "Starting at:", (time.strftime('%a %H:%M:%S'))
print "Importing modules."

#set workspace to workspace of script
script_dir = os.path.dirname(os.path.realpath(__file__))
get_biomass_dir= os.path.join(script_dir, "biomass")

#Get list of all local biomass directories within biomass folder to run processes on (value_raster_dir)
print "Getting list of subdirectories from each local biomass directory"
file_list = [f for f in listdir(script_dir) if isdir(join(script_dir, f))]
#file_list= ['base', 'miti']
print file_list

value_raster_dir= []

print "Appending subdirectory locations to one list"
# get value_raster_dir which is list of the 4 directories in biomass folder
#list of 4 draw folders so that i can iterate within them and get tif names
for file in file_list[:]:
        if file.endswith("base"):
            value_path = os.path.join(script_dir, file)
            #print value_path
            value_list= os.listdir(value_path)
            for value in value_list[:]:
                if value.startswith("high") and value.endswith("all.csv"):
                    #print file
                    value= os.path.join(value_path, value)
                    value_raster_dir.append(value)
                    #print "value_raster_dir", value_raster_dir

        if file.endswith("miti"):
            value_path = os.path.join(script_dir, file)
            #print value_path
            value_list= os.listdir(value_path)
            for value in value_list[:]:
                if value.startswith("high") and value.endswith("all.csv"):
                    value= os.path.join(value_path, value)
                    value_raster_dir.append(value)

value_raster_dir= sorted(value_raster_dir)
print "value_raster_dir",value_raster_dir 

print "Starting processing biomass data"
out_table_list= []
for i in value_raster_dir[:]:
    #print "Loading snag data from:", i
    filelist= []
    for r,d,f in os.walk(i):
        #for file in each raster directory
        for file in f:
            filelist.append(os.path.join(r, file))
        for entry in filelist[:]:
                if not entry.endswith(".tif"):
                    filelist.remove(entry)
    target = next(f for f in filelist)
    print "target", target

    #Make output save folders
    
    if target.startswith("abs_StemSnags_to_Products"):
        out_data_dir = os.path.join(script_dir, "out_tables_"+ str(target.split("base_fire_")[1].split("\\output")[0]))
        out_table_list.append(out_data_dir)
        print"Saving out statistics data to:", out_data_dir
        cost_data_dir = os.path.join(script_dir, "snag_biomass_cost_"  + str(target.split("base_fire_")[1].split("\\output")[0]))
        print"Saving masked data to:", cost_data_dir
    else:
        out_data_dir = os.path.join(script_dir, "out_tables_" + str(target.split("base_fire_")[1].split("\\output")[0]))
        out_table_list.append(out_data_dir)
        cost_data_dir = os.path.join(script_dir, "snag_biomass_cost_"  + str(target.split("base_fire_")[1].split("\\output")[0]))
        #Save all output rasters to same location (including masked data)
        masked_data_dir = cost_data_dir


print "Merging all draw statistics together"

masterDF= pd.DataFrame()

for i in value_raster_dir[:]:
    with open(i, mode='r') as infile:
                        reader = csv.reader(infile)
                        df= pd.DataFrame(reader)
                        df= df.drop(df.index[0])
                        df.columns=['UNIQUE_ID', 'Mill_ID', 'YEAR', 'STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)','AVERAGE_COST_ZONALCOUNT','AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED']
                        print df
                        #df2=df.reindex(columns=['UNIQUE_ID', 'Mill_ID', 'CostIndex', 'YEAR', 'STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)','AVERAGE_COST_ZONALCOUNT','AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED'])
                        #df2= df.drop(df.index[0])
                        #print df2
                        masterDF=masterDF.append(df)
                        print masterDF
                        #masterDF1.append(df2)

for i in out_table_list[:]:
    #print i
    try:
        print i
        csv_list = [f for f in listdir(i) if isfile(join(i, f))]
        for file in csv_list:
            if file.startswith("high") and file.endswith("all.csv"):
                new_path= os.path.join(i, file)

                with open(new_path, mode='r') as infile:
                    reader = csv.reader(infile)
                    df= pd.DataFrame(reader)
                    df= df.drop(df.index[0])
                    df.columns=['UNIQUE_ID', 'Mill_ID', 'YEAR', 'STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)','AVERAGE_COST_ZONALCOUNT','AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED']
                    print df
                    #df2=df.reindex(columns=['UNIQUE_ID', 'Mill_ID', 'CostIndex', 'YEAR', 'STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)','AVERAGE_COST_ZONALCOUNT','AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED'])
                    #df2= df.drop(df.index[0])
                    #print df2
                    masterDF=masterDF.append(df)
                    print masterDF
                    #masterDF1.append(df2)
    except WindowsError:
        print "Skipping non-existent directory" 

out_test= os.path.join(script_dir, "MCFire_all_draw_statistics.csv")
masterDF.to_csv(out_test, index=False)

print "Finished at:", (time.strftime('%a %H:%M:%S'))
