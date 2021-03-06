#Calculate future fire spatial statistics
#Process step 2
#Processes local copies of ... spatial layers:
    #1. calculates output statistics per draw csvs
    #2. creates max/min/median/mean per draw csvs for mapping purposes
#Paulina Marczak December 8 2019

#Organization:
    #Must have cost_dest folder in same directory as script- with costpoint, desination, and pixel_area raster in folder
    #Cost/dest rasters are generated by models in ArcGIS- from 
    #M:\...paulina.gdb 
    #and Pixel Area
    #M:\...pixel_area.tif

# Import system modules
import os
from os import listdir

import time

print "Starting at:", (time.strftime('%a %H:%M:%S'))
print "Importing modules."

try:
    import archook  # The module which locates arcgis
    archook.get_arcpy()
    import arcpy
    from arcpy.sa import *
except ImportError:
    print "Import error."

#Import necessary modules
import csv
import sys
import glob
import numpy as np
from os.path import isfile, isdir, join
import setuptools
import subprocess
import shutil
import errno
import fnmatch 

try:
    gdaltranslate = "C:\\Python27\\ArcGISx6410.5\\Lib\\site-packages\\osgeo\\gdal_translate.exe"
except ImportError:
    print "No gdaltranslate found- must set directory"
try:
    gdalwarp = "C:\\Python27\\ArcGISx6410.5\\Lib\\site-packages\\osgeo\\gdalwarp.exe"
except ImportError:
    print "No gdalwarp found- must set directory"
try:
    import pandas as pd
except ImportError:
    print "No pandas found- must install from pip"
print "Done importing modules."

#set workspace
script_dir = os.path.dirname(os.path.realpath(__file__))
#set copy folder directories
get_biomass_dir= os.path.join(script_dir, "biomass")
base_dir= os.path.join(get_biomass_dir, "base")
miti_dir= os.path.join(get_biomass_dir, "miti")
salvage_dir= os.path.join(script_dir, "salvage_zones")
costRastPath = os.path.join(script_dir, "input_gdb")
working_dir = os.path.join(script_dir, "working")
#import costpoint raster (see costpoint model in 
#M:\....\cost_dest.gdb 
#Toolbox- costpoint_KEEP
#for processing steps of raster)
costRast= os.path.join(costRastPath, "costpoint_bc.tif")
#import destination raster (see destination1 model in 
#M:\...cost_dest.gdb 
#combine_unique_id_raster_KEEP
#for processing steps of raster)
destRast =  os.path.join(costRastPath, "combine1.tif")
#when running, make sure mills are still operating.
#if mill not operating, original script should be rerun to select mill destination rank 2 (or so on if rank 2 is shut down)
#import true pixel area for calculating total hectares of biomass per mill
pixel_area = os.path.join(costRastPath, "pixel_area.tif")

#make folders
print"Python script launched from:", script_dir
print"Getting cost raster from", costRast

arcpy.env.overwriteOutput = True

###################
#Process: process key tables for creating output statistical files with indexing information
#append column with mill name based on mill ID


mill_info= [[1,'Elko Sawmill'], #Whether or not a mill closed does not matter within mill_info, because this list only appends mill names that are outputted from above script
[2,'Canfor Taylor Pulp'],
[3,'Northwood Pulp Mill' ],
[4,'Prince George Pulp and Paper and Intercontinental Pulp mills'],
[5,'Cariboo Pulp and Paper Company' ],
[6, 'Crofton Division'],
[7,'Port Alberni Division' ],
[8,'Powell River Division' ],
[9, 'CIPA Lumber Co. Ltd.' ], 
[10, 'Annacis Division' ],
[11, 'Conifex Inc. (SFO)' ],
[12, 'Mackenzie Site 2 & Power Plant' ],
[13, 'Kamloops Mill (SFO)' ],
[14, 'Dunkley Lumber Ltd.'],
[15, 'Howe Sound Pulp and Paper Mill' ],
[16, 'Grand Forks Sawmill' ],
[17, 'Kruger Products L.P.' ],
[18, 'Mackenzie Pulp Mill' ],
[19, 'Harmac Pacific Operations'],
[20, 'Neucel Specialty Cellulose (SFO)' ],
[21, 'Skookumchuck Operation' ],
[22, 'Heffley Creek Division' ],
[23, 'Lavington Planer Mill' ],
[24, 'Nicola Valley Division' ],
[25, 'Quesnel River Pulp' ],
[26, 'West Fraser Mills- Williams Lake' ],
[27, 'Princeton Sawmill' ],
[28, 'Zellstoff Celgar Limited Partnership']]


mill_df = pd.DataFrame(mill_info, columns=['key','NAME'])
mill_df.key= mill_df.key.astype(int)

# mill_dfSave= os.path.join(script_dir, "mapping_folder" + "Mill_IDs.csv")
# mill_df.to_csv(mill_dfSave, index=False)

#this table is just a copy of the raster output entitled "combine" stored in cost_dest.gdb, which was outputted by combining destination mill raster, forest region raster, and stand type rasters.
#To generate combine raster again, use modelbuilder model called "combine" in cost_dest.gdb.

new_path= os.path.join(script_dir, "Combine_Mill_by_DF_forestregion.csv")
with open(new_path, mode='r') as infile:
    MILL_ID_table=csv.reader(infile)
    mill_id_df= pd.DataFrame(MILL_ID_table)
    mill_id_df.columns= ['OBJECTID',
                        'Value',
                        'Count',
                        'Mill_ID',
                        'Reclass_for_1',
                        'Reclass_ldsp1',
                        'costpoint_bc_thr']
    mill_id_df= mill_id_df.drop(mill_id_df.index[0])
    mill_id_df.Mill_ID= mill_id_df.Mill_ID.astype(int)
    mill_id_df.Reclass_ldsp1= mill_id_df.Reclass_ldsp1.astype(int)
    mill_id_df.costpoint_bc_thr= mill_id_df.costpoint_bc_thr.astype(int)

    new_key_to_mills= mill_id_df.merge(mill_df, how='left', left_on= 'Mill_ID', right_on='key')

    #define what coded combine values mean by making key tables
        #regions
    Reclass_for_1=[[32,'Southern Region'], [33,'Coastal Region'], [34,'Northern Interior']]
    Reclass_for_1_df = pd.DataFrame(Reclass_for_1, columns=['Stand1','REGION_NAME'])
    Reclass_for_1_df.Stand1= Reclass_for_1_df.Stand1.astype(int)
    new_key_to_mills.Reclass_for_1=new_key_to_mills.Reclass_for_1.astype(int)
    new_key_to_mills.Reclass_ldsp1=new_key_to_mills.Reclass_ldsp1.astype(int)

        #species
    Reclass_ldsp1=[[30,'Non-Douglas Fir Stand'], [31,'Douglas Fir Stand']]
    Reclass_ldsp1_df = pd.DataFrame(Reclass_ldsp1, columns=['Stand','STAND_TYPE'])
    Reclass_ldsp1_df.Stand= Reclass_ldsp1_df.Stand.astype(int)

        #cost thresholds
    cost_thres = [[1, 'Cost Less than 25'], 
                [2, 'Cost Between 25 and 50'],
                [3, 'Cost Between 50 and 75'], 
                [4, 'Cost Between 75 and 100'], 
                [5, 'Cost Greater than 100']]
    cost_thres_df= pd.DataFrame(cost_thres, columns=['CostIndex', 'COST_THRESHOLD'])
    cost_thres_df.CostIndex= cost_thres_df.CostIndex.astype(int)

    #append names of stand region and cost threshold to master mill key
    new_key_to_mills= new_key_to_mills.merge(Reclass_for_1_df, how='left', left_on= 'Reclass_for_1', right_on='Stand1')
    new_key_to_mills= new_key_to_mills.merge(Reclass_ldsp1_df, how='left', left_on= 'Reclass_ldsp1', right_on='Stand')
    new_key_to_mills= new_key_to_mills.merge(cost_thres_df, how='left', left_on= 'costpoint_bc_thr', right_on='CostIndex')

    #print new_key_to_mills
    # new_key_to_millsSave=os.path.join(out_data_dir, value_raster_dir.split("biomass_")[1] + "newkey.csv")
    # new_key_to_mills.to_csv(new_key_to_millsSave, index=False)
    # print new_key_to_millsSave

#Create empty dataframe to merge outputs
masterDF = pd.DataFrame(columns = ['UNIQUE_ID', 'Mill_ID', 'YEAR','STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)', 'AVERAGE_COST_ZONALCOUNT', 'AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED'])
#A temp masterDF for mapping purposes
masterDF1 = pd.DataFrame(columns = ['UNIQUE_ID', 'Mill_ID', 'CostIndex', 'YEAR','STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)', 'AVERAGE_COST_ZONALCOUNT', 'AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED'])
##################

#Get list of all local biomass directories within biomass folder to run processes on (value_raster_dir)
print "Getting list of subdirectories from each local biomass directory"
file_list = [f for f in listdir(get_biomass_dir) if isdir(join(get_biomass_dir, f))]
#file_list= ['base', 'miti']

value_raster_dir= []

print "Appending subdirectory locations to one list"
# get value_raster_dir which is list of the 4 directories in biomass folder
#list of 4 draw folders so that i can iterate within them and get tif names
for file in file_list[:]:
        if file.endswith("base"):
            value_path = os.path.join(get_biomass_dir, file)
            #print value_path
            value_list= os.listdir(value_path)
            for value in value_list[:]:
                value= os.path.join(value_path, value)
                value_raster_dir.append(value)
                #print value_raster_dir

        if file.endswith("miti"):
            value_path = os.path.join(get_biomass_dir, file)
            #print value_path
            value_list= os.listdir(value_path)
            for value in value_list[:]:
                value= os.path.join(value_path, value)
                value_raster_dir.append(value)

value_raster_dir= sorted(value_raster_dir)

print "This script will be processing the biomass data from the following local directories:", value_raster_dir
#Get a list of all salvage zone directories from local drive copies (event_raster_dir)
#list of 100 draw folders so that i can iterate within them and get tif names
print"This script will be using the salvage masks from the following local directories:", salvage_dir

##Generate list to loop through in processing phase
print "Deciding how many times to run processing"
biomass_list=[]

for i in value_raster_dir[:]:
    # print "Loading snag data from:", i
    filelist= []
    for r,d,f in os.walk(i):
        #for file in each raster directory
        for file in f:
            filelist.append(os.path.join(r, file))
        for entry in filelist[:]:
                if not entry.endswith(".tif"):
                    filelist.remove(entry)
    biomass_list.append(filelist)

#Build iterable salvage event lists for looping/dictionaries/processing
print "Creating list of local salvage folders that match local biomass folders for processing"
file_list_salvage = [f for f in listdir(salvage_dir) if isdir(join(salvage_dir, f))]
#print file_list_salvage
event_raster_dir= []
for file in file_list_salvage[:]:
    value_path = os.path.join(salvage_dir, file)
    event_raster_dir.append(value_path)

#Filter how many local copies of salvage/burn layer to use
for i in sorted(event_raster_dir)[:]:
    def createList(r1, r2): 
        return list(range(r1, r2+1)) 
    r1, r2 = 1, 100
    draws= createList(r1,r2)
    #wildcard= "*"
    extension_list= ['_base', '_miti']
    drawlist= []
    for i in draws[:]:
        for e in extension_list[:]:
            #entry= wildcard + "0" + str(i) + 'high_'+ e
            #entry_max= wildcard + "high_" + "draw" + "0"+ str(i) + e
            if i <10:
                entry_max= "high_" + "draw" + "00"+ str(i) + e
            elif i <100:
                entry_max= "high_" + "draw" + "0"+ str(i) + e
            else:
                entry_max= "high_" + "draw" + str(i) + e
            #drawlist.append(entry)
            drawlist.append(entry_max)

    #Search output directory for instances of draw names and add those to the ignore list 
    dictionary_event_raster_dir=[]
    # for x in drawlist[:]:
    #     for file in os.listdir(script_dir):
    #         if fnmatch.fnmatch(file, x):
    #             #print x, "exists as a biomass layer so salvage mask directory is retained"
    #             x= x.split("high_")[1].split("_")[0]
    #             #print x
    #             dictionary_event_raster_dir.append(os.path.join(salvage_dir,x))
    # for x in drawlist[:]:
    #     for r,d,f in os.walk(get_biomass_dir):
    #         for name in d:
    #             #print dir
    #             if fnmatch.fnmatch(name, x):
    #                 x= x.split("high_")[1].split("_")[0]
    #                 print x, "is a burn layer to add to dictionary for sorting"
    #                 dictionary_event_raster_dir.append(os.path.join(salvage_dir,x))

    for x in drawlist[:]:
        for i in value_raster_dir[:]:
            i = i.split("base_fire_")[1]
            #print x
            #print i
            if x==i:
                x= x.split("high_")[1].split("_")[0]
                #print x, "is a burn layer to add to dictionary for sorting"
                dictionary_event_raster_dir.append(os.path.join(salvage_dir,x))

#Remove duplicate folder structure
dictionary_event_raster_dir = list(dict.fromkeys(dictionary_event_raster_dir))
#print "Salvage folders to process biomass layers with are", dictionary_event_raster_dir

#make a dictionary for each of the biomass miti/base folders (so far there's 4, and match with the 2 salvage zones)
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
    #print "target", target

    #Make output save folders
    
    if target.startswith("abs_StemSnags_to_Products"):
        out_data_dir = os.path.join(script_dir, "out_tables_" + str(target.split("base_fire_")[1].split("\\output")[0]))
        out_table_list.append(out_data_dir)
        #if  os.path.exists(out_data_dir):
            #shutil.rmtree(out_data_dir)
        if not os.path.exists(out_data_dir):
            os.makedirs(out_data_dir)
            print"Saving out statistics data to:", out_data_dir
        cost_data_dir = os.path.join(script_dir, "snag_biomass_cost_"  + str(target.split("base_fire_")[1].split("\\output")[0]))
        #if  os.path.exists(cost_data_dir):
            #shutil.rmtree(cost_data_dir)
        if not os.path.exists(cost_data_dir):
            os.makedirs(cost_data_dir)
            print"Saving masked data to:", cost_data_dir
    else:
        out_data_dir = os.path.join(script_dir, "out_tables_" + str(target.split("base_fire_")[1].split("\\output")[0]))
        out_table_list.append(out_data_dir)
        #if  os.path.exists(out_data_dir):
            #shutil.rmtree(out_data_dir)
        if not os.path.exists(out_data_dir):
            os.makedirs(out_data_dir)
            print"Saving out statistics data to:", out_data_dir
        cost_data_dir = os.path.join(script_dir, "snag_biomass_cost_"  + str(target.split("base_fire_")[1].split("\\output")[0]))
        #if  os.path.exists(cost_data_dir):
            #shutil.rmtree(cost_data_dir)
        if not os.path.exists(cost_data_dir):
            os.makedirs(cost_data_dir)
        print"Saving cost data to:", cost_data_dir
        #Save all output rasters to same location (including masked data)
        masked_data_dir = cost_data_dir

    # Build a matching dictionary of the files to match
    overlay_dict = {}

    #For each file in each local biomass directory make a dictionary conditional function, starting with a-z
    for file1 in sorted(filelist)[:]:
        #print "this is the sorted filelist", filelist
        #tifs are full name therefore remove os.path.join
        #print file1, 'file1'
        target1 = file1
        file1_parts = file1.split(".")
        name1_parts = file1_parts[0].split("_")
        name1 = name1_parts[0]
        year1 = name1_parts[(len(name1_parts) - 1)]
        #print "year 1 is", year1
        dict1 = file1
        draw1_pre= file1.split("bc_fire_uncertainty_harvest_base_fire_high_")[1].split("\\output")[0]
        draw1=draw1_pre.split("_")[0]
        #print "draw1 is", draw1
        #print "file 1 is", target1
        #For each salvage directory, try to find a match, starting with a-z
        for j in sorted(dictionary_event_raster_dir)[:]:
            event_file_list= []
            for r,d,f in os.walk(j):
                #for file in each raster directory
                #if subdirectory draw number/pattern matches the draw number/pattern using for biomass then match files
                for file in f:
                    event_file_list.append(os.path.join(r, file))
                for entry in sorted(event_file_list)[:]:
                        if not entry.endswith(".tif"):
                            event_file_list.remove(entry)
                        for file2 in sorted(event_file_list):
                            #print "file2 is", file2
                            target2 = file2
                            file2_parts = file2.split(".")
                            name2_parts = file2_parts[0].split("_")
                            name2 = name2_parts[0]
                            year2 = name2_parts[(len(name2_parts) - 1)]
                            draw2= file2.split("salvage_zones\\")[1].split("\\burn_")[0]
                            #draw2= file2.split("salvage_zones\\")[1].split("\\salvage_")[0]
                            #print "year 2 is", year2
                            #print "draw2 is", draw2
                            if year2 == year1 and draw2 == draw1:
                                dict2 = target2
                                overlay_dict[dict1] = dict2
                                #print "dictionary match found for", target1, target2
    
    print "Dictionary for draw", draw1,"is", (overlay_dict)

    #Create rest of summary columns and append to first output table
    for value_tif, event_tif in sorted(overlay_dict.items()):


            arcpy.CheckOutExtension("spatial")
            #Mask biomass layers by salvage zone layers
            print "Currently processing outputs for", i.split("high_")[1]
            print "Masking", value_tif, "with", event_tif
            outraster = os.path.join(cost_data_dir,  os.path.basename(value_tif).split(".")[0] + "_masked_"+ i.split("bc_fire_uncertainty_harvest_")[1] + ".tif")
            arcpy.env.workspace = masked_data_dir
            arcpy.Delete_management(outraster)
            out_extract= arcpy.sa.ExtractByMask(value_tif, event_tif)
            out_extract.save(outraster)
            print "Compressing", outraster
            compressed_tiff = os.path.join(cost_data_dir, os.path.basename(outraster).split(".")[0] + '_compressed.tif')
            cmd = [gdaltranslate, outraster, compressed_tiff,  "-co", "tiled=yes", "-co", "compress=lzw"]
            sp = subprocess.Popen(cmd)
            sp.communicate()
            print "Masked and compressed biomass layer saved to", cost_data_dir

            # TOTAL_COST(avg cost for cost_rast masked by residue masked layer)
            # mask costRast by masked output
            print "Masking costpoint Raster by", compressed_tiff.split("_masked_base_fire_high_")[1]
            costMask = os.path.join(cost_data_dir,  os.path.basename(value_tif).split(".")[0] + i.split("bc_fire_uncertainty_harvest_")[1] + "_costmasked.tif")
            cost_Mask= arcpy.sa.ExtractByMask(costRast, compressed_tiff)
            cost_Mask.save(costMask)
            print "Compressing", costMask
            costMask_compressed = os.path.join(cost_data_dir, os.path.basename(costMask).split(".")[0] + '_compressed.tif')
            cmd = [gdaltranslate, costMask, costMask_compressed,  "-co", "tiled=yes", "-co", "compress=lzw"]
            sp = subprocess.Popen(cmd)
            sp.communicate()
            print "Masked and compressed costpoint layer saved to", cost_data_dir
            print "Getting Average Cost per Mill Zones (total_cost column)"
            total_cost = os.path.join(out_data_dir,  "total_cost.dbf")
            output_totalcost= arcpy.sa.ZonalStatisticsAsTable(destRast, "Value", costMask_compressed, total_cost, "DATA", "ALL")

            # COST_CALC (sum of oven-dried biomass cost in $ per mill)
            print "Creating Cost Tif for", compressed_tiff.split("\\snag_biomass_cost_high_draw")[1], "with", costRast
            biomass_cost = os.path.join(masked_data_dir,  os.path.basename(value_tif).split(".")[0] + "_cost")
            out_cost = out_extract * 2 * costRast
            # #out_cost.save(biomass_cost)
            null_cost=arcpy.sa.SetNull(out_cost <= 0, out_cost)
            print "Masking negative values for temp biomass cost per pixel out file with NoData"
            nullcostsave = os.path.join(cost_data_dir,  os.path.basename(value_tif).split(".")[0] + i.split("bc_fire_uncertainty_harvest_")[1] + "_costnull.tif")
            arcpy.Delete_management(nullcostsave)
            null_cost.save(nullcostsave)
            print "Compressing", nullcostsave
            nullcostsave_compressed = os.path.join(cost_data_dir, os.path.basename(value_tif).split(".")[0] + '_costnull'+ '_compressed.tif')
            cmd = [gdaltranslate, nullcostsave, nullcostsave_compressed,  "-co", "tiled=yes", "-co", "compress=lzw"]
            sp = subprocess.Popen(cmd)
            sp.communicate()
            print "Masked and compressed costpoint layer saved to", cost_data_dir
            costcalc = os.path.join(out_data_dir,  os.path.basename(value_tif).split(".")[0] + "cost_calc.dbf")
            output_costcalc= arcpy.sa.ZonalStatisticsAsTable(destRast, "Value", nullcostsave_compressed, costcalc, "DATA", "SUM")

            #RESIDUE_VALUE column
            print "Calculating total biomass (Tc) for fibre sheds based on", out_extract
            residue = os.path.join(out_data_dir,  os.path.basename(value_tif).split(".")[0] + "residue_val.dbf")
            residue_out=arcpy.sa.ZonalStatisticsAsTable(destRast, "Value", out_extract, residue, "DATA", "SUM")

            #AREA column
            print "Extracting total area(ha) per mill for", value_tif
            area_out = os.path.join(masked_data_dir,  os.path.basename(value_tif).split(".")[0] + "_area.tif")
            arcpy.env.workspace = masked_data_dir
            area_extract=arcpy.sa.ExtractByMask(pixel_area, out_extract)
            area_extract.save(area_out)
            print "Compressing", area_out
            area_extract_compressed = os.path.join(cost_data_dir, os.path.basename(value_tif).split(".")[0] + '_area'+'_compressed.tif')
            cmd = [gdaltranslate, area_out, area_extract_compressed,  "-co", "tiled=yes", "-co", "compress=lzw"]
            sp = subprocess.Popen(cmd)
            sp.communicate()
            print "Masked and compressed area layer saved to", cost_data_dir
           
            area = os.path.join(out_data_dir,  os.path.basename(value_tif).split(".")[0] + "area.dbf")
            area_out1=arcpy.sa.ZonalStatisticsAsTable(destRast, "Value", area_extract_compressed, area, "DATA", "SUM")

            #merge tables
            print "Joining tables for", value_tif
            join_out = os.path.join(os.path.basename(value_tif).split(".")[0] + "_merged.csv")
            joined=arcpy.JoinField_management(output_costcalc, "Value", residue_out, "Value", "SUM")
            joined=arcpy.JoinField_management(output_costcalc, "Value", area_out1, "Value", "SUM")
            joined=arcpy.JoinField_management(output_costcalc, "Value", output_totalcost, "Value", "MEAN")
            joined=arcpy.JoinField_management(output_costcalc, "Value", output_totalcost, "Value", "COUNT")
            joined=arcpy.JoinField_management(output_costcalc, "Value", output_totalcost, "Value", "SUM")

            #make CSVs
            arcpy.TableToTable_conversion(output_costcalc, out_data_dir, join_out, where_clause="", field_mapping='Value "Value" true true false 10 Long 0 10 ,First,#,' + str(output_costcalc) + 'Value,-1,-1;Mill "NewFieldName" true true false 50 Text 0 0 ,First,#;Total_Cost "Total_Cost" true true false 19 Double 0 0 ,First,#,' + str(output_costcalc) + 'MEAN,-1,-1;Residue_Value "Residue_Value" true true false 19 Double 0 0 ,First,#,' + str(output_costcalc) + 'SUM_1,-1,-1;Pixel_Area "Pixel_Area" true true false 19 Double 0 0 ,First,#,' + str(output_costcalc) + 'SUM_12,-1,-1;Cost_Calc "Cost_Calc" true true false 19 Double 0 0 ,First,#,' + str(output_costcalc) + 'SUM,-1,-1', config_keyword="")
            print "Deleting temp layer-", outraster
            arcpy.Delete_management(outraster)
            print "Deleting temp layer-", costMask
            arcpy.Delete_management(costMask)
            # print "Deleting temp layer-", compressed_tiff
            # arcpy.Delete_management(compressed_tiff)
            print "Deleting temp layer-", nullcostsave
            arcpy.Delete_management(nullcostsave)
            # print "Deleting temp layer-", nullcostsave_compressed
            # arcpy.Delete_management(nullcostsave_compressed)
            print "Deleting temp layer-", area_out
            arcpy.Delete_management(area_out)
            # print "Deleting temp layer-", area_extract_compressed
            # arcpy.Delete_management(area_extract_compressed)

    #Check in python extension
    arcpy.CheckInExtension("spatial")

    csv_list = [f for f in listdir(out_data_dir) if isfile(join(out_data_dir, f))]
    for file in csv_list[:]:
            if file.endswith("merged.csv"):
                # csv_list.remove(file)
                # print csv_list
                new_path= os.path.join(out_data_dir, file)
                #print value_path
                out_test= os.path.join(out_data_dir, os.path.basename(file).split(".")[0] + "name.csv")

                with open(new_path, mode='r') as infile:
                    reader = csv.reader(infile)
                    df= pd.DataFrame(reader)
                    #rename output columns
                    df.columns = ['ID', 'UNIQUE_ID', 'COUNT', 'IGNORE', 'TOTAL_COST($)', 'RESIDUE_VALUE(Tc)', 'AREA(ha)', 'AVERAGE_COST($/ODT)', 'AVERAGE_COST_ZONALCOUNT', 'AVERAGE_COST_ZONALSUM']#, 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED']
                    #name1= os.path.basename(file).split("Snag_")[1].split("_merged")[0]
                    #df['YEAR']= os.path.basename(file).split("Snags_")[1].split("_merged")[0]
                    #might have to switch between snag and products in year
                    #deleted year/draw and moved down here.
                    df= df.drop(df.index[0])
                    df= df.drop(['ID', 'COUNT', 'IGNORE'], axis= 1)
                    df.UNIQUE_ID=df.UNIQUE_ID.astype(int)
                    new_key_to_mills.Value= new_key_to_mills.Value.astype(int)
                    df= new_key_to_mills.merge(df, how='left', left_on='Value', right_on='UNIQUE_ID').fillna(0)

                    if file.startswith("abs_Merch_to_StemSnags"):
                        df['YEAR']= os.path.basename(file).split("StemSnags_")[1].split("_merged")[0]
                        df['DRAW']= i.split("bc_fire_uncertainty_harvest_base_fire_high_")[1].split("_base")[0] + "_high" + "_base"
                    else:
                        df['YEAR']= os.path.basename(file).split("Products_")[1].split("_merged")[0]
                        df['DRAW']= i.split("bc_fire_uncertainty_harvest_base_fire_high_")[1].split("_miti")[0] + "_high" + "_miti"
                    #df= df.merge(new_key_to_mills, how='left', left_on='UNIQUE_ID', right_on='Value').fillna(0)
                    #index must have same names and columns as the masterDF for best use
                    df['RESIDUE_VALUE(Tc)']=df['RESIDUE_VALUE(Tc)'].astype(float)
                    df['STEM_SNAG_BIOMASS(ODT)']= df['RESIDUE_VALUE(Tc)'].mul(2)
                    df['TOTAL_COST($)']=df['TOTAL_COST($)'].astype(float)
                    df1=df.reindex(columns=['UNIQUE_ID', 'Mill_ID', 'YEAR', 'STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)','AVERAGE_COST_ZONALCOUNT','AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED'])
                    
                    #make statistical output tables
                    masterDF= masterDF.append(df1)
                    df1.to_csv(out_test, index= False)
                    masterDFsave=os.path.join(out_data_dir, i.split("bc_fire_uncertainty_harvest_base_fire_")[1] + "all.csv")
                    masterDF.to_csv(masterDFsave, index=False)

                    #make temp table with costIndex key for joining some stats
                    df2=df.reindex(columns=['UNIQUE_ID', 'Mill_ID', 'CostIndex', 'YEAR', 'STAND_TYPE', 'REGION_NAME', 'DRAW', 'COST_THRESHOLD', 'NAME','TOTAL_COST($)', 'STEM_SNAG_BIOMASS(ODT)', 'AREA(ha)', 'AVERAGE_COST($/ODT)','AVERAGE_COST_ZONALCOUNT','AVERAGE_COST_ZONALSUM', 'TOTAL_DISTANCE', 'TOTAL_DISTANCE_PAVED', 'TOTAL_DISTANCE_UNPAVED'])
                    masterDF1= masterDF1.append(df2)
                    #df.to_csv(out_test, index= False)
                    masterDF1save=os.path.join(out_data_dir, i.split("bc_fire_uncertainty_harvest_base_fire_")[1] + "all1.csv")
                    masterDF1.to_csv(masterDF1save, index=False)


                    #         if not (file.endswith("_mergedname.csv")) and not (file.endswith("all.csv")) and not (file.endswith("_merged.csv")):
                    #             #delete temp files
                    #             new_path= os.path.join(out_data_dir, file)
                    #             os.remove(new_path)
                    #print masterDF1


    #Calculate aggregate statistics csv for mapping purposes
    newdf=masterDF1.groupby(['Mill_ID', 'CostIndex'], as_index= False).agg({
        'STEM_SNAG_BIOMASS(ODT)':['median','min','max','mean'],
        'TOTAL_COST($)': ['median','min','max','mean']
        },
    )


    # #separate column for each threshold?

    newdf.columns = newdf.columns.droplevel(level=0)

    #create key to join table to output shapefile mills_dissolved_gridcode based on key matching to combine_mill_thres_

    #cant rename in above command because older version of pandas to work with python 2.7
    #DO NOT add special characters like $ or () in column names due to ArcMap not liking them in the join procedure for mapping
    newdf.columns=[
     'MILL_ID',
     'CostIndex',
     'STEM_SNAG_BIOMASS_ODT_MED',
     'STEM_SNAG_BIOMASS_ODT_MIN', 
     'STEM_SNAG_BIOMASS_ODT_MAX',
     'STEM_SNAG_BIOMASS_ODT_MEAN',
     'TOTAL_COST_MED',
     'TOTAL_COST_MIN', 
     'TOTAL_COST_MAX',
     'TOTAL_COST_MEAN'
     ]
    newdf=newdf.round(2)
    newdf.MILL_ID= newdf.MILL_ID.astype(int)
    newdf.CostIndex= newdf.CostIndex.astype(int)

    #pd.set_option('display.float_format', '{:.2f}'.format)
    # masterDFsave=os.path.join(out_data_dir, value_raster_dir.split("biomass_")[1] + "all_aggregate.csv")
    # newdf.to_csv(masterDFsave, index=False)

    #One way of mapping is to map per mill catchment, with each bar on map representing stats for next threshold.
    #Condensed/neater version of mapping per mill/cost thres.
    table= pd.pivot_table(masterDF1, values= 'STEM_SNAG_BIOMASS(ODT)', index=['Mill_ID'], columns=['CostIndex'],
           aggfunc={'STEM_SNAG_BIOMASS(ODT)': [min, max, np.median, np.mean]}, fill_value=0)
    table=table.round(2)
    mi= table.columns
    ind = pd.Index([e[0] + str(e[1]) for e in mi.tolist()])

    #change the max of median columns to "mead_max" for arcmap compatibility *spell it wrong on purpose*
    table.columns = ind
    table['Maxmax']= table.iloc[:, 1:5].max(axis=1)
    table["Maxmax"]=table["Maxmax"].max()

    table['Maxmean']= table.iloc[:, 6:11].max(axis=1)
    table["Maxmean"]=table["Maxmean"].max()

    table['Maxmead']= table.iloc[:, 12:16].max(axis=1)
    table["Maxmead"]=table["Maxmead"].max()

    table['Maxmin']= table.iloc[:, 17:21].max(axis=1)
    table["Maxmin"]=table["Maxmin"].max()

    table = table.iloc[:, np.r_[0:5, -4, 6:10, -3, 11:15, -2, 16:20, -1]]

    tableSave=os.path.join(out_data_dir, i.split("bc_fire_uncertainty_harvest_base_fire_high_")[1] + "_pivot_table_new.csv")
    table.to_csv(tableSave)

    #Percentage of accessible biomass within cost thresholds (<25, <50, <75, <100, 100+) for each Timber Supply Area
    #pixel value map for each of the mills for total_cost. so each pixel will show range of values of total cost.
    #task: residue_value split by total cost per tonne. when you take wood products thats what u turn it into, so 50% sawnwood (2x4s) 19.1% panels, 2.5% other, 28.3% pulp. so cost factor per tonne/odt.


    #delete temp folder
    info_tree=os.path.join(out_data_dir, "info")
    try:
        shutil.rmtree(info_tree)
    except OSError:
        pass
        print "Folder doesn't exist"

print "Merging all draw statistics together"
for i in out_table_list[:]:
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

out_test= os.path.join(script_dir, "MCFire_all_draw_statistics.csv")
masterDF.to_csv(out_test, index=False)




print "Finished at:", (time.strftime('%a %H:%M:%S'))
