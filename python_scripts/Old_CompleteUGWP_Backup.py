

import arcpy, os
from arcpy import env
from time import strftime
import shutil

# Set the workspace for ListFeatureClasses
arcpy.env.workspace = r"C:\Users\marthajensen\AppData\Roaming\ESRI\Desktop10.7\ArcCatalog\sde@ugsgwp.nrwugspgressp.sde"


#name of file geodatabase, append with date
out_name = "Ugsgwp_Backup2" + time.strftime ("%b%d%y") + ".gdb"

#file geodatabase location
path = r"M:\My Drive\UGWP_Backup"

#create file geodatabase
db = arcpy.CreateFileGDB_management(path, out_name)

#List feature classes, then copy feature classes to the file geodatabase
# for fc in arcpy.ListFeatureClasses():
#      arcpy.FeatureClassToGeodatabase_conversion(fc, db)
#
# print("all feature classes have been exported")

#List the tables then copy the tables to the file geodatabase
for table in arcpy.ListTables():
    arcpy.TableToGeodatabase_conversion(table,db)
    
    print("all tables have been exported")

    #shutil.make_archive(out_name, 'zip', out_name)
    #arcpy.Delete_management(db)

print ("done!")








