

import arcpy
import datetime
import os
import zipfile
import shutil


from zipfile import ZipFile

date = str(datetime.datetime.now()).split(" ")[0].replace("-", "_")

newPath = r"G:/My Drive/UGWP_Backup/UGWP" + date + "_UGWP_tables"
if not os.path.exists(newPath): os.makedirs(newPath)
else:
    newPath = newPath + "_3"
    os.makedirs(newPath)
File_List = os.listdir(r"C:\Users\marthajensen\AppData\Roaming\ESRI\Desktop10.8\ArcCatalog")

DB_List = [s for s in File_List if "sde@ugsgwp" in s]

for db in DB_List:
    print "Connecting to: " + db
    arcpy.env.workspace = r"Database Connections\\" + db

    tables = arcpy.ListTables()
    db_name = db.split("@")[0] + ".gdb"
    print "Copying data to: " + newPath + '\\' + db_name

    arcpy.CreateFileGDB_management(newPath, db_name)

    try:
        print "Exporting tables..."
        arcpy.TableToGeodatabase_conversion(tables, newPath + '\\' + db_name)
    except:
        print "No featureclasses found."


        