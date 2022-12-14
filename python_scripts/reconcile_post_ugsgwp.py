import arcpy, time

# Set the workspace.
arcpy.env.workspace = r"C:\Users\marthajensen\Documents\ArcGIS\Projects\Staff_Meeting\ugsgwp.postgres.geology.utah.gov(4).sde"

# Set a variable for the workspace.
adminConn = arcpy.env.workspace

# Get a list of connected users.
userList = arcpy.ListUsers(adminConn)

# Get a list of user names of users currently connected and make email addresses.
#emailList = [user.Name + "@yourcompany.com" for user in arcpy.ListUsers(adminConn)]

# Take the email list and use it to send an email to connected users.
#SERVER = "mailserver.yourcompany.com"
#FROM = "SDE Admin <python@yourcompany.com>"
#TO = emailList
#SUBJECT = "Maintenance is about to be performed"
#MSG = "Auto generated Message.\n\rServer maintenance will be performed in 15 minutes. Please log off."

# # Prepare actual message.
# MESSAGE = """\
# From: %s
# To: %s
# Subject: %s
#
# %s
# """ % (FROM, ", ".join(TO), SUBJECT, MSG)
#
# # Send the email.
# print("Sending email to connected users")
# server = smtplib.SMTP(SERVER)
# server.sendmail(FROM, TO, MESSAGE)
# server.quit()

# Block new connections to the database.
print("The database is no longer accepting connections")
arcpy.AcceptConnections(adminConn, False)

# Wait 15 minutes.
time.sleep(10)

# Disconnect all users from the database.
print("Disconnecting all users")
arcpy.DisconnectUser(adminConn, "ALL")

# Get a list of versions to pass into the ReconcileVersions tool.
# Only reconcile versions that are children of Default.
print("Compiling a list of versions to reconcile")
verList = arcpy.da.ListVersions(adminConn)
versionList = [ver.name for ver in verList if ver.parentVersionName == 'sde.DEFAULT']

# Execute the ReconcileVersions tool.
print("Reconciling all versions")
arcpy.ReconcileVersions_management(adminConn, "ALL_VERSIONS", "sde.DEFAULT", versionList, "LOCK_ACQUIRED", "NO_ABORT", "BY_OBJECT", "FAVOR_EDIT_VERSION", "POST", "KEEP_VERSION")

# Run the compress tool.
print("Running compress")
arcpy.Compress_management(adminConn)

# Allow the database to begin accepting connections again.
print("Allow users to connect to the database again")
arcpy.AcceptConnections(adminConn, True)

# Update statistics and indexes for the system tables.
# Note: To use the "SYSTEM" option, the user must be an geodatabase or database administrator.
# Rebuild indexes on the system tables.
print("Rebuilding indexes on the system tables")
arcpy.RebuildIndexes_management(adminConn, "SYSTEM")

# Update statistics on the system tables.
print("Updating statistics on the system tables")
arcpy.AnalyzeDatasets_management(adminConn, "SYSTEM")

print("Finished.")