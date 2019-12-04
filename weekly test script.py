import System
from System import DateTimeOffset
from System import *
from System.Collections.Generic import *

#================Import From S3================
import System
from System import *
from System.Collections.Generic import *
from System import IO
from System.IO import File
from System.Text import Encoding
from System import *
from System.Collections.Generic import *
import System

filepath = Util.CreateFilePath()
App.Log(filepath)
filesTodump = Dictionary[str,str]()
filesTodump.Add('LocalFilePath',filepath)

ChangedDate=System.DateTime.Now
Day = str(ChangedDate.Date.Day).zfill(2)
Month = str(ChangedDate.Date.Month).zfill(2)
Year = str(ChangedDate.Date.Year).zfill(4)[2:4]
TodayDate=Year+Month+Day
App.Log(TodayDate)


Files = ["Position","PositionProductMap","PositionSpecialtyMap","SalesTeam","SalesTeamAlignmentEntityTypeInclusionRules","AccountCoTSalesTeamMap","AccountExclusion","AccountInclusion","AccountSubCatSalesTeamMap","CallDeckAccount","CallDeckIndividual","GlobalAccountExclusion","GlobalIndividualExclusion","HBPCoTSalesTeamMap","Hierarchy","IndividualExclusion","IndividualInclusion"]

for i in range(0,len(Files)):
    App.Log(Files[i])
    overWriteParams = Dictionary[str, object]()
    overWriteParams.Add('FileName',TodayDate+"/21/Output_JAC/"+Files[i]+".txt")
    overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01")

    mapper_1=App.Data.CreateFileMapper()
    fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    App.Log(str(response_1.Content))
    App.Log(str(response_1.EndPoint))
    App.Log(str(response_1.IsSuccess))
    App.Log(str(response_1.ResponseType))
    

    filename=filepath+'\\'+str(Files[i])+'.txt'
    # App.Log(filename)
    # text = File.ReadAllText(filename)
    # App.Log(File.Exists(filename))
    # File.WriteAllText(filename,text,Encoding.Unicode)
    # App.Log(filename)
    # text = File.ReadAllText(filename)
    # text=text.Replace("\n", "\r\n")
    # App.Log(File.Exists(filename))
    # if File.Exists(filename):
        # App.Log("Converting File Encoding")
        # File.WriteAllText(filename,text,Encoding.UTF8)
    
    response=App.Data.Import(filename, mapper_1, True)
    App.Log(response)
    App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    App.Log(response.GetStatus(str(Files[i]+".txt")))

    if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        App.Log("Data Load Issue")  
        app.Log("error")

    if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        App.Log("File Has Object Reference Issue")       
        app.Log("error")


filepath = Util.CreateFilePath()
App.Log("Completed Import From S3")
filesTodump = Dictionary[str,str]()
filesTodump.Add('LocalFilePath',filepath)
#================/Import From S3================

#================#================#================#================Constant Module#================#================#================#================
currentdate=System.DateTime.Now
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
# Check=(Month+"/"+Day+"/"+Year+" 00:00:00 AM +00:00")
Check=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("Check : " + str(Check))
check1=(Month+"/"+Day+"/"+Year)
App.Log("Check1 : " + str(check1))

check2=("9999-12-31 00:00:00 -05:00")

Number=1
#================First Day of Month Next Month Non Mad
date=System.DateTime.Now.Day
month=System.DateTime.Now.Month
year=System.DateTime.Now.Year
daysInMonth=System.DateTime.DaysInMonth(year,month)
daysAdded=daysInMonth-date
endDate=System.DateTime.Now.AddDays(daysAdded+1).AddMonths(Number-1)
currentdate=endDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
StartDate_Nonmad=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("StartDate_Nonmad : " + str(StartDate_Nonmad))

Number=0
#================Last Day of Month Month Non Mad
date=System.DateTime.Now.Day
month=System.DateTime.Now.AddMonths(Number).Month
year=System.DateTime.Now.Year
daysInMonth=System.DateTime.DaysInMonth(year,month)
daysAdded=daysInMonth-date
endDate=System.DateTime.Now.AddMonths(Number)
endDate=endDate.AddDays(daysAdded)
currentdate=endDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
EndDate_Nonmad=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("EndDate_Nonmad : " + str(EndDate_Nonmad))

Number=1
#================Last Day of Next Month Month Non Mad
date=System.DateTime.Now.Day
month=System.DateTime.Now.AddMonths(Number).Month
year=System.DateTime.Now.Year
daysInMonth=System.DateTime.DaysInMonth(year,month)
daysAdded=daysInMonth-date
endDate=System.DateTime.Now.AddMonths(Number)
endDate=endDate.AddDays(daysAdded)
currentdate=endDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
EndDateNext_Nonmad=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("EndDateNext_Nonmad : " + str(EndDateNext_Nonmad))

Number=2
#================First Day of Month Next Month Non Mad DateB
date=System.DateTime.Now.Day
month=System.DateTime.Now.Month
year=System.DateTime.Now.Year
daysInMonth=System.DateTime.DaysInMonth(year,month)
daysAdded=daysInMonth-date
endDate=System.DateTime.Now.AddDays(daysAdded+1).AddMonths(Number-1)
currentdate=endDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
StartDate_Nonmad_Dateb=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("StartDate_Nonmad_Dateb : " + str(StartDate_Nonmad_Dateb))

def GetEndOfWeekMAD(n): #n=1 to get the end of the current week ie Sunday
    TempTable = App.Sql.CreateTempTable("TempDate")
    TempTable.AddField("TODAY_DATE","datetimeoffset(7)").AddField("DAY_NAME","nvarchar(255)")
    TempTable.Execute()
    valuestoinsert = ['getdate()','DATENAME(dw, getdate())']
    TempTableInsert = App.Sql.Insert("#TempDate").Fields("TODAY_DATE", "DAY_NAME").Values(valuestoinsert).Execute()
    data = App.Sql.Read("#TempDate").Execute()
    datelist = []
    for d in data:
        currentday=str(d.GetValue("DAY_NAME"))
        currentdate=d.GetValue("TODAY_DATE")
        #App.Log(currentday)
        #App.Log(currentdate)
    if currentday=="Monday":
        offset=(7-n)
    elif currentday=="Tuesday":
        offset=(6-n)
    elif currentday=="Wednesday":
        offset=(5-n)
    elif currentday=="Thursday":
        offset=(4-n)
    elif currentday=="Friday":
        offset=(10-n)
    elif currentday=="Saturday":
        offset=(9-n)
    elif currentday=="Sunday":
        offset=(8-n)
    #App.Log(offset)
    reqddate=currentdate.AddDays(offset).Date
    #App.Log(str(reqddate))
    return reqddate


endOfcurrentWeek_MAD = GetEndOfWeekMAD(1) #getdate as the upcoming sunday
GetStartOfNextWeek = endOfcurrentWeek_MAD.AddDays(1) #getdate as the date after the above sunday ie monday
endOfNextWeek_MAD = endOfcurrentWeek_MAD.AddDays(7)

currentdate=GetStartOfNextWeek
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
StartDate_mad=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("StartDate_mad : " + str(StartDate_mad))

currentdate=endOfcurrentWeek_MAD
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
EndDate_mad=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("EndDate_mad : " + str(EndDate_mad))

currentdate=endOfNextWeek_MAD
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
EndDateNext_mad=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("EndDateNext_mad : " + str(EndDateNext_mad))

endOfNextWeek_MAD = endOfcurrentWeek_MAD.AddDays(21)
currentdate=endOfNextWeek_MAD
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
Check_Four_week=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("Check_Four_week : " + str(Check_Four_week))


#This function returns the end of month as per number specified, 0 specifies current month
Number=4
date=System.DateTime.Now.Day
month=System.DateTime.Now.AddMonths(Number).Month
year=System.DateTime.Now.Year
daysInMonth=System.DateTime.DaysInMonth(year,System.DateTime.Now.Month)
daysAdded=daysInMonth-date
if ((System.DateTime.Now.Date >= App.GetObjectById("CutOffDate","1").GetValue("CutOffDate").Date) and (System.DateTime.Now.Date <= App.GetObjectById("CutOffDate","1").GetValue("End_Date").Date)) or (System.DateTime.Now.Date < App.GetObjectById("CutOffDate","1").GetValue("Start_Date").Date):
    endDate=System.DateTime.Now.AddMonths(Number+1)
    year = endDate.Year
    daysInMonth=System.DateTime.DaysInMonth(year,endDate.Month)
    daysAdded=daysInMonth-endDate.Day
    endDate=endDate.AddDays(daysAdded)
else:
    endDate=System.DateTime.Now.AddMonths(Number)
    year = endDate.Year
    daysInMonth=System.DateTime.DaysInMonth(year,endDate.Month)
    daysAdded=daysInMonth-endDate.Day
    endDate=endDate.AddDays(daysAdded)
currentdate=endDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)

Check_Four_Month=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("Check_Four_Month : " + str(Check_Four_Month))

#This function returns the start of month as per number specified, 1 would specify next month
Number=1
date=System.DateTime.Now.Day
month=System.DateTime.Now.Month
year=System.DateTime.Now.Year
daysInMonth=System.DateTime.DaysInMonth(year,month)
daysAdded=daysInMonth-date
if ((System.DateTime.Now.Date > App.GetObjectById("CutOffDate","1").GetValue("CutOffDate").Date) and (System.DateTime.Now.Date <= App.GetObjectById("CutOffDate","1").GetValue("End_Date").Date)) or (System.DateTime.Now.Date < App.GetObjectById("CutOffDate","1").GetValue("Start_Date").Date): #Added start date condition
               effectiveDate=System.DateTime.Now.AddDays(daysAdded+1).AddMonths(Number+1)
else:
               effectiveDate=System.DateTime.Now.AddDays(daysAdded+1).AddMonths(Number)
currentdate=effectiveDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)

Check_Start_Month=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("Check_Start_Month : " + str(Check_Start_Month))

#---------------------------FUCNTION_ADDED_FOR_MAD--------------------------------------------------------------------------


VarDate = endOfcurrentWeek_MAD #getdate as the upcoming sunday
endDate = VarDate.AddDays(-7)
startDate = VarDate.AddDays(1) #getdate as the start of the upcoming week ie monday

currentdate=startDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
StartDate_mad_Dateb=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("StartDate_mad_Dateb : " + str(StartDate_mad_Dateb))

currentdate=endDate
Month=str(currentdate.Month).zfill(2)
Day=str(currentdate.Day).zfill(2)
Year=str(currentdate.Year).zfill(4)
endDate_mad_new=(Year+"-"+Month+"-"+Day+" 00:00:00 -05:00")
App.Log("endDate_mad_new: " + str(endDate_mad_new))

#------------------------------------------END----------------------------------------------------------------------------------

#================#================#================#================/Constant Module#================#================#================#================
#batch Count
Impersonation1 = App.Sql.CreateTempTable("#TempBatchCount")
Impersonation1.AddField("FileName","nvarchar(255)").AddField("Count","nvarchar(255)")
Impersonation1.Execute()


startDate=App.GetObjectById("CutOffDate","1").GetValue("Start_Date")
endDate=App.GetObjectById("CutOffDate","1").GetValue("End_Date")

App.Log("Start Date : "+str(startDate))
App.Log("End Date : "+str(endDate))

#get MAD/Non MAD Selector
TeamsCompare=App.Sql.Read("Position","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Id").And("ppc.Medical_Affair_Team_Flag","'1'"))
TeamsCompare.Select("Temp1.Position_Legacy_Id,Temp1.Id as PositionID,Temp1.Sales_Team")

TableCreate = App.Sql.CreateTempTable("Position_1")
TableCreate.AddField("Position_Legacy_Id","nvarchar(255)").AddField("PositionID","nvarchar(255)").AddField("Sales_Team","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#Position_1").Fields("Position_Legacy_Id,PositionID,Sales_Team").Values(TeamsCompare).Execute()

#================#================#================#================End dates alignments for deleted positions#================#================#=======

#get Deleted MAD/Non MAD Position

TeamsCompare=App.Sql.Read("Position","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Temp1.Action_Code","'3'"))
TeamsCompare.Select("Temp1.Position_Legacy_Id,Temp1.Id as PositionID,Temp1.Sales_Team")

TableCreate = App.Sql.CreateTempTable("Position_Del")
TableCreate.AddField("Position_Legacy_Id","nvarchar(255)").AddField("PositionID","nvarchar(255)").AddField("Sales_Team","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#Position_Del").Fields("Position_Legacy_Id,PositionID,Sales_Team").Values(TeamsCompare).Execute()

#get CDA To be Deleted

TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================#================#================#================CreateChangeHistoryAccount#================#================#================#================
endDate=App.GetObjectById("CutOffDate","1").GetValue("End_Date")
Start_Date = App.GetObjectById("CutOffDate","1").GetValue("End_Date").AddDays(1)

TeamsCompare=App.Sql.Read("#CallDeckAccount_Del","Temp1").Join("AccountAddressMap","ppc","Inner",App.Sql.CreateClause("Temp1.Acct_Customer","ppc.Acct_Customer").And("ppc.Is_Primary_Address","'1'"))
TeamsCompare.Select("'4' as CallDeck_Change_Type,Temp1.Position,left(Temp1.Position,9) as Position_Legacy_Id,Temp1.Alignment_Status as CallDeck_Status,'System' as Changed_By_User_Name,Temp1.Acct_Customer as Acct_Customer_Master_Id,ppc.Display_Address as Acct_Customer_Address,Temp1.Id,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccountChangeHistory_Create_1")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Acct_Customer_Master_Id","nvarchar(255)").AddField("Acct_Customer_Name","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Acct_Customer_Address","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccountChangeHistory_Create_1").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Acct_Customer_Master_Id,Acct_Customer_Address,Id,Deleted,Last_Updated_Date,Effective_Date").Values(TeamsCompare).Execute()

#Join to get details
TeamsCompare=App.Sql.Read("#CallDeckAccountChangeHistory_Create_1","Temp1").Join("Customer","ppc","Inner",App.Sql.CreateClause("Temp1.Acct_Customer_Master_Id","ppc.Acct"))
TeamsCompare.Select("Temp1.CallDeck_Change_Type,Temp1.Position,Temp1.Position_Legacy_Id,Temp1.CallDeck_Status,Temp1.Changed_By_User_Name,ppc.Customer_Master_Id as Acct_Customer_Master_Id,ppc.Customer_Name as Acct_Customer_Name,Temp1.Acct_Customer_Address,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date").Select("Temp1.Last_Updated_Date+Temp1.Id","Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccountChangeHistory_Create_2")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Acct_Customer_Master_Id","nvarchar(255)").AddField("Acct_Customer_Name","nvarchar(255)").AddField("Acct_Customer_Address","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccountChangeHistory_Create_2").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Acct_Customer_Master_Id,Acct_Customer_Name,Acct_Customer_Address,Deleted,Last_Updated,Effective_Date,Id").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccountChangeHistory_Create_2").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccountChangeHistory_Create_2","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccountChangeHistory_Create_2'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccountChangeHistory"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountChangeHistory_Create_2")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccountChangeHistory")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccountChangeHistory"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccountChangeHistory"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountChangeHistory_Create_2")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================#================#================#================/CreateChangeHistoryAccount#================#================#================#================

#get CDI To be Deleted

TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("#Position_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Indv_Customer,Temp1.Position,Temp1.Alignment_Status,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del")
TableCreate.AddField("Indv_Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del").Fields("Indv_Customer,Position,Alignment_Status,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================#================#================#================CreateChangeHistoryIndividual#================#================#================#================
endDate=App.GetObjectById("CutOffDate","1").GetValue("End_Date")
Start_Date = App.GetObjectById("CutOffDate","1").GetValue("End_Date").AddDays(1)

TeamsCompare=App.Sql.Read("#CallDeckIndividual_Del","Temp1").Join("IndividualAddressMap","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_Customer","ppc.Indv_Customer").And("ppc.Is_Primary_Address","'1'"))
TeamsCompare.Select("'5' as CallDeck_Change_Type,Temp1.Position,left(Temp1.Position,9) as Position_Legacy_Id,Temp1.Alignment_Status as CallDeck_Status,'System' as Changed_By_User_Name,Temp1.Indv_Customer as Indv_Customer_Master_Id,ppc.Display_Address as Indv_Customer_Address,Temp1.Id,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividualChangeHistory_Create_1")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Indv_Customer_Master_Id","nvarchar(255)").AddField("Indv_Customer_Name","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Indv_Customer_Address","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividualChangeHistory_Create_1").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Indv_Customer_Master_Id,Indv_Customer_Address,Id,Deleted,Last_Updated_Date,Effective_Date").Values(TeamsCompare).Execute()

#Join to get details
TeamsCompare=App.Sql.Read("#CallDeckIndividualChangeHistory_Create_1","Temp1").Join("Customer","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_Customer_Master_Id","ppc.Indv"))
TeamsCompare.Select("Temp1.CallDeck_Change_Type,Temp1.Position,Temp1.Position_Legacy_Id,Temp1.CallDeck_Status,Temp1.Changed_By_User_Name,ppc.Customer_Master_Id as Indv_Customer_Master_Id,ppc.Customer_Name as Indv_Customer_Name,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date").Select("Temp1.Last_Updated_Date+Temp1.Id","Id")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividualChangeHistory_Create_2")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Indv_Customer_Master_Id","nvarchar(255)").AddField("Indv_Customer_Name","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividualChangeHistory_Create_2").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Indv_Customer_Master_Id,Indv_Customer_Name,Deleted,Last_Updated,Effective_Date,Id").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividualChangeHistory_Create_2").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividualChangeHistory_Create_2","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividualChangeHistory_Create_2'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndvChangeHistory"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualChangeHistory_Create_2")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndvChangeHistory")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndvChangeHistory"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndvChangeHistory"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualChangeHistory_Create_2")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================#================#================#================/CreateChangeHistoryIndividual#================#================#================#


#================#================#================#================/End dates alignments for deleted positions#================#================#=======

#================#================#================#================End dates Veeva Request for deleted positions#================#================#=======

#get Veeva Request To be Deleted

TeamsCompare=App.Sql.Read("VeevaRequest","Temp1").Join("#Position_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Requestor_Position","ppc.PositionID").And("Temp1.Overall_Status","'4'").And("Temp1.Request","''","NotEquals"))
TeamsCompare.Select("Veeva_Request_Id,Temp1.Request_Type,Temp1.Alignment_Status,Temp1.Customer_Id,Temp1.Requestor_Personnel_Id,Temp1.Customer,Temp1.Requestor_Personnel,Temp1.Requestor_Position,Temp1.Requestor_Comments,'3' as Overall_Status,'1' as Rejection_Reason,Temp1.Approver_Personnel,Temp1.Approver_Comments,Temp1.Request,'1' as Is_Processed,'0' as Send_Out,Temp1.Processed_Date,Temp1.Requestor_Personnel_Name,Temp1.Requestor_Position_LegacyId,'1' as Medical_Affair_Send_Out,Temp1.Sales_Team_Name,Temp1.SubCat,Temp1.Specialty,Temp1.Account_COT,Temp1.Individual_COT,Id,'N' as Deleted")

TableCreate = App.Sql.CreateTempTable("VeevaRequest_Del")
TableCreate.AddField("Veeva_Request_Id","nvarchar(255)").AddField("Request_Type","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Customer_Id","nvarchar(255)").AddField("Requestor_Personnel_Id","nvarchar(255)").AddField("Customer","nvarchar(255)").AddField("Requestor_Personnel","nvarchar(255)").AddField("Requestor_Position","nvarchar(255)").AddField("Requestor_Comments","nvarchar(255)").AddField("Overall_Status","nvarchar(255)").AddField("Rejection_Reason","nvarchar(255)").AddField("Approver_Personnel","nvarchar(255)").AddField("Approver_Comments","nvarchar(255)").AddField("Request","nvarchar(255)").AddField("Is_Processed","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Processed_Date","nvarchar(255)").AddField("Requestor_Personnel_Name","nvarchar(255)").AddField("Requestor_Position_LegacyId","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Sales_Team_Name","nvarchar(255)").AddField("SubCat","nvarchar(255)").AddField("Specialty","nvarchar(255)").AddField("Account_COT","nvarchar(255)").AddField("Individual_COT","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#VeevaRequest_Del").Fields("Veeva_Request_Id,Request_Type,Alignment_Status,Customer_Id,Requestor_Personnel_Id,Customer,Requestor_Personnel,Requestor_Position,Requestor_Comments,Overall_Status,Rejection_Reason,Approver_Personnel,Approver_Comments,Request,Is_Processed,Send_Out,Processed_Date,Requestor_Personnel_Name,Requestor_Position_LegacyId,Medical_Affair_Send_Out,Sales_Team_Name,SubCat,Specialty,Account_COT,Individual_COT,Id,Deleted").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#VeevaRequest_Del").Distinct()
CountPersnl=App.Sql.Read("#VeevaRequest_Del","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'VeevaRequest_Del'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "VeevaRequest"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/VeevaRequest_Del")
exportConfig.SetQueryAndFileName(PersonnelFile,"VeevaRequest")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"VeevaRequest"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["VeevaRequest"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/VeevaRequest_Del")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#Delete Related request

# App.Sql.Delete("AccountAlignmentRequest","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("IndividualAlignmentRequest","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("RemoveAcntAlignmentRequest","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("RemoveIndvAlignmentRequest","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("RequestCustomer","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("RequestHistory","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("RequestPosition","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("CallDeckTransferShareRequest","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Request","ppc.Request")).Execute()
# App.Sql.Delete("Request","Temp1").Join("#VeevaRequest_Del","ppc","Inner",App.Sql.CreateClause("Temp1.Id","ppc.Request")).Execute()

#================#================#================#================/End dates Veeva Request for deleted positions#================#================#=======
#================#================This part creates personnelPosition record for positions that don't have one#================#================#=======

#insert update position set
TeamsCompare=App.Sql.Read("Position","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'1'"))
TeamsCompare.Select("Temp1.Position_Legacy_Id,Temp1.Id as PositionID,Temp1.Sales_Team")

TableCreate = App.Sql.CreateTempTable("Position_2")
TableCreate.AddField("Position_Legacy_Id","nvarchar(255)").AddField("PositionID","nvarchar(255)").AddField("Sales_Team","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#Position_2").Fields("Position_Legacy_Id,PositionID,Sales_Team").Values(TeamsCompare).Execute()

TeamsCompare=App.Sql.Read("Position","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'2'"))
TeamsCompare.Select("Temp1.Position_Legacy_Id,Temp1.Id as PositionID,Temp1.Sales_Team")

App.Sql.Insert("#Position_2").Fields("Position_Legacy_Id,PositionID,Sales_Team").Values(TeamsCompare).Execute()

#left join with PersonnelPositionMap

TeamsCompare=App.Sql.Read("#Position_2","Temp1").Join("PersonnelPositionMap","ppc","LeftOuter",App.Sql.CreateClause("Temp1.PositionID","ppc.Position"))
TeamsCompare.Select("Temp1.Position_Legacy_Id,Temp1.PositionID,Temp1.Sales_Team,ppc.Is_Vacant")

TableCreate = App.Sql.CreateTempTable("Position_3")
TableCreate.AddField("Position_Legacy_Id","nvarchar(255)").AddField("PositionID","nvarchar(255)").AddField("Sales_Team","nvarchar(255)").AddField("Is_Vacant","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#Position_3").Fields("Position_Legacy_Id,PositionID,Sales_Team,Is_Vacant").Values(TeamsCompare).Execute()

#Insert PPM final set

TeamsCompare=App.Sql.Read("#Position_3","Temp1").Where(App.Sql.CreateClause("Temp1.Is_Vacant","","IsNull"))
TeamsCompare.Select("Temp1.PositionID,'1' as Is_Vacant,Temp1.PositionID as Id,'N' as Deleted")

TableCreate = App.Sql.CreateTempTable("PersonnelPositionMap_New")
TableCreate.AddField("Position","nvarchar(255)").AddField("Is_Vacant","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#PersonnelPositionMap_New").Fields("Position,Is_Vacant,Id,Deleted").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#PersonnelPositionMap_New").Distinct()
CountPersnl=App.Sql.Read("#PersonnelPositionMap_New","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'PersonnelPositionMap_New'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "PersonnelPositionMap"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/PersonnelPositionMap_New")
exportConfig.SetQueryAndFileName(PersonnelFile,"PersonnelPositionMap")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"PersonnelPositionMap"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["PersonnelPositionMap"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/PersonnelPositionMap_New")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================#======This part changes the approver of positions for positions there is a change in parent position#================#================#=======
#############################################################################################################################################################################
SalesTeamRecords=App.Query("SalesTeam").Fields("Id").Where("Medical_Affair_Team_Flag","1")
PositionRecords=App.Query("Position").Fields("Id").Where("Sales_Team.Id",SalesTeamRecords,"In")
def GetParentPositonList(childList):
    #This function returns list of valid approver parents for every child in childLIst
    #Get parent for all positions for which a pending request is there
    vacantPersonnelPositionList=App.Query("PersonnelPositionMap").Where("Personnel","","IsNull").List()             #get a list of vacant positions
    TotalVacantPositions=[personnelPosition.GetValue("Position") for personnelPosition in vacantPersonnelPositionList]
    hierarchyList=[]
    temp=App.Query("Hierarchy")
    count1=0
    totalCount=0
    positionCount=len(childList)
    for position in childList:
        temp=temp.Or("Action_Code","3","NotEquals").And("Child_Position.Id",position.Id()).And("Parent_Position.Level.LevelValue","3","GreaterThan")
        count1+=1
        totalCount+=1
        if count1==150 or totalCount==positionCount:
            temp=temp.List()
            count1=0
            hierarchyList=hierarchyList+list(temp)
            temp=App.Query("Hierarchy")

    ChildParentLevelList=[[hierarchy.GetValue("Child_Position"),[hierarchy.GetValue("Parent_Position")],hierarchy.GetValue("Parent_Position.Level.LevelValue")] for hierarchy in hierarchyList if hierarchy.GetValue("Parent_Position") not in TotalVacantPositions]

    levelSortedChildParentLevelList=sorted(ChildParentLevelList,key=lambda x:x[2],reverse=True)
    childSortedChildParentLevelList=sorted(levelSortedChildParentLevelList,key=lambda x:x[0])

    App.Log("Total Child Parent pairs :"+str(len(childSortedChildParentLevelList)))

    FinalValidChildParent=[]
    childposition=[]
    currentlevel=0
    for childParent in childSortedChildParentLevelList:
        if(childParent[0] not in childposition ):
            childposition.append(childParent[0])
            FinalValidChildParent.append(childParent)
            currentlevel=childParent[2]
        elif currentlevel==childParent[2]:
            FinalValidChildParent[-1][1].extend(childParent[1])

    PositionsapprovedbyHQ=list(set(childList) - set(childposition))                #Get list of child position for which there is no parent till DCO level
    #Complete Get parent for all positions for which a pending request is there

    #Get parent for all positions for which a pending request is there but there is no parent until DCO level
    hierarchyList1=[]
    temp=App.Query("Hierarchy")
    count1=0
    totalCount=0
    positionCount=len(PositionsapprovedbyHQ)
    for position in PositionsapprovedbyHQ:
        temp=temp.Or("Action_Code","3","NotEquals").And("Child_Position.Id",position.Id()).And("Parent_Position.Level.LevelValue","1")
        count1+=1
        totalCount+=1
        if count1==150 or totalCount==positionCount:
            temp=temp.List()
            count1=0
            hierarchyList1=hierarchyList1+list(temp)
            temp=App.Query("Hierarchy")
    ChildHQLevelList=[[hierarchy.GetValue("Child_Position"),[hierarchy.GetValue("Parent_Position")],hierarchy.GetValue("Parent_Position.Level.LevelValue")] for hierarchy in hierarchyList1 if hierarchy.GetValue("Parent_Position") not in TotalVacantPositions]

    levelSortedChildHQLevelList=sorted(ChildHQLevelList,key=lambda x:x[2],reverse=True)
    childSortedChildHQLevelList=sorted(levelSortedChildHQLevelList,key=lambda x:x[0])

    App.Log("Total Child Parent pairs :"+str(len(childSortedChildHQLevelList)))

    FinalValidChildHQ=[]
    childposition1=[]
    currentlevel=0
    for childHQ in childSortedChildHQLevelList:
        if(childHQ[0] not in childposition ):
            childposition.append(childHQ[0])
            FinalValidChildHQ.append(childHQ)
            currentlevel=childHQ[2]
        elif currentlevel==childHQ[2]:
            FinalValidChildHQ[-1][1].extend(childHQ[1])
    #Get parent for all positions for which a pending request is there but there is no parent until DCO level

    FinalChildParentPositionPairList=FinalValidChildParent + FinalValidChildHQ
    return FinalChildParentPositionPairList             #This is a list of this type[[c1,[p1,p2],4],[c2,[p3,p4],5]


#This part changes the approver of positions for positions there is a change in parent position
#Get all hierarchy objects that have been updated
insertDeleteHierarchyList=App.Query("Hierarchy").Where("Child_Position.Id",PositionRecords,"In").Where("Action_Code","3").Or("Action_Code","1").List()
App.Log("to be searched reults :"+str(len(insertDeleteHierarchyList)))

vacantPersonnelPositionList=App.Query("PersonnelPositionMap").Where("Position.Id",PositionRecords,"In").Where("Personnel","","IsNull").List()             #get a list of vacant positions

TotalVacantPositions=[personnelPosition.GetValue("Position") for personnelPosition in vacantPersonnelPositionList]

#Get all request which were raise on a position for which a parent is inserted or deleted
AccountAlignmentRequests=[]
IndividualAlignmentRequests=[]
RemoveAccountAlignmentRequests=[]
RemoveIndividualAlignmentRequests=[]

tempAccountAlignmentRequests=App.Query("AccountAlignmentRequest")
tempIndividualAlignmentRequests=App.Query("IndividualAlignmentRequest")
tempRemoveAccountAlignmentRequests=App.Query("RemoveAcntAlignmentRequest")
tempRemoveIndividualAlignmentRequests=App.Query("RemoveIndvAlignmentRequest")
count2=0
totalcount=0
totalPosition=len(insertDeleteHierarchyList)
for childParent in insertDeleteHierarchyList:
    tempAccountAlignmentRequests=tempAccountAlignmentRequests.Or("Request_Position.Position.Id",childParent.GetValue("Child_Position").Id()).And("Request.Overall_Request_Status","1")
    tempIndividualAlignmentRequests=tempIndividualAlignmentRequests.Or("Request_Position.Position.Id",childParent.GetValue("Child_Position").Id()).And("Request.Overall_Request_Status","1")
    tempRemoveAccountAlignmentRequests=tempRemoveAccountAlignmentRequests.Or("Request_Position.Position.Id",childParent.GetValue("Child_Position").Id()).And("Request.Overall_Request_Status","1")
    tempRemoveIndividualAlignmentRequests=tempRemoveIndividualAlignmentRequests.Or("Request_Position.Position.Id",childParent.GetValue("Child_Position").Id()).And("Request.Overall_Request_Status","1")
    count2=count2+1
    totalcount=totalcount + 1
    if count2==250 or totalcount==totalPosition:
        tempAccountAlignmentRequests=tempAccountAlignmentRequests.List()
        tempIndividualAlignmentRequests=tempIndividualAlignmentRequests.List()
        tempRemoveAccountAlignmentRequests=tempRemoveAccountAlignmentRequests.List()
        tempRemoveIndividualAlignmentRequests=tempRemoveIndividualAlignmentRequests.List()
        AccountAlignmentRequests=AccountAlignmentRequests+list(tempAccountAlignmentRequests)
        IndividualAlignmentRequests=IndividualAlignmentRequests+list(tempIndividualAlignmentRequests)
        RemoveAccountAlignmentRequests=RemoveAccountAlignmentRequests+list(tempRemoveAccountAlignmentRequests)
        RemoveIndividualAlignmentRequests=RemoveIndividualAlignmentRequests+list(tempRemoveIndividualAlignmentRequests)
        count2=0
        tempAccountAlignmentRequests=App.Query("AccountAlignmentRequest")
        tempIndividualAlignmentRequests=App.Query("IndividualAlignmentRequest")
        tempRemoveAccountAlignmentRequests=App.Query("RemoveAcntAlignmentRequest")
        tempRemoveIndividualAlignmentRequests=App.Query("RemoveIndvAlignmentRequest")

TotalRequests=list(AccountAlignmentRequests)+ list(IndividualAlignmentRequests)+ list(RemoveAccountAlignmentRequests)+list(RemoveIndividualAlignmentRequests)
App.Log("Requests found for approver change"+str(len(TotalRequests)))
#Complete Get all request which were raise on a position for which a parent is inserted or deleted

RequestChildPair=[(request.GetValue("Request"),request.GetValue("Request_Position.Position")) for request in TotalRequests ]
TotalRequestsFinal=[request.GetValue("Request") for request in TotalRequests ]
totalChildPositionList=[request.GetValue("Request_Position.Position") for request in TotalRequests ]

#Delete request approvals for existing request approvals
totalcount=0
count=0
totalrequest=len(TotalRequestsFinal)
FinalRequestApprovals=[]
temp=App.Query("RequestApproval")
for request in TotalRequestsFinal:
    temp=temp.Or("Request.Id",request.Id())
    count=count+1
    totalcount=totalcount+1
    if(count==400 or totalcount==totalrequest):
        temp=temp.List()
        FinalRequestApprovals=FinalRequestApprovals+list(temp)
        temp=App.Query("RequestApproval")
        count=0

App.Log("Number of request approvals deleted : "+str(len(FinalRequestApprovals)))
App.BulkDelete(FinalRequestApprovals)
App.Log("Request Approvals deleted" + str(len(FinalRequestApprovals)))


#Complete Delete request approvals for existing request approvals

FinalChildParentPositionPairList=GetParentPositonList(totalChildPositionList)
finalChildList=[childParentLevel[0] for childParentLevel in FinalChildParentPositionPairList]

#Create request approval records according to new parents
requestApprovalList=[]
for requestChild in RequestChildPair:
    child=requestChild[1]
    request=requestChild[0]
    if  child in finalChildList:
        parentList=FinalChildParentPositionPairList[finalChildList.index(child)][1]
        for parent in parentList:
            reqapprobject=App.CreateObject("RequestApproval")
            reqapprobject.SetValue("Request",request)
            reqapprobject.SetValue("Position",parent)
            reqapprobject.SetValue("Approval_Round","1")
            reqapprobject.SetValue("Request_Approval_Round_Status","0")
            requestApprovalList.append(reqapprobject)
App.BulkSave(requestApprovalList)
App.Log("New request approval created"+ str(len(requestApprovalList)))
#Complete This part changes the approver of positions for positions there is a change in parent position
###############################################################################################################################################################################
#================#======/This part changes the approver of positions for positions there is a change in parent position#================#================
#=====================This part is run monthly and updates those alignments which were end date in JCDM but whose end dates were removed in JAMS=========
#Join to get Filtered Position CDI
TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.End_Date,19)+' -05:00'","","IsNotNull").And("Temp1.Action_Code","'0'").And("Temp1.Alignment_Status","'2'"))
TeamsCompare.Select("Temp1.Indv_Customer,Temp1.Position,Temp1.Alignment_Status,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("''","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Update_End_Date")
TableCreate.AddField("Indv_Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Update_End_Date").Fields("Indv_Customer,Position,Alignment_Status,Effective_Date,Last_Updated_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Update_End_Date").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Update_End_Date","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Update_End_Date'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Update_End_Date")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Update_End_Date")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#Join to get Filtered Position CDA
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.End_Date,19)+' -05:00'","","IsNotNull").And("Temp1.Action_Code","'0'").And("Temp1.Alignment_Status","'2'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("''","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Update_End_Date")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Update_End_Date").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Last_Updated_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Update_End_Date").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Update_End_Date","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Update_End_Date'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Update_End_Date")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Update_End_Date")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#=====================/This part is run monthly and updates those alignments which were end date in JCDM but whose end dates were removed in JAMS=========

#=====================This part end dates all temporary alignments corresponding to which a permanent alignment has been received from JAMS#==============

App.Log("Started the part where we end date all temporary alignments corresponding to which a permanent alignment has been received from JAMS :"+str(System.DateTime.Now))
DateA=App.GetObjectById("CutOffDate","1").GetValue("End_Date").AddDays(1)
DateB=DateA.AddMonths(1)
App.Log("DateA"+str(DateA))
App.Log("DateB"+StartDate_mad_Dateb)

#=====================CallDeckAccount Part
#get Temporary Valid Assignments
#shreya Code Change Recommendation
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Id")

# TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
# TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Temporary_Active")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Temporary_Active").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Action_Code,Id").Values(TeamsCompare).Execute()

#get Permananet Valid Assignments Implicit
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+StartDate_mad_Dateb+"'","LessThanOrEqualTo").And("Temp1.Alignment_Status","'2'").And("Temp1.Action_Code","'1'").And("Temp1.Is_Explicit","'0'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Permanent_JAMS_Implicit")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Permanent_JAMS_Implicit").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Action_Code,Id").Values(TeamsCompare).Execute()

#get Permananet Valid Assignments Explicit
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+StartDate_mad_Dateb+"'","LessThanOrEqualTo").And("Temp1.Alignment_Status","'2'").And("Temp1.Action_Code","'1'").And("Temp1.Is_Explicit","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Permanent_JAMS_Explicit")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Permanent_JAMS_Explicit").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Action_Code,Id").Values(TeamsCompare).Execute()

#get Permananet Explicit
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+StartDate_mad_Dateb+"'","LessThanOrEqualTo").And("Temp1.Alignment_Status","'2'").And("Temp1.Is_Explicit","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Permanent_Full_Explicit")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Permanent_Full_Explicit").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Action_Code,Id").Values(TeamsCompare).Execute()

# #get Permananet Implicit
# TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+StartDate_mad_Dateb+"'","LessThanOrEqualTo").And("Temp1.Alignment_Status","'2'").And("Temp1.Is_Explicit","'0'"))
# TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Id")

# TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Permanent_Full_Implicit")
# TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Id","nvarchar(255)")
# TableCreate.Execute()

# App.Sql.Insert("#CallDeckAccount_Permanent_Full_Implicit").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Id").Values(TeamsCompare).Execute()

#get temporary Permananet Valid Assignments Explicit
TeamsCompare=App.Sql.Read("#CallDeckAccount_Temporary_Active","Temp1").Join("#CallDeckAccount_Permanent_JAMS_Explicit","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_TempPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_TempPerm").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_TempPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_TempPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_TempPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_TempPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_TempPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#get temporary Permanent Valid Assignments Implicit
TeamsCompare=App.Sql.Read("#CallDeckAccount_Permanent_JAMS_Implicit","Temp1").Join("#CallDeckAccount_Temporary_Active","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00','1' as Is_Explicit,'4' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_imp_TempPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_imp_TempPerm").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_imp_TempPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_imp_TempPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_imp_TempPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_imp_TempPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_imp_TempPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#####################################################NEW CODE BLOCK##################################################################
#get temporary Permananet Valid Assignments Implicit - Create New explicit insert
TeamsCompare=App.Sql.Read("#CallDeckAccount_Del_imp_TempPerm","Temp1")
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,'1' as Is_Explicit,'1' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+StartDate_mad+"'","Effective_Date").Select("Temp1.Id+'PermanentExplicitCreate'","Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Add_imp_TempPerm_Explcit")
TableCreate.AddField("Acct_Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Add_imp_TempPerm_Explcit").Fields("Position,Alignment_Status,Acct_Customer,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Deleted,Last_Updated_Date,Effective_Date,Id").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Add_imp_TempPerm_Explcit").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Add_imp_TempPerm_Explcit","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Add_imp_TempPerm_Explcit'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Add_imp_TempPerm_Explcit")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Add_imp_TempPerm_Explcit")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


# #================Delete Action code 4 records================
# App.Sql.Delete("CallDeckAccount","Temp1").Join("#CallDeckAccount_Del_imp_TempPerm","ppc","Inner",App.Sql.CreateClause("Temp1.Id","ppc.Id").And("ppc.Action_Code","'4'").And("ppc.Is_Explicit","'1'").And("Temp1.Is_Explicit","'0'")).Execute()
# #================/Delete Action code 4 records================

#Add Record in CallDeckAccountRemoveQueue1
TeamsCompare=App.Sql.Read("#CallDeckAccount_Del_imp_TempPerm","Temp1")
TeamsCompare.Select("Temp1.Acct_Customer,Temp1.Position,Temp1.Id,'N' as Deleted").Select("'"+Check_Four_week+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccountRemoveQueue1_1")
TableCreate.AddField("End_Date","nvarchar(255)").AddField("Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccountRemoveQueue1_1").Fields("Customer,Position,Id,Deleted,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccountRemoveQueue1_1").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccountRemoveQueue1_1","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccountRemoveQueue1_1'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccountRemoveQueue1"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountRemoveQueue1_1")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccountRemoveQueue1")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccountRemoveQueue1"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccountRemoveQueue1"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountRemoveQueue1_1")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================Explicit from JAMS Explicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckAccount_Permanent_Full_Explicit","Temp1").Join("#CallDeckAccount_Permanent_JAMS_Explicit","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer").And("Temp1.Alignment_Status","'2'").And("Temp1.Action_Code","'0'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_Exp_ExplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_Exp_ExplicitPerm").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_Exp_ExplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_Exp_ExplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_Exp_ExplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_Exp_ExplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_Exp_ExplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================Explicit from JAMS Implicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckAccount_Permanent_JAMS_Explicit","Temp1").Join("CallDeckAccount","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer").And("ppc.Alignment_Status","'2'").And("ppc.Is_Explicit","'0'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("ppc.Position,ppc.Alignment_Status,ppc.Acct_Customer,ppc.Effective_Date,ppc.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,ppc.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_imp_ExplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_imp_ExplicitPerm").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_imp_ExplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_imp_ExplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_imp_ExplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_imp_ExplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_imp_ExplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================Implicit from JAMS Implicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckAccount_Permanent_JAMS_Implicit","Temp1").Join("CallDeckAccount","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer").And("ppc.Alignment_Status","'2'").And("ppc.Is_Explicit","'0'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("ppc.Position,ppc.Alignment_Status,ppc.Acct_Customer,ppc.Effective_Date,ppc.End_Date,ppc.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,ppc.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_imp_ImplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_imp_ImplicitPerm").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_imp_ImplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_imp_ImplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_imp_ImplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_imp_ImplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_imp_ImplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================Implicit from JAMS Explicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckAccount_Permanent_JAMS_Implicit","Temp1").Join("CallDeckAccount","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer").And("ppc.Alignment_Status","'2'").And("ppc.Is_Explicit","'1'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_exp_ImplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_exp_ImplicitPerm").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_exp_ImplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_exp_ImplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_exp_ImplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_exp_ImplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_exp_ImplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#####################################################NEW CODE BLOCK##################################################################
App.Log("Create object of CallDeckAccountRemoveQueue")

#get temporary Permananet Valid Assignments Implicit
TeamsCompare=App.Sql.Read("#CallDeckAccount_Permanent_JAMS_Implicit","Temp1").Join("#CallDeckAccount_Temporary_Active","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Acct_Customer","ppc.Acct_Customer"))
TeamsCompare.Select("Temp1.Id as CallDeckAccount,ppc.End_Date as End_Date,Temp1.Id,'N' as Deleted")

TableCreate = App.Sql.CreateTempTable("CallDeckAccountRemoveQueue_1")
TableCreate.AddField("CallDeckAccount","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccountRemoveQueue_1").Fields("CallDeckAccount,End_Date,Id,Deleted").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccountRemoveQueue_1").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccountRemoveQueue_1","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccountRemoveQueue_1'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccountRemoveQueue"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountRemoveQueue_1")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccountRemoveQueue")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccountRemoveQueue"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccountRemoveQueue"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountRemoveQueue_1")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================#================#================#================CreateChangeHistoryAccount#================#================#================#================
endDate=App.GetObjectById("CutOffDate","1").GetValue("End_Date")
Start_Date = App.GetObjectById("CutOffDate","1").GetValue("End_Date").AddDays(1)

TeamsCompare=App.Sql.Read("#CallDeckAccount_Del_TempPerm","Temp1").Join("AccountAddressMap","ppc","Inner",App.Sql.CreateClause("Temp1.Acct_Customer","ppc.Acct_Customer").And("ppc.Is_Primary_Address","'1'"))
TeamsCompare.Select("'4' as CallDeck_Change_Type,Temp1.Position,left(Temp1.Position,9) as Position_Legacy_Id,Temp1.Alignment_Status as CallDeck_Status,'System' as Changed_By_User_Name,Temp1.Acct_Customer as Acct_Customer_Master_Id,ppc.Display_Address as Acct_Customer_Address,Temp1.Id,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccountChangeHistory_Create_3")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Acct_Customer_Master_Id","nvarchar(255)").AddField("Acct_Customer_Name","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Acct_Customer_Address","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccountChangeHistory_Create_3").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Acct_Customer_Master_Id,Acct_Customer_Address,Id,Deleted,Last_Updated_Date,Effective_Date").Values(TeamsCompare).Execute()

#Join to get details
TeamsCompare=App.Sql.Read("#CallDeckAccountChangeHistory_Create_3","Temp1").Join("Customer","ppc","Inner",App.Sql.CreateClause("Temp1.Acct_Customer_Master_Id","ppc.Acct"))
TeamsCompare.Select("Temp1.CallDeck_Change_Type,Temp1.Position,Temp1.Position_Legacy_Id,Temp1.CallDeck_Status,Temp1.Changed_By_User_Name,ppc.Customer_Master_Id as Acct_Customer_Master_Id,ppc.Customer_Name as Acct_Customer_Name,Temp1.Acct_Customer_Address,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date").Select("left(Temp1.Last_Updated_Date,19)+' -05:00'+Temp1.Id","Id")

TableCreate = App.Sql.CreateTempTable("CallDeckAccountChangeHistory_Create_4")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Acct_Customer_Master_Id","nvarchar(255)").AddField("Acct_Customer_Name","nvarchar(255)").AddField("Acct_Customer_Address","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccountChangeHistory_Create_4").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Acct_Customer_Master_Id,Acct_Customer_Name,Acct_Customer_Address,Deleted,Last_Updated,Effective_Date,Id").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccountChangeHistory_Create_4").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccountChangeHistory_Create_4","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccountChangeHistory_Create_4'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccountChangeHistory"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountChangeHistory_Create_4")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccountChangeHistory")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccountChangeHistory"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccountChangeHistory"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccountChangeHistory_Create_4")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================#================#================#================/CreateChangeHistoryAccount#================#================#================#================


#=====================CallDeckIndividual Part
#get Temporary Valid Assignments
TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Temporary_Active")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Temporary_Active").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Id").Values(TeamsCompare).Execute()

#get Permananet Valid Assignments Implicit
TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+StartDate_mad_Dateb+"'","LessThanOrEqualTo").And("Temp1.Alignment_Status","'2'").And("Temp1.Action_Code","'1'").And("Temp1.Is_Explicit","'0'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Permanent_JAMS_Implicit")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Permanent_JAMS_Implicit").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Id").Values(TeamsCompare).Execute()

#get Permananet Valid Assignments Explicit
TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+StartDate_mad_Dateb+"'","LessThanOrEqualTo").And("Temp1.Alignment_Status","'2'").And("Temp1.Action_Code","'1'").And("Temp1.Is_Explicit","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',left(Temp1.Last_Updated_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Id")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Permanent_JAMS_Explicit")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Id","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Permanent_JAMS_Explicit").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Last_Updated_Date,Is_Explicit,Id").Values(TeamsCompare).Execute()


#get temporary Permananet Valid Assignments Explicit
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Temporary_Active","Temp1").Join("#CallDeckIndividual_Permanent_JAMS_Explicit","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_TempPerm")
TableCreate.AddField("Indv_Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_TempPerm").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_TempPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_TempPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_TempPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_TempPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_TempPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#get temporary Permananet Valid Assignments Implicit
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Permanent_JAMS_Implicit","Temp1").Join("#CallDeckIndividual_Temporary_Active","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00','1' as Is_Explicit,'4' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_imp_TempPerm")
TableCreate.AddField("Indv_Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_imp_TempPerm").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_imp_TempPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_imp_TempPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_imp_TempPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_imp_TempPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))
# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_imp_TempPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#####################################################NEW CODE BLOCK##################################################################
#get temporary Permananet Valid Assignments Implicit - Create New explicit insert
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Del_imp_TempPerm","Temp1")
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,'1' as Is_Explicit,'1' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+StartDate_mad+"'","Effective_Date").Select("Temp1.Id+'PermanentExplicitCreate'","Id")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Add_imp_TempPerm_Explcit")
TableCreate.AddField("Indv_Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Add_imp_TempPerm_Explcit").Fields("Position,Alignment_Status,Indv_Customer,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Deleted,Last_Updated_Date,Effective_Date,Id").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Add_imp_TempPerm_Explcit").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Add_imp_TempPerm_Explcit","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Add_imp_TempPerm_Explcit'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Add_imp_TempPerm_Explcit")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Add_imp_TempPerm_Explcit")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#================Delete Action code 4 records================
# App.Sql.Delete("CallDeckIndividual","Temp1").Join("#CallDeckIndividual_Del_imp_TempPerm","ppc","Inner",App.Sql.CreateClause("Temp1.Id","ppc.Id").And("ppc.Action_Code","'4'").And("ppc.Is_Explicit","'1'").And("Temp1.Is_Explicit","'0'")).Execute()
#================/Delete Action code 4 records================

#Add Record in CallDeckIndividualRemoveQueue1
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Del_imp_TempPerm","Temp1")
TeamsCompare.Select("Temp1.Indv_Customer,Temp1.Position,Temp1.Id,'N' as Deleted").Select("'"+Check_Four_week+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividualRemoveQueue1_1")
TableCreate.AddField("End_Date","nvarchar(255)").AddField("Customer","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividualRemoveQueue1_1").Fields("Customer,Position,Id,Deleted,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividualRemoveQueue1_1").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividualRemoveQueue1_1","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividualRemoveQueue1_1'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividualRemoveQueue1"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualRemoveQueue1_1")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividualRemoveQueue1")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividualRemoveQueue1"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividualRemoveQueue1"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualRemoveQueue1_1")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================Explicit from JAMS Implicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Permanent_JAMS_Explicit","Temp1").Join("CallDeckIndividual","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer").And("ppc.Alignment_Status","'2'").And("ppc.Is_Explicit","'0'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("ppc.Position,ppc.Alignment_Status,ppc.Indv_Customer,ppc.Effective_Date,ppc.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,ppc.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_imp_ExplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_imp_ExplicitPerm").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_imp_ExplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_imp_ExplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_imp_ExplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_imp_ExplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_imp_ExplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")

#================Explicit from JAMS Explicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Permanent_JAMS_Explicit","Temp1").Join("CallDeckIndividual","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer").And("ppc.Alignment_Status","'2'").And("ppc.Is_Explicit","'1'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("ppc.Position,ppc.Alignment_Status,ppc.Indv_Customer,ppc.Effective_Date,ppc.End_Date,ppc.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,ppc.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_Exp_ExplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_Exp_ExplicitPerm").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_Exp_ExplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_Exp_ExplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_Exp_ExplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_Exp_ExplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_Exp_ExplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================Implicit from JAMS Implicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Permanent_JAMS_Implicit","Temp1").Join("CallDeckIndividual","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer").And("Temp1.Alignment_Status","'2'").And("ppc.Is_Explicit","'0'").And("Temp1.Is_Explicit","'0'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("ppc.Position,ppc.Alignment_Status,ppc.Indv_Customer,ppc.Effective_Date,ppc.End_Date,ppc.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,ppc.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_imp_ImplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_imp_ImplicitPerm").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_imp_ImplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_imp_ImplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_imp_ImplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_imp_ImplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_imp_ImplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================Implicit from JAMS Explicit in JCDM================
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Permanent_JAMS_Implicit","Temp1").Join("CallDeckIndividual","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer").And("ppc.Alignment_Status","'2'").And("ppc.Is_Explicit","'1'").And("ppc.Action_Code","'0'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,'' as Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'Y' as Deleted").Select("'"+Check+"'","Last_Updated_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_exp_ImplicitPerm")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_exp_ImplicitPerm").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,End_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_exp_ImplicitPerm").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_exp_ImplicitPerm","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_exp_ImplicitPerm'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_exp_ImplicitPerm")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_exp_ImplicitPerm")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#####################################################/NEW CODE BLOCK##################################################################
App.Log("Create object of CallDeckIndividualRemoveQueue")

#get temporary Permananet Valid Assignments Implicit
TeamsCompare=App.Sql.Read("#CallDeckIndividual_Permanent_JAMS_Implicit","Temp1").Join("#CallDeckIndividual_Temporary_Active","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Customer","ppc.Indv_Customer"))
TeamsCompare.Select("Temp1.Id as CallDeckIndividual,ppc.End_Date as End_Date,Temp1.Id,'N' as Deleted")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividualRemoveQueue_1")
TableCreate.AddField("CallDeckIndividual","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividualRemoveQueue_1").Fields("CallDeckIndividual,End_Date,Id,Deleted").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividualRemoveQueue_1").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividualRemoveQueue_1","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividualRemoveQueue_1'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividualRemoveQueue"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualRemoveQueue_1")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividualRemoveQueue")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividualRemoveQueue"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividualRemoveQueue"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualRemoveQueue_1")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================#================#================#================CreateChangeHistoryIndividual#================#================#================#==
endDate=App.GetObjectById("CutOffDate","1").GetValue("End_Date")
Start_Date = App.GetObjectById("CutOffDate","1").GetValue("End_Date").AddDays(1)

TeamsCompare=App.Sql.Read("#CallDeckIndividual_Del_TempPerm","Temp1").Join("IndividualAddressMap","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_Customer","ppc.Indv_Customer").And("ppc.Is_Primary_Address","'1'"))
TeamsCompare.Select("'5' as CallDeck_Change_Type,Temp1.Position,left(Temp1.Position,9) as Position_Legacy_Id,Temp1.Alignment_Status as CallDeck_Status,'System' as Changed_By_User_Name,Temp1.Indv_Customer as Indv_Customer_Master_Id,ppc.Display_Address as Indv_Customer_Address,Temp1.Id,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividualChangeHistory_Create_3")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Indv_Customer_Master_Id","nvarchar(255)").AddField("Indv_Customer_Name","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Indv_Customer_Address","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividualChangeHistory_Create_3").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Indv_Customer_Master_Id,Indv_Customer_Address,Id,Deleted,Last_Updated_Date,Effective_Date").Values(TeamsCompare).Execute()

#Join to get details
TeamsCompare=App.Sql.Read("#CallDeckIndividualChangeHistory_Create_3","Temp1").Join("Customer","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_Customer_Master_Id","ppc.Indv"))
TeamsCompare.Select("Temp1.CallDeck_Change_Type,Temp1.Position,Temp1.Position_Legacy_Id,Temp1.CallDeck_Status,Temp1.Changed_By_User_Name,ppc.Customer_Master_Id as Indv_Customer_Master_Id,ppc.Customer_Name as Indv_Customer_Name,Temp1.Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","Effective_Date").Select("left(Temp1.Last_Updated_Date,19)+' -05:00'+Temp1.Id","Id")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividualChangeHistory_Create_4")
TableCreate.AddField("CallDeck_Change_Type","nvarchar(255)").AddField("Position","nvarchar(255)").AddField("Position_Legacy_Id","nvarchar(255)").AddField("CallDeck_Status","nvarchar(255)").AddField("Changed_By_User_Name","nvarchar(255)").AddField("Indv_Customer_Master_Id","nvarchar(255)").AddField("Indv_Customer_Name","nvarchar(255)").AddField("Last_Updated","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividualChangeHistory_Create_4").Fields("CallDeck_Change_Type,Position,Position_Legacy_Id,CallDeck_Status,Changed_By_User_Name,Indv_Customer_Master_Id,Indv_Customer_Name,Deleted,Last_Updated,Effective_Date,Id").Values(TeamsCompare).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividualChangeHistory_Create_4").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividualChangeHistory_Create_4","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividualChangeHistory_Create_4'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndvChangeHistory"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualChangeHistory_Create_4")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndvChangeHistory")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndvChangeHistory"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndvChangeHistory"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividualChangeHistory_Create_4")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#================#================#================#================/CreateChangeHistoryIndividual#================#================#================#
#=====================/This part end dates all temporary alignments corresponding to which a permanent alignment has been received from JAMS#=========

#=============#=====================#=============================Apply Account BR=========#=====================#=====================#==============

#get CDA To be Deleted - GlobalAccountExclusion
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("GlobalAccountExclusion","ppc","Inner",App.Sql.CreateClause("Temp1.Acct_Customer","ppc.Customer").And("ppc.Action_Code","'1'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_globalEx")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_globalEx").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Update("GlobalAccountExclusion","Temp1").Set("Temp1.Action_Code","'0'").Where(App.Sql.CreateClause("Action_Code","'1'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_globalEx").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_globalEx","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_globalEx'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_globalEx")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_globalEx")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#get CDA with details
TeamsCompare=App.Sql.Read("CallDeckAccount","Temp1").Join("Account","ppc","Inner",App.Sql.CreateClause("Temp1.Acct_Customer","ppc.Id").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,left(Temp1.Last_Updated_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',ppc.Acct_CoT,ppc.Acct_Sub_Cat")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_withsubcatcot")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Acct_CoT","nvarchar(255)").AddField("Acct_Sub_Cat","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_withsubcatcot").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Last_Updated_Date,End_Date,Acct_CoT,Acct_Sub_Cat").Values(TeamsCompare).Execute()

#get CDA with details
TeamsCompare=App.Sql.Read("#CallDeckAccount_withsubcatcot","Temp1").Join("Position","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Id"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,left(Temp1.Last_Updated_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',Temp1.Acct_CoT,Temp1.Acct_Sub_Cat,ppc.Sales_Team")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_withsubcatcot_team")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Acct_CoT","nvarchar(255)").AddField("Acct_Sub_Cat","nvarchar(255)").AddField("Sales_Team","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_withsubcatcot_team").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Last_Updated_Date,End_Date,Acct_CoT,Acct_Sub_Cat,Sales_Team").Values(TeamsCompare).Execute()



#get CDA To be Deleted - AccountSubCatSalesTeamMap
TeamsCompare=App.Sql.Read("#CallDeckAccount_withsubcatcot_team","Temp1").Join("AccountSubCatSalesTeamMap","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Acct_Sub_Cat","ppc.Acct_Sub_Cat").And("ppc.Action_Code","'3'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_AcctSubcat")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_AcctSubcat").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Delete("AccountSubCatSalesTeamMap","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_AcctSubcat").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_AcctSubcat","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_AcctSubcat'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AcctSubcat")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AcctSubcat")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#get CDA To be Deleted - AccountCoTSalesTeamMap
TeamsCompare=App.Sql.Read("#CallDeckAccount_withsubcatcot_team","Temp1").Join("AccountCoTSalesTeamMap","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Acct_Sub_Cat","","IsNull").And("Temp1.Acct_CoT","ppc.Acct_CoT").And("ppc.Action_Code","'3'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_AcctCot")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_AcctCot").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Delete("AccountCoTSalesTeamMap","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_AcctCot").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_AcctCot","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_AcctCot'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AcctCot")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AcctCot")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#get CDA To be Deleted - AccountExclusion
TeamsCompare=App.Sql.Read("#CallDeckAccount_withsubcatcot_team","Temp1").Join("AccountExclusion","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Acct_Customer","ppc.Customer").And("ppc.Action_Code","'1'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_AccountExclusion")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_AccountExclusion").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Update("AccountExclusion","Temp1").Set("Temp1.Action_Code","'0'").Where(App.Sql.CreateClause("Action_Code","'1'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_AccountExclusion").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_AccountExclusion","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_AccountExclusion'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AccountExclusion")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AccountExclusion")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#get CDA To be Deleted - AccountInclusion
TeamsCompare=App.Sql.Read("#CallDeckAccount_withsubcatcot_team","Temp1").Join("AccountInclusion","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Acct_Customer","ppc.Customer").And("ppc.Action_Code","'3'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Acct_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckAccount_Del_AccountInclusion")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Acct_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckAccount_Del_AccountInclusion").Fields("Position,Alignment_Status,Acct_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Delete("AccountInclusion","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckAccount_Del_AccountInclusion").Distinct()
CountPersnl=App.Sql.Read("#CallDeckAccount_Del_AccountInclusion","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckAccount_Del_AccountInclusion'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckAccount"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AccountInclusion")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckAccount")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckAccount"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckAccount"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckAccount_Del_AccountInclusion")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")





#=============#=====================#=============================/Apply Account BR=========#=====================#=====================#==============
#=============#=====================#=============================Apply Individual BR=========#=====================#=====================#==============

#get CDA To be Deleted - GlobalIndividualExclusion
TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("GlobalIndividualExclusion","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_Customer","ppc.Customer").And("ppc.Action_Code","'1'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_globalEx")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_globalEx").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Update("GlobalIndividualExclusion","Temp1").Set("Temp1.Action_Code","'0'").Where(App.Sql.CreateClause("Action_Code","'1'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_globalEx").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_globalEx","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_globalEx'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_globalEx")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_globalEx")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#get CDI with details
TeamsCompare=App.Sql.Read("CallDeckIndividual","Temp1").Join("Individual","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_Customer","ppc.Id").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,left(Temp1.Last_Updated_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',ppc.Indv_Type,ppc.Indv_Specialty_CD,ppc.Indv_CoT")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_withDetails")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Indv_Type","nvarchar(255)").AddField("Indv_Specialty_CD","nvarchar(255)").AddField("Indv_CoT","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_withDetails").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Last_Updated_Date,End_Date,Indv_Type,Indv_Specialty_CD,Indv_CoT").Values(TeamsCompare).Execute()

#get CDI with details
TeamsCompare=App.Sql.Read("#CallDeckIndividual_withDetails","Temp1").Join("Position","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Id"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,left(Temp1.Last_Updated_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',Temp1.Indv_Type,Temp1.Indv_Specialty_CD,Temp1.Indv_CoT,ppc.Sales_Team,ppc.Is_Scientific_Awareness_Product")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_withDetails_team")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Indv_Type","nvarchar(255)").AddField("Indv_Specialty_CD","nvarchar(255)").AddField("Indv_CoT","nvarchar(255)").AddField("Sales_Team","nvarchar(255)").AddField("Is_Scientific_Awareness_Product","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_withDetails_team").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Last_Updated_Date,End_Date,Indv_Type,Indv_Specialty_CD,Indv_CoT,Sales_Team,Is_Scientific_Awareness_Product").Values(TeamsCompare).Execute()

#get CDI with details -COT
TeamsCompare=App.Sql.Read("#CallDeckIndividual_withDetails_team","Temp1").Join("CoT","ppc","Inner",App.Sql.CreateClause("Temp1.Indv_CoT","ppc.Id"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,Temp1.Action_Code,Temp1.Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,left(Temp1.Last_Updated_Date,19)+' -05:00',left(Temp1.End_Date,19)+' -05:00',Temp1.Indv_Type,Temp1.Indv_Specialty_CD,Temp1.Indv_CoT,Temp1.Sales_Team,Temp1.Is_Scientific_Awareness_Product,ppc.Cust_Type_CD,ppc.Owner_CD")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_withDetails_team_Cot")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Indv_Type","nvarchar(255)").AddField("Indv_Specialty_CD","nvarchar(255)").AddField("Indv_CoT","nvarchar(255)").AddField("Sales_Team","nvarchar(255)").AddField("Is_Scientific_Awareness_Product","nvarchar(255)").AddField("Cust_Type_CD","nvarchar(255)").AddField("Owner_CD","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_withDetails_team_Cot").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Last_Updated_Date,End_Date,Indv_Type,Indv_Specialty_CD,Indv_CoT,Sales_Team,Is_Scientific_Awareness_Product,Cust_Type_CD,Owner_CD").Values(TeamsCompare).Execute()

#get CDA To be Deleted - Individual PositionSpecialtyMap
TeamsCompare=App.Sql.Read("#CallDeckIndividual_withDetails_team_Cot","Temp1").Join("PositionSpecialtyMap","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.Position").And("Temp1.Indv_Specialty_CD","ppc.Specialty").And("ppc.Action_Code","'3'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'").And("Temp1.Is_Scientific_Awareness_Product","'0'").And("Temp1.Indv_Type","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_PositionSpecialtyMap")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_PositionSpecialtyMap").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Delete("PositionSpecialtyMap","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_PositionSpecialtyMap").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_PositionSpecialtyMap","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_PositionSpecialtyMap'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_PositionSpecialtyMap")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_PositionSpecialtyMap")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")


#get CDA To be Deleted - Individual HBPCotSalesTeamMap
TeamsCompare=App.Sql.Read("#CallDeckIndividual_withDetails_team_Cot","Temp1").Join("HBPCotSalesTeamMap","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Cust_Type_CD","ppc.Cust_Type_CD").And("Temp1.Owner_CD","ppc.Owner_CD").And("ppc.Action_Code","'3'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'").And("Temp1.Indv_Type","'2'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_HBPCotSalesTeamMap")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_HBPCotSalesTeamMap").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Delete("HBPCotSalesTeamMap","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_HBPCotSalesTeamMap").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_HBPCotSalesTeamMap","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_HBPCotSalesTeamMap'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_HBPCotSalesTeamMap")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_HBPCotSalesTeamMap")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#get CDA To be Deleted - Individual IndividualExclusion
TeamsCompare=App.Sql.Read("#CallDeckIndividual_withDetails_team_Cot","Temp1").Join("IndividualExclusion","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Indv_Customer","ppc.Customer").And("ppc.Action_Code","'1'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_IndividualExclusion")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_IndividualExclusion").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Update("IndividualExclusion","Temp1").Set("Temp1.Action_Code","'0'").Where(App.Sql.CreateClause("Action_Code","'1'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_IndividualExclusion").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_IndividualExclusion","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_IndividualExclusion'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_IndividualExclusion")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_IndividualExclusion")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#get CDA To be Deleted - Individual IndividualInclusion
TeamsCompare=App.Sql.Read("#CallDeckIndividual_withDetails_team_Cot","Temp1").Join("IndividualInclusion","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team").And("Temp1.Indv_Customer","ppc.Customer").And("ppc.Action_Code","'3'").And("left(Temp1.Effective_Date,19)+' -05:00'","'"+Check+"'","LessThanOrEqualTo").And("left(Temp1.End_Date,19)+' -05:00'","'"+Check+"'","GreaterThan").And("Temp1.Alignment_Status","'1'"))
TeamsCompare.Select("Temp1.Position,Temp1.Alignment_Status,Temp1.Indv_Customer,left(Temp1.Effective_Date,19)+' -05:00',Temp1.Is_Explicit,'2' as Action_Code,'0' as Send_Out,Temp1.Implicit_Explicit_Change_Date,'1' as Medical_Affair_Send_Out,Temp1.Id,'N' as Deleted").Select("'"+Check+"'","Last_Updated_Date").Select("'"+endDate_mad_new+"'","End_Date")

TableCreate = App.Sql.CreateTempTable("CallDeckIndividual_Del_IndividualInclusion")
TableCreate.AddField("Position","nvarchar(255)").AddField("Alignment_Status","nvarchar(255)").AddField("Indv_Customer","nvarchar(255)").AddField("Effective_Date","nvarchar(255)").AddField("End_Date","nvarchar(255)").AddField("Last_Updated_Date","nvarchar(255)").AddField("Is_Explicit","nvarchar(255)").AddField("Action_Code","nvarchar(255)").AddField("Send_Out","nvarchar(255)").AddField("Implicit_Explicit_Change_Date","nvarchar(255)").AddField("Medical_Affair_Send_Out","nvarchar(255)").AddField("Id","nvarchar(255)").AddField("Deleted","nvarchar(255)")
TableCreate.Execute()

App.Sql.Insert("#CallDeckIndividual_Del_IndividualInclusion").Fields("Position,Alignment_Status,Indv_Customer,Effective_Date,Is_Explicit,Action_Code,Send_Out,Implicit_Explicit_Change_Date,Medical_Affair_Send_Out,Id,Deleted,Last_Updated_Date,End_Date").Values(TeamsCompare).Execute()

#Update Action Code
#App.Sql.Delete("IndividualInclusion","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

#================Export To S3================
PersonnelFile=App.Sql.Read("#CallDeckIndividual_Del_IndividualInclusion").Distinct()
CountPersnl=App.Sql.Read("#CallDeckIndividual_Del_IndividualInclusion","Temp1").Select("COUNT((Temp1.Id)) AS 'Count'").Select("'CallDeckIndividual_Del_IndividualInclusion'","FileName")
App.Sql.Insert("#TempBatchCount").Fields("Count","FileName").Values(CountPersnl).Execute()
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CallDeckIndividual"+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_IndividualInclusion")
exportConfig.SetQueryAndFileName(PersonnelFile,"CallDeckIndividual")
exportConfig.RemoveDeletedColumn()
path = Util.CreateFilePath()
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CallDeckIndividual"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))

# #================Import From S3================
# Files = ["CallDeckIndividual"]

# for i in range(0,len(Files)):
    # App.Log(Files[i])
    # overWriteParams = Dictionary[str, object]()
    # overWriteParams.Add('FileName',Files[i]+".txt")
    # overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Archival/CallDeckIndividual_Del_IndividualInclusion")
    # response_1 = App.Integration.Consumer.Consume("ImportFromS3",filesTodump,overWriteParams)
    # App.Log(str(response_1.Content))
    # App.Log(str(response_1.EndPoint))
    # App.Log(str(response_1.IsSuccess))
    # App.Log(str(response_1.ResponseType))

    # mapper_1=App.Data.CreateFileMapper()
    # fileToBoMapping1 = mapper_1.SetFileToBoMapping(Files[i], Files[i])
    # response=App.Data.Import(response_1.Content, mapper_1, True)
    # App.Log(response)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusCode)
    # App.Log(response.GetStatus(str(Files[i]+".txt")).StatusMessage)
    # App.Log(response.GetStatus(str(Files[i]+".txt")))

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "BadData":
        # App.Log("Data Load Issue")
        # app.Log("Error")

    # if str(response.GetStatus(str(Files[i]+".txt")).StatusCode) == "NoResponseFound":
        # App.Log("File Has Object Reference Issue")
        # app.Log("error")



#=============#=====================#=============================//Apply Individual BR=========#=====================#=====================#==============


#=====================This part Deletes hierarchy rows with action code delete#==============

# App.Sql.Delete("Hierarchy","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Child_Position","ppc.PositionID").And("Action_Code","'3'")).Execute()
# #deleteHierarchyList=App.Query("Hierarchy").Where("Child_Position.Id",PositionRecords,"In").Where("Action_Code","3").Delete()
# # updatePositionList=App.Query("Position").SetValue("Action_Code","0").Where("Sales_Team.Id",SalesTeamRecords,"In").Where("Action_Code","1").Or("Action_Code","2").Update()
# App.Sql.Update("Position","Temp1").Set("Temp1.Action_Code","'0'").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team_Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'1'")).Execute()

# App.Sql.Update("Position","Temp1").Set("Temp1.Action_Code","'0'").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team_Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'2'")).Execute()

# # updateHierarchyList=App.Query("Hierarchy").SetValue("Action_Code","0").Where("Child_Position.Id",PositionRecords,"In").Where("Action_Code","1").Update()

# App.Sql.Update("Hierarchy","Temp1").Set("Temp1.Action_Code","'0'").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Child_Position","ppc.PositionID").And("Action_Code","'1'")).Execute()

# App.Sql.Delete("PositionProductMap","Temp1").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("Action_Code","'3'")).Execute()
# # positionProductDeleteList=App.Query("PositionProductMap").Where("Position.Id",PositionRecords,"In").Where("Action_Code","3").Delete()
# App.Sql.Update("PositionProductMap","Temp1").Set("Temp1.Action_Code","'0'").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("Action_Code","'1'")).Execute()
# App.Sql.Update("PositionProductMap","Temp1").Set("Temp1.Action_Code","'0'").Join("#Position_1","ppc","Inner",App.Sql.CreateClause("Temp1.Position","ppc.PositionID").And("Action_Code","'2'")).Execute()
# # positionProductInsertUpdateList=App.Query("PositionProductMap").SetValue("Action_Code","0").Where("Position.Id",PositionRecords,"In").Where("Action_Code","1").Or("Action_Code","2").Update()

# App.Sql.Delete("AccountExclusion","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team_Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'3'")).Execute()

# App.Sql.Delete("IndividualExclusion","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team_Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'3'")).Execute()

# App.Sql.Delete("SalesTeamAlignmentEntityTypeInclusionRules","Temp1").Join("SalesTeam","ppc","Inner",App.Sql.CreateClause("Temp1.Sales_Team","ppc.Sales_Team_Id").And("ppc.Medical_Affair_Team_Flag","'1'").And("Action_Code","'3'")).Execute()

# App.Sql.Delete("GlobalAccountExclusion","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()

# App.Sql.Delete("GlobalIndividualExclusion","Temp1").Where(App.Sql.CreateClause("Action_Code","'3'")).Execute()


# deleteAccountExclusion=App.Query("AccountExclusion").Where("Sales_Team",SalesTeamValue,"In").Where("Action_Code","3").Delete()
# delete_global_account_exclusion=App.Query("GlobalAccountExclusion").Where("Action_Code","3").Delete()
# deleteIndividualExclusion=App.Query("IndividualExclusion").Where("Sales_Team",SalesTeamValue,"In").Where("Action_Code","3").Delete()
# delete_global_individual_exclusion=App.Query("GlobalIndividualExclusion").Where("Action_Code","3").Delete()
# delete_SalesTeamAlignmentEntityTypeInclusionRules=App.Query("SalesTeamAlignmentEntityTypeInclusionRules").Where("Sales_Team",SalesTeamValue,"In").Where("Action_Code","3").Delete()


#============================Count  Log File

PersonnelFile=App.Sql.Read("#TempBatchCount")
exportConfig=App.Data.CreateFileExportConfig()
overWriteParams = Dictionary[str, object]()
overWriteParams.Add('FileName', "CountCheck_MonthlyJAMSImport"+Check+".txt")
overWriteParams.Add('Bucket',"aws-a0116-use1-00-d-s3b-merc-shr-deploy01/JAMS_To_JCDM/Logs")
overWriteParams.Add('Delimiter',"pipe")
exportConfig.SetQueryAndFileName(PersonnelFile,"CountCheck_MonthlyJAMSImport")
exportConfig.SetDelimiter("Pipe")
actualPath = App.Data.Export(path, exportConfig,False)
App.Log("Exported to " + str(actualPath))
filePathList = List[str]()
filePathList.Add(actualPath+"/"+"CountCheck_MonthlyJAMSImport"+".txt")
integration = App.Integration.Publisher.Publish("ExportToNewS3",filePathList,overWriteParams)
App.Log(str(integration[0].Content))
App.Log(str(integration[0].EndPoint))
App.Log(str(integration[0].IsSuccess))
App.Log(str(integration[0].ResponseType))




