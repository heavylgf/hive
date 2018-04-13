DECLARE @startDay int  --yyyyMMdd
DECLARE @endDay int    --yyyyMMdd

set @startDay=20150712
set @endDay=20180103

print 'start bcp out'
while @startDay<=@endDay
begin
  declare @table nvarchar(100)
  declare @outfile nvarchar(100)
  declare @cmd nvarchar(300)
  
  set @table='silverlogdb.dbo.BackBoxLog_'+convert(varchar(20),@startDay)
  set @outfile='G:\'+convert(varchar(20),@startDay)
  set @cmd='bcp '+'"select UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description,0,0,GUID,'''','''','''' from '+@table+'"'+' queryout '+@outfile+' -c -t, -T -SWIN-4IEIO2EG432\SQLSERVER2008'

    exec master..xp_cmdshell @cmd

  declare @dt datetime
  set @dt=convert(datetime,convert(varchar,@startDay),112) 
  set @dt=DATEADD(day,1,@dt)
  set @startDay=convert(int, convert(varchar,@dt,112))  
end

print 'bcp out over.'


EXEC master..xp_cmdshell 'BCP "SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description,0,0,GUID,'''','''','''' FROM silverlogdb.dbo.BackBoxLog_20150712" queryout G:\20150712 -c -t, -T -SWIN-4IEIO2EG432\SQLSERVER2008'

EXEC master..xp_cmdshell 'BCP "SELECT *,-1,-1 FROM silverlogdb.dbo.BackBoxLog_20170712" queryout G:\20170712 -c -t, -T -S"127.0.0.1,1435" -U"xxx" -P"xxxx"'
SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, 0, GUID, "", "", "" FROM silverlogdb.dbo.BackBoxLog_20170712
SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, GameDiff, GUID, "", "", "" FROM silverlogdb.dbo.GameLog_20140120
SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, 0, GUID, "", "", "" FROM silverlogdb.dbo.SafeBoxLog_20131003

EXEC master..xp_cmdshell 'BCP "SELECT *,-1,-1 FROM silverlogdb.dbo.BackBoxLog_20170712" queryout G:\20170712 -c -t, -T -S"127.0.0.1,1435" -U"xxx" -P"xxxx"'



if(
  SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, 0, GUID, "", "", "" FROM silverlogdb.dbo.BackBoxLog_20170712)
union all
if(
  SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, GameDiff, GUID, "", "", "" FROM silverlogdb.dbo.GameLog_20140120)
union all
SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, 0, GUID, "", "", "" FROM silverlogdb.dbo.SafeBoxLog_20131003