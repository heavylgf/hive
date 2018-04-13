DECLARE @startDay int  --yyyyMMdd
DECLARE @endDay int    --yyyyMMdd

set @startDay=20170101
set @endDay=20170101

print 'start bcp out'
while @startDay<=@endDay
begin
  declare @table1 nvarchar(100)
  declare @table2 nvarchar(100)
  declare @table3 nvarchar(100)
  declare @outfile nvarchar(100)
  declare @cmd nvarchar(1000)
  
  set @table1='silverlogdb.dbo.BackBoxLog_'+convert(varchar(20),@startDay)
  set @table2='silverlogdb.dbo.GameLog_'+convert(varchar(20),@startDay)
  set @table3='silverlogdb.dbo.SafeBoxLog_'+convert(varchar(20),@startDay)
  set @outfile='G:\'+convert(varchar(20),@startDay)
  set @cmd='bcp '+'"(SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, 0, GUID, '''', '''', '''' FROM '+@table1+') union all (SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, GameDiff, GUID, '''', '''', '''' FROM '+@table2+ ') union all (SELECT UserID, GameID, OperationID, Deposit, Balance, IP, CreateDate, CreateTime, Description, 0, 0, GUID, '''', '''', '''' FROM '+@table3+')"'+' queryout '+@outfile+' -c -t, -T -SWIN-4IEIO2EG432\SQLSERVER2008'

    exec master..xp_cmdshell @cmd

  declare @dt datetime
  set @dt=convert(datetime,convert(varchar,@startDay),112) 
  set @dt=DATEADD(day,1,@dt)
  set @startDay=convert(int, convert(varchar,@dt,112))  
end

print 'bcp out over.'



