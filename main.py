from etl import etl_Data

query = """
        SELECT
            MarketName, 
            Booking, 
            DateOfBirth, 
            DateBooked, 
            WeekBooked, 
            isnull(ws.SortOrder,13) as SortOrder,
            ProgramDuration,
            DateCaxed, 
            FiscalYear, 
            ProgramStartDate, 
            SalesRepID, 
            u.Name as SalesRepName, 
            OriginalRanking, 
            MethodOfCreation,
            hasBooked =1,
            hasCaxed = case when DateCaxed is null then 0 else 1 end
        from sales s
        join Users u on u.User_id = s.SalesRepID
        left join WeekSort ws on ws.WeekNum = right(s.WeekBooked,2)
        """
        
etl_Data(query)