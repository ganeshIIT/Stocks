use Stocksdb

select * from stocks
select * from scrips



declare @maxdate date
select @maxdate = max(date) from stocks
-- select @maxdate

select s.name, 
sc.Quantity,
BuyPrice = sc.Avg_Trading_Price, 
CurrentPrice = s.[close], 
InvestedValue = sc.Avg_Trading_Price * sc.quantity,
CurrentValue = s.[close] * sc.quantity,
Profit = (s.[close] * sc.quantity)-(sc.Avg_Trading_Price * sc.quantity),
ProfitPercent = (s.[close] - sc.Avg_Trading_Price)* 100.0/sc.Avg_Trading_Price,
OverallNetProfit = sum((s.[close] * sc.quantity)-(sc.Avg_Trading_Price * sc.quantity)) over(),
OverallNetProfitPercent = sum((s.[close] * sc.quantity)-(sc.Avg_Trading_Price * sc.quantity)) over() 
                            * 100/
                            sum(sc.Avg_Trading_Price * sc.quantity) over(),
ContributionToOverallProfit = ((s.[close] * sc.quantity)-(sc.Avg_Trading_Price * sc.quantity)) 
                                * 100/
                                (sum((s.[close] * sc.quantity)-(sc.Avg_Trading_Price * sc.quantity)) over())
from stocks s join scrips sc
on sc.scrip = s.name
where s.date = @maxdate