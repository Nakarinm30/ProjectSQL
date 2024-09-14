-- Check import data before analysis
select * from Bakery_Sales
select * from Bakery_price

-- Data Transform and Data Cleaning

-- Check data and Remove Duplicate Data
WITH cte_DuplicateRows AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY 
            date_time, day_of_week, place, 
            angbutter, plain_bread, jam, americano, croissant, 
            caffe_latte, tiramisu_croissant, cacao_deep, pain_au_chocolat, 
            almond_croissant, croque_monsieur, mad_garlic, milk_tea, 
            gateau_chocolat, pandoro, cheese_cake, lemon_ade, 
            orange_pound, wiener, vanila_latte, berry_ade, tiramisu, 
            merinque_cookies
            ORDER BY date_time
        ) AS RowNum
    FROM Bakery_Sales
)
-- Select the duplicates to verify before deletion
SELECT *
FROM cte_DuplicateRows
WHERE RowNum > 1;

-- Delete the duplicate rows
WITH cte_DuplicateRows AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY 
            date_time, day_of_week, place, 
            angbutter, plain_bread, jam, americano, croissant, 
            caffe_latte, tiramisu_croissant, cacao_deep, pain_au_chocolat, 
            almond_croissant, croque_monsieur, mad_garlic, milk_tea, 
            gateau_chocolat, pandoro, cheese_cake, lemon_ade, 
            orange_pound, wiener, vanila_latte, berry_ade, tiramisu, 
            merinque_cookies
            ORDER BY date_time
        ) AS RowNum
    FROM Bakery_Sales
)
DELETE FROM Bakery_Sales
WHERE EXISTS (
    SELECT 1
    FROM cte_DuplicateRows
    WHERE Bakery_Sales.datetime = cte_DuplicateRows.datetime
    AND Bakery_Sales.day_of_week = cte_DuplicateRows.day_of_week
    AND Bakery_Sales.place = cte_DuplicateRows.place
    AND cte_DuplicateRows.RowNum > 1
)

-- Handling Missing Values
select * from Bakery_Sales
where place is null

delete from Bakery_Sales
where place is null

-- Unpivot Data 

-- Select data before create table
select 
	date_time,
	day_of_week,
	total,
	place,
	items,
	quantity
from Bakery_Sales
unpivot (
	quantity for items in ( [angbutter], [plain_bread], [jam], [americano], [croissant], 
		[caffe_latte], [tiramisu_croissant], [cacao_deep], [pain_au_chocolat], 
		[almond_croissant], [croque_monsieur], [mad_garlic], [milk_tea], 
		[gateau_chocolat], [pandoro], [cheese_cake], [lemon_ade], 
		[orange_pound], [wiener], [vanila_latte], [berry_ade], 
		[tiramisu], [merinque_cookies]
	)
) as Unpivotdata

create table Bakery_Sales_unpivot (
	date_time datetime,
	day_of_week nvarchar(50),
	total int,
	place nvarchar(50),
	items nvarchar(100),
	quantity int
)

insert into Bakery_Sales_unpivot ( date_time, day_of_week, total, place, items, quantity)
select 
	date_time,
	day_of_week,
	total,
	place,
	items,
	quantity
from Bakery_Sales
unpivot (
	quantity for items in ( [angbutter], [plain_bread], [jam], [americano], [croissant], 
		[caffe_latte], [tiramisu_croissant], [cacao_deep], [pain_au_chocolat], 
		[almond_croissant], [croque_monsieur], [mad_garlic], [milk_tea], 
		[gateau_chocolat], [pandoro], [cheese_cake], [lemon_ade], 
		[orange_pound], [wiener], [vanila_latte], [berry_ade], 
		[tiramisu], [merinque_cookies]
	)
) as Unpivotdata

exec sp_rename 'Projects.dbo.Bakery_Sales' , 'Bakery_Sales_Backup'

exec sp_rename 'Bakery_Sales_unpivot', 'Bakery_Sales'


-- Data analysis 
-- Total Revenue by Each Product: 
select
	s.items,
	SUM(s.quantity) as total_sold,
	SUM(s.quantity * p.price) as total_revenue
from Bakery_Sales s
inner join Bakery_price p
on s.items = p.Name
group by s.items
order by total_revenue desc

-- Revenue per Day of the Week:
select
	DATENAME(WEEKDAY, s.date_time) as Day_of_week,
	SUM(s.quantity) as total_sold,
	SUM(s.quantity * p.price) as total_revenue
from Bakery_Sales s
inner join Bakery_price p
on s.items = p.Name
group by DATENAME(WEEKDAY, s.date_time), DATEPART(WEEKDAY, s.date_time)
order by DATEPART(WEEKDAY, s.date_time)

-- Total Orders by Place:
select 
	place,
	SUM(quantity) as Total_Order
from Bakery_Sales
group by place
order by Total_Order desc

-- Average Basket Value:
with cte_OrderTotal as
(
	select
		s.date_time as OrderTime,
		sum(s.quantity * p.price) as Total_Order,
		count(*) as Total_transaction
	from Bakery_Sales s
	inner join Bakery_price p
		on s.items = p.Name
	group by s.date_time
)
select
	OrderTime,
	Total_Order / Total_Transaction as average_basket_value
from cte_OrderTotal
order by OrderTime asc

-- Peak Sales Hours:
select 
	DATEPART(hh, date_time) as HourTime,
	SUM(s.quantity * p.price) as total_revenue
from Bakery_Sales s
inner join Bakery_price p
on s.items = p.Name
group by DATEPART(hh, date_time)
order by HourTime asc

-- Product Performance Throughout the Day:
select
	case
		when DATEPART(hh, date_time) between 5 and 12 then 'Morning'
		when DATEPART(hh, date_time) between 13 and 16 then 'Afternoon'
		when DATEPART(hh, date_time) between 17 and 20 then 'Evening'
		when DATEPART(hh, date_time) between 22 and 24 then 'Night'
	end as Time_routine,
	items,
	SUM(quantity) as Total_Order
from Bakery_Sales
group by 
	case
		when DATEPART(hh, date_time) between 5 and 12 then 'Morning'
		when DATEPART(hh, date_time) between 13 and 16 then 'Afternoon'
		when DATEPART(hh, date_time) between 17 and 20 then 'Evening'
		when DATEPART(hh, date_time) between 22 and 24 then 'Night'
	end,
	items
order by Total_Order desc

-- MoM Sales Trend Analysis
with cte_Monthly_Sale as
(
	select 
		DATEPART(yyyy, date_time) as Year_date,
		DATEPART(mm, date_time) as Month_date,
		SUM(s.quantity * p.price) as Total_Revenue
	from Bakery_Sales s
	inner join Bakery_price p
	on s.items = p.Name
	group by DATEPART(yyyy, date_time), DATEPART(mm, date_time)
)
select 
	Year_date,
	Month_date,
	Total_Revenue as Current_Monthly_Sale,
	LAG(Total_Revenue) over (order by Year_date, Month_date) as Prev_Monthly_Sale,
	round((Total_Revenue - LAG(Total_Revenue) over (order by Year_date, Month_date)) / cast(LAG(Total_Revenue) over (order by Year_date, Month_date) as decimal(9,2)) * 100,2) as MoM
from cte_Monthly_Sale
order by Year_date, Month_date

-- Market Basket Analysis:
with cte_MBA as
(
	select 
		a.items as itemsA,
		b.items as itemsB,
		count(1) as Total_count
	from Bakery_Sales a
	inner join Bakery_Sales b
	on a.date_time = b.date_time
	and a.items <> b.items
	and a.items < b.items
	group by a.items, b.items
),
cte_total_transaction as
(
	select
		count(distinct date_time) as Total_Transaction
	from Bakery_Sales
),
cte_Total_items_purchase as
(
	select
		items as item,
		count(date_time) as Total_items_purchase
	from Bakery_Sales
	group by items
)
select 
	cte_MBA.itemsA,
	cte_MBA.itemsB,
	cte_MBA.Total_count,
	-- Support(A ? B)
	(cte_MBA.Total_count / CAST(tt.Total_Transaction AS DECIMAL(9,2))) * 100 AS 'SupportA=>B',
	-- Support(A)
	(tpA.Total_items_purchase / CAST(tt.Total_Transaction AS DECIMAL(9,2))) * 100 AS SupportA,
	-- Support(B)
	(tpB.Total_items_purchase / CAST(tt.Total_Transaction AS DECIMAL(9,2))) * 100 AS SupportB,
	-- Confidence(A ? B)
	((cte_MBA.Total_count / CAST(tt.Total_Transaction AS DECIMAL(9,2))) 
	/ (tpA.Total_items_purchase / CAST(tt.Total_Transaction AS DECIMAL(9,2)))) * 100 AS 'ConfidenceA=>B',
	-- Lift(A ? B)
	((cte_MBA.Total_count / CAST(tt.Total_Transaction AS DECIMAL(9,2))) 
	/ ((tpA.Total_items_purchase / CAST(tt.Total_Transaction AS DECIMAL(9,2))) 
		* (tpB.Total_items_purchase / CAST(tt.Total_Transaction AS DECIMAL(9,2))))) AS 'LiftA=>B'
from cte_MBA
cross join cte_total_transaction tt
inner join cte_Total_items_purchase  tpA
on cte_MBA.itemsA = tpA.item
inner join cte_Total_items_purchase  tpB
on cte_MBA.itemsB = tpB.item
order by [SupportA=>B] desc
