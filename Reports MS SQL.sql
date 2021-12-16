
create procedure sp_calculate_serv_qty_stat
as
begin

drop table if exists new_servers_per_client_month;
drop table if exists #base_orders;
drop table if exists first_order_server_qty;
drop table if exists new_servers_per_client_month;
drop table if exists #periods;





--drop table #base_orders
select 
o.ID
,c.Name as Client
,s.server_configuration
,cast(year(service_start_date) as varchar)+'_'+cast(month(service_start_date) as varchar) as st_month_year
,rank() over(partition by c.Name  order by year(service_start_date),month(service_start_date) asc) as st_mnth_rank
,month(service_end_date) as end_mnth
,service_start_date
,service_end_date
,price
into #base_orders
from Order_ o
left join Client c on c.ID=o.client_id
left join Service s on s.ID=o.service_id;



select 
Client
,st_month_year
,count(ID) as first_order_server_qty
into first_order_server_qty
from #base_orders
where st_mnth_rank=1
group by
Client
,st_month_year;


select distinct st_month_year
into #periods
from #base_orders;

DECLARE 
    @periods NVARCHAR(MAX) = '',
	@sql NVARCHAR(MAX) = '';

SELECT 
    @periods += QUOTENAME(st_month_year) + ','
FROM 
    #periods

ORDER BY 
 st_month_year;

SET @periods = LEFT(@periods, LEN(@periods) - 1);


set @sql='
with order_gr (Client,st_month_year,qty)
as
	(select 
		Client
		,st_month_year
		,count(ID) as qty
	 from #base_orders
	 group by 
		Client
		,st_month_year
	 )
select 
*
into new_servers_per_client_month
from 
order_gr b
pivot
(sum(qty)
for st_month_year
in ('+ @periods +')
) pvt'



exec sp_executesql @sql;


end;

