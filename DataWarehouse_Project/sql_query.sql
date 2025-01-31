use data_warehouse;

-- Q1 
with top_5 as (
    select t.time_month, case when t.weekend = 0 then 'weekday' else 'weekend' end as day_type, p.productname, sum(s.total_sales) as total_revenue, row_number() over (partition by t.time_month, t.weekend order by sum(s.total_sales) desc) as or_der  
    from sales s 
    join products p on s.productid = p.productid 
    join time t on s.time_id = t.time_id 
    where t.time_year = 2019 
    group by t.time_month, t.weekend, p.productname
)
select time_month, day_type, productname, total_revenue 
from top_5 
where or_der <=5 
order by time_month, day_type, or_der;

-- Q2
with quarterly_revenue as (
    select s.storeid,s2.storename,t.time_quarter,t.time_year,sum(s.total_sales) as total_revenue from sales s
    join store s2 on s.storeid = s2.storeid join time t on s.time_id = t.time_id where t.time_year = 2019
    group by s.storeid, s2.storename, t.time_quarter, t.time_year
),
growth_rate as (
    select storeid,storename,time_quarter,total_revenue,
    lag(total_revenue) over ( partition by storeid order by time_year, time_quarter) as prev_revenue from quarterly_revenue
)
select storeid,storename,time_quarter,total_revenue,prev_revenue,
case when prev_revenue is not null then ((total_revenue - prev_revenue) / prev_revenue) * 100
else null end as growth_rate from growth_rate order by storeid, time_quarter;


-- Q3
select s.storeid,s2.storename,sp.supplierid,sp.suppliername,p.productname,sum(s.total_sales) as total_sales_contribution
from sales s join store s2 on s.storeid = s2.storeid join supplier sp on s.supplierid = sp.supplierid join products p on s.productid = p.productid
group by s2.storeid, s2.storename, sp.supplierid, sp.suppliername, p.productname
order by s2.storeid, sp.supplierid, p.productname;

-- Q4
select p.productname as product_name,case when t.time_month in (3,4,5) then 'spring' when t.time_month in (6,7,8) then 'summer' when t.time_month in (9,10,11)
then 'autumn' when t.time_month in (12,1,2) then 'winter' end as seasonal_period,sum(s.total_sales) as total_sales from sales s
join products p on s.productid = p.productid
join time t on s.time_id = t.time_id
group by product_name, seasonal_period
order by product_name, seasonal_period;

-- Q5
with temp as (
    select sa.storeid, sa.supplierid, t.time_year as year1, t.time_month as month1, sum(sa.total_sales) as total_revenue
    from sales sa
    join time t on sa.time_id = t.time_id
    group by sa.storeid, sa.supplierid, t.time_year, t.time_month
),
m_v as (
    select storeid, supplierid, year1, month1, total_revenue,
        lag(total_revenue) over (partition by storeid, supplierid order by year1, month1) as prev_revenue,
        case 
            when lag(total_revenue) over (partition by storeid, supplierid order by year1, month1) is null 
            then null
            else (total_revenue - lag(total_revenue) over (partition by storeid, supplierid order by year1, month1)) / 
                 lag(total_revenue) over (partition by storeid, supplierid order by year1, month1) * 100
        end as revenue_volatility
    from temp
)
select storeid, supplierid, year1, month1, total_revenue, prev_revenue, revenue_volatility 
from m_v 
order by storeid, supplierid, year1, month1;

-- Q6
select p1.productname as product_1, p2.productname as product_2, count(*) as bought_together
from sales s1 join sales s2 on s1.order_id = s2.order_id and s1.productid<s2.productid
join products p1 on s1.productid = p1.productid 
join products p2 on s2.productid = p2.productid 
group by p1.productName, p2.productName 
order by bought_together desc 
limit 5;

-- Q7
select year(t.time_date) as year, s1.storeid, s1.supplierid, s1.productid, sum(s1.total_sales) as yearly_revenue 
from sales s1 
join time t on s1.time_id = t.time_id 
group by year(t.time_date), s1.storeid, s1.supplierid, s1.productid with rollup 
order by year, s1.storeid, s1.supplierid, s1.productid;

-- Q8
select p.productname as product, 
sum(case when t.half_year = 1 then s.total_sales else 0 end) as revenue_h1, 
sum(case when t.half_year = 2 then s.total_sales else 0 end) as revenue_h2, 
sum(s.total_sales) as total_revenue_year, 
sum(case when t.half_year = 1 then s.quantity else 0 end) as quantity_h1, 
sum(case when t.half_year = 2 then s.quantity else 0 end) as quantity_h2, 
sum(s.quantity) as total_quantity_year 
from sales s 
join products p on s.productid = p.productid 
join time t on s.time_id = t.time_id 
group by p.productname;

-- Q9
select p.productname as product,t.time_date as date,sum(s.total_sales) as daily_sales,avg(sum(s.total_sales)) over (partition by p.productid) as average_daily, 
case when sum(s.total_sales) > 2 * avg(sum(s.total_sales)) over (partition by p.productid) then 'outlier' else 'normal' end as flag 
from sales s 
join products p on s.productid = p.productid 
join time t on s.time_id = t.time_id 
group by p.productid, p.productname, t.time_date
order by p.productname, t.time_date;

-- Q10
drop view if exists store_quarterly_sales;
create view store_quarterly_sales as
select s.storeid, st.storename, t.time_year, t.time_quarter, sum(s.total_sales) as total_sale_quarter
from sales s
join store st on s.storeid = st.storeid
join time t on s.time_id = t.time_id
group by s.storeid, st.storename, t.time_year, t.time_quarter
order by st.storename, t.time_year, t.time_quarter; 