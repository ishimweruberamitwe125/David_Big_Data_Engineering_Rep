#use challenge1;
#question1
select s.customer_id,sum(m.price) from sales as s
left join menu as m on s.product_id=m.product_id
group by customer_id;
#--------------------------------------------------------------
#question2
select  distinct customer_id, count(distinct DATE(order_date)) from sales
group by customer_id;
#----------------------------------------------------------------------------------------
#question 3
with first_product as (
select s.customer_id,m.product_name, row_number() over (partition by customer_id order by customer_id) row_first
from sales as s 
inner join menu as m on s.product_id=m.product_id)
select * from first_product where row_first=1;
#---------------------------------------------------------------
#question 4
select m.product_name, purchases.total_purchases
from (select product_id, COUNT(*) AS total_purchases
from sales 
group by product_id
order by total_purchases desc limit 1
    ) AS purchases
join menu m on purchases.product_id = m.product_id;


#------------------------------------------------------------------------------
#question 5
select s.customer_id,m.product_name,COUNT(s.product_id) AS frequency
from sales as s
left join menu AS m on s.product_id = m.product_id
group by  s.customer_id, m.product_name
having count(s.product_id) = (#max
select max(product_count)from(select s2.product_id,count(s2.product_id) AS product_count
from sales AS s2 where s2.customer_id = s.customer_id group by s2.product_id) as customer_max_counts)
order by s.customer_id;
#-----------------------------------------------------------------
#question 6
WITH Rank1 AS (
select s.customer_id,m.product_name,me.join_date,s.order_date
from sales s join menu m on m.product_id = s.product_id 
join members me on me.customer_id = s.customer_id
where s.order_date >= me.join_date and s.order_date = (
#subquery to find the earliest order date for each customer
select min(s2.order_date) from sales s2 where s2.customer_id = s.customer_id
and s2.order_date >= me.join_date))
select * from Rank1 order by s.customer_id;
#-------------------------------------------------------------------------
#question 7
With Rank1 as
(
Select  s.customer_id,m.product_name,Dense_rank() OVER (Partition by s.customer_id Order by s.order_date) as Rank2
From sales s
Join Menu m
ON m.product_id = s.product_id
JOIN Members mem
ON Mem.customer_id = s.customer_id
Where s.order_date < mem.join_date  
)
Select customer_id, product_name
From Rank1
Where Rank2 = 1;
#----------
WITH Rank2 AS (
select s.customer_id,m.product_name,me.join_date,s.order_date from sales s
join menu m on m.product_id = s.product_id
join members me on me.customer_id = s.customer_id where s.order_date < me.join_date
and s.order_date =(#subquery to get the earliest order date before the join date
select min(s2.order_date) from sales s2
where s2.customer_id = s.customer_id and s2.order_date < me.join_date)
)select customer_id, product_name, join_date,order_date from Rank2 order by customer_id;
#---------------------------------------------------------------------------------
#question 8
select s.customer_id ,count(s.product_id) as total_items_purchased,SUM(m.price) AS total_amount_spent
from sales  s
inner join menu  m on s.product_id = m.product_id
inner join members  me
on me.customer_id = s.customer_id
where s.order_date < me.join_date
group by s.customer_id;

#-------------------------------------------------------
#question 9
select s.customer_id, sum(case 
when m.product_id = 1 then m.price * 20 else m.price * 10 
end) as points1
from sales s
join (select product_id, price, case 
when product_id = 1 then price * 20 else price * 10 
end AS points from menu) as m on s.product_id = m.product_id
group by s.customer_id;
#----------------------------------------------------------------------------
#question 10
select s.customer_id, sum(case 
when (datediff(me.join_date, s.order_date) between 0 AND 7) or (m.product_id = 1) 
then m.price * 20 else m.price * 10 end) as total_points
from members  me
inner join sales s
on s.customer_id = me.customer_id
inner join menu AS m 
on m.product_id = s.product_id
where s.order_date >= me.join_date and s.order_date <= '2021-01-31'
group by me.customer_id;





  
