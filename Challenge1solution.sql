use challenge1;
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
#question4
select m.product_name,count(s.product_id) as purchase_frequency
from sales as s
join menu as m on s.product_id=m.product_id
group by m.product_name
order by purchase_frequency desc limit 1;
#------------------------------------------------------------------------------
#question 5
With rank1 as
(
Select s.customer_id ,m.product_name, count(s.product_id) as frequency,
dense_rank()  Over (Partition by s.customer_id order by Count(S.product_id) desc ) as rank2
From menu m
join sales s
On m.product_id = s.product_id
group by s.customer_id,s.product_id,m.product_name
)
Select customer_id,product_name,frequency
From rank1
where rank2 = 1;
#-----------------------------------------------------------------
#question 6
With Rank1 as
(
Select  s.customer_id,
        m.product_name,
	    Dense_rank() OVER (Partition by s.customer_id Order by s.order_date) as Rank2
From sales s
Join menu m
ON m.product_id = s.product_id
JOIN Members mem
ON mem.customer_id = s.customer_id
Where s.order_date >= mem.join_date  
)
Select *
From Rank1
Where Rank2 = 1;
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
#---------------------------------------------------------------------------------
#question 8
select s.customer_id,count(s.product_id ) as quantity ,sum(m.price) as total_sales
from sales s
join menu m
on m.product_id = s.product_id
join members mem
on Mem.Customer_id = S.customer_id
where s.order_date < mem.join_date
group by s.customer_id;
#-------------------------------------------------------
#question 9
with points_table as
(select *, case when product_id = 1 then price*20 Else price*10 End as points
From menu
)
select s.customer_id, sum(p.points) as points1
From sales s
Join points_table p
On p.product_id = s.product_id
Group by s.customer_id;
#----------------------------------------------------------------------------
#question 10
select s.customer_id,sum(CASE when (DATEDIFF(me.join_date, s.order_date) between 0 and 7) or (m.product_ID = 1) Then m.price * 20
else m.price * 10 end) As Points
from members as me
inner Join sales as s on s.customer_id = me.customer_id
inner Join menu as m on m.product_id = s.product_id
where s.order_date >= me.join_date and s.order_date <= CAST('2021-01-31' AS DATE)
Group by s.customer_id;




  