-- custom SQL query in QuickSight
select distinct a.*, 
--update
case when a.sold_out != b.sold_out then 'Ticket Sold Out' else 'Ticket Selling' end as current_ticket_status,
-- delete
case when b.id is null then 'Ticket Cancelled' else 'Ticket Valid' end as ticket_cancelled, 
-- insert
case when a.id is null and b.id is not null then 'New Ticket Listed' end as new_tickets
from raw_tickets.sporting_event  a
full join curated_tickets.sporting_event b on a.id = b.id
