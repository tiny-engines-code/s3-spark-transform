-- Create some mock data representing our original request
-- Normally we would already have this data
-- We want to be abe to pvot the ses data and then join against this to get a full journey of each customer
--
create table IF NOT EXISTS ses_requests as (
select message_id,    -- join key
   "to" as recipient,
   (completion_time::timestamp) - (random() * 4)::int * interval '1 seconds' request_time, -- random request time
   coalesce((select ARRAY ['LATAM', 'APAC', 'EU'])[floor((random() * 20))::int],'NA') as region -- random region
  from ses_mail
);
