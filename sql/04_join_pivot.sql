WITH ses AS (
    -- ses mail pivot
    SELECT message_id, max(recipient) recipeint, max(completion_time) as completion_time
             , sum(rejected) rejected
             , sum(sent) sent
             , sum(delivered) delivered
             , sum(bounced) bounced
             , sum(opened) opened
             , sum(clicked) clicked
             , sum(complained) complained
    FROM (
        -- pivot base
        SELECT
            message_id
             ,  "to" as recipient
             , coalesce(completion_time::timestamp, "timestamp"::timestamp) as completion_time
             , (CASE WHEN (event_class = 'reject') THEN 1 ELSE 0 END) rejected
             , (CASE WHEN (event_class = 'send') THEN 1 ELSE 0 END) sent
             , (CASE WHEN (event_class = 'delivery') THEN 1 ELSE 0 END) delivered
             , (CASE WHEN (event_class = 'bounce') THEN 1 ELSE 0 END) bounced
             , (CASE WHEN (event_class = 'open') THEN 1 ELSE 0 END) opened
             , (CASE WHEN (event_class = 'complaint') THEN 1 ELSE 0 END) complained
             , (CASE WHEN (event_class = 'click') THEN 1 ELSE 0 END) clicked
        FROM
            ses_mail
        ) as sm
        GROUP BY message_id
    )
-- grouped by region
SELECT region,
       sum(ses.rejected) rejected,
       sum(ses.sent) sent,
       sum(ses.bounced) bounced,
       sum(ses.delivered) delivered,
       sum(ses.complained) complained,
       sum(ses.opened) opened,
       sum(ses.clicked) clicked
FROM
    (ses_requests R
        LEFT JOIN ses ON (R.message_id = ses.message_id))
group by region;