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