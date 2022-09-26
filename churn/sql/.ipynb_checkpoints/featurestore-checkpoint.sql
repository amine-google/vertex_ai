CREATE OR REPLACE TABLE `looker-sandbox-323013.tests.churn_featurestore` AS
    SELECT user_id,
           mod(abs(farm_fingerprint(cast(user_id as string))), 10) as bucket,
           -- Features
           count(1) as lifetime_orders,
           sum(case when date(created_at) >= date_add(date_add(current_date(), interval -15 day), interval -7 day)  then 1 else 0 end) as nb_orders_last_7_days,
           sum(case when date(created_at) >= date_add(date_add(current_date(), interval -15 day), interval -15 day) then 1 else 0 end) as nb_orders_last_15_days,
           -- Label
           user_id in (
             select user_id
               from `looker-private-demo.thelook.order_items`
              where date(created_at) >= date_add(current_date(), interval -15 day)
              group by 1
           ) as is_churner
      FROM `looker-private-demo.thelook.order_items` 
     where date(created_at)  < date_add(current_date(), interval -15 day)
       and date(created_at) >= date_add(current_date(), interval -30 day)
     group by 1