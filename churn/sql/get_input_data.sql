select featurestore.* except(is_churner),
       -- Use Case Specific Features
       coalesce(use_case_features.nb_orders_last_12_days, 0) as nb_orders_last_12_days,
       featurestore.is_churner,
       case 
           when featurestore.bucket < 7 then 'TRAIN' 
           when featurestore.bucket >= 7 and featurestore.bucket < 9 then 'VALIDATE'
           when featurestore.bucket >= 9 then 'TEST'
       end as split
  from `looker-sandbox-323013.tests.churn_featurestore` as featurestore
  left outer join (
      SELECT user_id,
             sum(case when date(created_at) >= date_add(date_add(current_date(), interval -15 day), interval -12 day)  then 1 else 0 end) as nb_orders_last_12_days
        FROM `looker-sandbox-323013.thelook.order_items` 
       where date(created_at)  < date_add(current_date(), interval -15 day)
         and date(created_at) >= date_add(current_date(), interval -30 day)
       group by 1
  ) as use_case_features
  using(user_id)