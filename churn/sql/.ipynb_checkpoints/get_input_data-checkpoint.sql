select featurestore.* except(is_churner),
       -- Use Case Specific Features
       use_case_features.* except(user_id),
       featurestore.is_churner
  from `looker-sandbox-323013.tests.churn_featurestore` as featurestore
  left outer join (
      SELECT user_id,
             sum(case when date(created_at) >= date_add(date_add(current_date(), interval -15 day), interval -12 day)  then 1 else 0 end) as nb_orders_last_12_days
        FROM `looker-private-demo.thelook.order_items` 
       where date(created_at)  < date_add(current_date(), interval -15 day)
         and date(created_at) >= date_add(current_date(), interval -30 day)
       group by 1
  ) as use_case_features
  using(user_id)