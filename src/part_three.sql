# Inmon's approach to data warehousing involves building a centralized data warehouse with a normalized data model.
# In the context of the streaming service subscription lifecycle, we can create an Inmon-style data model with a few tables, capturing the essential information about users, subscription plans, and subscription statuses.

# 1 Create a table to store subscription plans:
CREATE TABLE dim_subscription_plans
(
    subscription_plan_id INTEGER PRIMARY KEY,
    name                 VARCHAR(255) NOT NULL
);

-- Insert subscription plans
INSERT INTO dim_subscription_plans (subscription_plan_id, name)
VALUES (1, 'Mobile'),
       (2, 'Basic'),
       (3, 'Standard'),
       (4, 'Premium');


# 2 Create a table to store user data:
CREATE TABLE users
(
    user_id INTEGER PRIMARY KEY
);


# 3 Create a table to store the user subscription history:
CREATE TABLE fact_user_subscription_history
(
    id                     SERIAL PRIMARY KEY,
    user_id                INTEGER   NOT NULL,
    subscription_plan_id   INTEGER   NOT NULL,
    from_date              TIMESTAMP NOT NULL,
    to_date                TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (subscription_plan_id) REFERENCES dim_subscription_plans (subscription_plan_id)
);


CREATE VIEW subscription_movements AS
SELECT
    user_id,
    subscription_plan_id,
    from_date AS action_date,
    CASE
        WHEN previous_plan_id IS NULL AND subscription_plan_id IS NOT NULL THEN 'Subscribed'
        WHEN previous_plan_id IS NOT NULL AND subscription_plan_id IS NULL THEN 'Cancelled'
        WHEN previous_plan_id < subscription_plan_id THEN 'Upgraded'
        ELSE 'Downgraded'
        END AS action
FROM (
         SELECT
             user_id,
             subscription_plan_id,
             from_date,
             LAG(subscription_plan_id) OVER (PARTITION BY user_id ORDER BY from_date) AS previous_plan_id,
             LAG(to_date) OVER (PARTITION BY user_id ORDER BY from_date) AS previous_to_date
         FROM fact_user_subscription_history
     ) AS subquery
WHERE
        subscription_plan_id != previous_plan_id OR
(previous_to_date IS NOT NULL AND previous_to_date != from_date - INTERVAL '1 day')
ORDER BY user_id, action_date;