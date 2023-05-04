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


# 2 Create a table to store subscription status:
CREATE TABLE dim_subscription_status
(
    subscription_status_id INTEGER PRIMARY KEY,
    name                   VARCHAR(255) NOT NULL
);

-- Insert subscription status
INSERT INTO dim_subscription_status (subscription_status_id, name)
VALUES (1, 'Subscribed'),
       (2, 'Upgraded'),
       (3, 'Downgraded'),
       (4, 'Churned');

# 3 Create a table to store user data:
CREATE TABLE users
(
    user_id INTEGER PRIMARY KEY,
    -- Other user attributes like name, email, etc.
);


# 4 Create a table to store the user subscription history:
CREATE TABLE fact_user_subscription_history
(
    id                     SERIAL PRIMARY KEY,
    user_id                INTEGER   NOT NULL,
    subscription_plan_id   INTEGER   NOT NULL,
    subscription_status_id INTEGER   NOT NULL,
    event_timestamp        TIMESTAMP NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (subscription_plan_id) REFERENCES dim_subscription_plans (subscription_plan_id),
    FOREIGN KEY (subscription_status_id) REFERENCES dim_subscription_status (subscription_status_id)
);

# With this schema, we separate the dimensions (dim_subscription_plans and dim_subscription_status) from the fact table (fact_user_subscription_history).
# The fact table holds the historical records of user subscription events, with timestamps for each event.
# This allows for easier tracking and analysis of changes in user subscriptions over time.
# To update a user's subscription or set the status to "Churned," you would insert a new record into the fact_user_subscription_history table with the corresponding subscription status ID and a timestamp.