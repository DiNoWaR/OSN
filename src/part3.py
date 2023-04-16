"""
We can define a class Subscription that has the following properties:

plan - the subscription plan name, which can be one of Mobile, Basic, Standard, or Premium
price - the subscription plan price
status - the subscription status, which can be one of Subscribed, Upgraded, Downgraded, or Churned

We define a class User that has the following properties:

id - the unique identifier of the user
subscription - an instance of Subscription that represents the current subscription plan of the user
history - a list of instances of Subscription that represents the subscription plan changes of the user

The example of usage:

mobile_plan = Subscription('Mobile', 10, 'Subscribed')
basic_plan = Subscription('Basic', 20, 'Subscribed')
standard_plan = Subscription('Standard', 30, 'Subscribed')
premium_plan = Subscription('Premium', 40, 'Subscribed')

user1 = user(1, mobile_plan)
user1.upgrade_subscription(basic_plan)

user2 = user(2, premium_plan)
user2.downgrade_subscription(standard_plan)

user3 = user(3, basic_plan)
user3.churn_subscription()

"""

class Subscription:
    def __init__(self, plan, price, status):
        self.plan = plan
        self.price = price
        self.status = status


class User:
    def __init__(self, id, subscription):
        self.id = id
        self.subscription = subscription
        self.history = []

    def upgrade_subscription(self, new_subscription):
        if new_subscription.price > self.subscription.price:
            new_subscription.status = 'Upgraded'
            self.history.append(new_subscription)
            self.subscription = new_subscription
        else:
            raise ValueError('New subscription plan must have higher price')

    def downgrade_subscription(self, new_subscription):
        if new_subscription.price < self.subscription.price:
            new_subscription.status = 'Downgraded'
            self.history.append(new_subscription)
            self.subscription = new_subscription
        else:
            raise ValueError('New subscription plan must have lower price')

    def churn_subscription(self):
        self.subscription.status = 'Churned'
        self.history.append(self.subscription)
        self.subscription = None
