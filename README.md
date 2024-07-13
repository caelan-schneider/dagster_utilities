# dagster_utilities

This repo contains some generic utilities to solve problems I've come accross working with Dagster.
* a function to import Jobs, Schedules, and Sensors from across the entire repo into your definitions object (with an example definitions object)
* a decorator that prevents IO managers from writing to disk when not in production
* a custom ScheduleDefinition subclass that yields SkipReasons instead of RunRequests on holidays (assumes you have a resource to access said holidays)
* a custom RetryPolicy subclass that only retries on production runs (credit: https://discuss.dagster.io/t/15747766/is-there-a-way-i-can-override-or-turn-off-the-retry-policies)
