from dagster import *
import os


DEPLOY_ENV = "DEPLOYMENT"
PROD_FLAG = "PRODUCTION"


def production_only(cls):
    """
    Decorator that prevents an IO Manager from writing to persistent storage when not in production.
    Example:

    @production_only
    class MyIOManager(ConfigurableIOManager):
      ...

      def load_input(context: InputContext):
        ...

      def handle_output(context: OutputContext, obj: Any):
        ...
      
    """
  
    original_handle_output = cls.handle_output

    def handle_output(self, context: OutputContext, obj: object):
        if os.getenv(DEPLOY_ENV) == PROD_FLAG:
            return original_handle_output(self, context, obj)
        else:
            context.log.warning(f"Data not written. Set environment variable {DEPLOY_ENV} to {PROD_FLAG} to persist.")

    cls.handle_output = handle_output
    return cls


class ProdOnlyRetryPolicy(RetryPolicy):
  """
  RetryPolicy that only works when in production. Use in place of RetryPolicy.
  """
    def __new__(cls,
            max_retries: int = 1,
            delay: Optional[check.Numeric] = None,
            backoff: Optional[Backoff] = None,
            jitter: Optional[Jitter] = None,
    ):
        return super().__new__(cls, max_retries, delay, backoff, jitter) if os.getenv(DEPLOY_ENV) == PROD_FLAG else None


class HolidayAwareSchedule(ScheduleDefinition):
  """
  ScheduleDefinition that doesn't run on holidays.
  """
    def __init__(**kwargs):
        def should_execute(context: ScheduleEvaluationContext):
            my_database_resource = context.resources.my_database_resource
            eval_date = context.scheduled_execution_time.date().isoformat()
            holiday = my_database_resource.execute(f"SELECT holiday FROM some_holiday_table WHERE date = {eval_date}")
            return holiday is not None

        kwargs["should_execute"] = should_execute
        kwargs["required_resource_keys"] = {"my_database_resource"} | kwargs.get("required_resource_keys", set())
        super().__init__(**kwargs)

    
