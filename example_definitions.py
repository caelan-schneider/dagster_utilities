from dagster import *
from dagster._core.definitions.partitioned_schedule import UnresolvedPartitionedAssetScheduleDefinition, UnresolvedAssetJobDefinition
from import_utils import get_all_instances
from resources import DatabaseResource, MyIOManager


defs = Definitions(
    # assumes some src directory exists
    assets=get_all_instances(root="src", dtypes=[AssetsDefinition]),
    schedules=get_all_instances(root="src", dtypes=[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]),
    sensors=get_all_instances(root="src", dtypes=[SensorDefinition, RunStatusSensorDefinition, AssetSensorDefinition, MultiAssetSensorDefinition, FreshnessPolicySensorDefinition]),
    jobs=get_all_instances(root="src", dtypes=[JobDefinition, UnresolvedAssetJobDefinition]),
    resources={
      "my_database_resource": DatabaseResource(...),
      "my_io_manager": MyIOManager(...),
    }
)
