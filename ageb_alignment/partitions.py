from dagster import DynamicPartitionsDefinition


zone_partitions = DynamicPartitionsDefinition(name="zone")
