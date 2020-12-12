# hdfs-clean-partitions

Script to clean malformed partitions from HDFS. For each table selected 

## Execution and configuration file

To execute the script:

```shell
python clean_partitions.py /path/to/config_file.yml
```

The configuration file must be in YAML format. Here is an example configuration file:

```yaml
hive_hdfs_path: /apps/hive/warehouse # for Hive 1.2 is the default path

tables:
  - schema: schema_name
    tables: 
      - table_name1
      - table_2

    part_col: partition_column
    format: "%Y-%m-%d" # partition column format
  - schema: other_schema
    tables: 
      - table_on_other_schema

    part_col: partition
    format: "%Y%m%d%H"
```

With this configuration file the script will check the tables on the schemas:

```shell
/apps/hive/warehouse/schema_name.db/table_name1/partition_column=1970-12-12
...
/apps/hive/warehouse/other_schema.db/table_on_other_schema/partition=1970121220
```

## @TODO

- [ ] Use WebHDFS instead of CLI
- [ ] Support for multiple partition columns on table (granularity to inner partitions)