from py4j.java_gateway import java_import
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, MemoryStateBackend
from pyflink.table import DataTypes, StreamTableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime
from pyflink.java_gateway import get_gateway

# gateway = get_gateway()
# java_import(gateway.jvm, "org.apache.flink.table.api.*")

exec_env = StreamExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
exec_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)  # 事件时间
exec_env.set_state_backend(MemoryStateBackend())  # 内存级状态存在


environment_settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()

table_env = StreamTableEnvironment.create(exec_env, environment_settings=environment_settings)

gateway = get_gateway()
java_list = gateway.jvm.java.util.ArrayList()
java_list.append(214)
print(java_list)

table_env \
    .connect(Kafka()
             .topic('test_input')
             .start_from_latest()
             .property("zookeeper.connect", "localhost:2181")
             .property("bootstrap.servers", "localhost:9092")
             .property("group.id", "g1")
             .property("client.id", "c1")
             ) \
    .with_format(Json().json_schema("{"
                                    "  type: 'object',"
                                    "  properties: {"
                                    "    name: {"
                                    "      type: 'string'"
                                    "    },"
                                    "    value: {"
                                    "      type: 'number'"
                                    "    },"
                                    "    timestamp: {"
                                    "      type: 'string',"
                                    "      format: 'date-time'"
                                    "    }"
                                    "  }"
                                    "}"
                                    ).fail_on_missing_field(True)) \
    .with_schema(Schema()
                 .field("rowtime", DataTypes.TIMESTAMP()).rowtime(
    Rowtime().timestamps_from_field("timestamp").watermarks_periodic_ascending())
                 .field("name", DataTypes.STRING())
                 .field("value", DataTypes.DOUBLE())) \
    .in_append_mode() \
    .register_table_sink('table_point_name_1')

# sql
revenue = table_env.sql_query("SELECT * from  table_point_name_1").insert_into()

# kafka sink
table_env \
    .connect(Kafka()
             .version('0.10')
             .topic('test_output')
             .property("zookeeper.connect", "localhost:2181")
             .property("bootstrap.servers", "localhost:9092")
             .property("client.id", "c1")
             .sink_partitioner_fixed()
             ).register_table_source("table_point_name_1")

table_env.execute('test')
