import os

from py4j.java_gateway import java_import
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.java_gateway import get_gateway
from pyflink.table import StreamTableEnvironment, CsvTableSink, DataTypes, EnvironmentSettings
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka
from pyflink.table.window import Tumble

if __name__ == '__main__':

    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    st_env = StreamTableEnvironment.create(s_env,
                                           environment_settings=EnvironmentSettings.new_instance().use_blink_planner().build())

    result_file = "tumble_time_window_streaming.csv"
    if os.path.exists(result_file):
        os.remove(result_file)

    # 输入
    st_env.connect(
        Kafka()
            .version("0.11")
            .topic("user")
            .start_from_earliest()
            .property("zookeeper.connect", "localhost:2181")
            .property("bootstrap.servers", "localhost:9092")
    ).with_format(
        Json()
            .fail_on_missing_field(True)
            .json_schema(
            "{"
            "  type: 'object',"
            "  properties: {"
            "    a: {"
            "      type: 'string'"
            "    },"
            "    b: {"
            "      type: 'number'"
            "    },"
            "    c: {"
            "      type: 'number'"
            "    },"
            "    time: {"
            "      type: 'string',"
            "      format: 'date-time'"
            "    }"
            "  }"
            "}"
        )
    ).with_schema(
        Schema()
            .field("rowtime", DataTypes.TIMESTAMP())
            .rowtime(
            Rowtime()
                .timestamps_from_field("time")
                .watermarks_periodic_bounded(10000))
            .field("a", DataTypes.STRING())
            .field("b", DataTypes.DECIMAL(precision=38, scale=0))
            .field("c", DataTypes.DECIMAL(precision=38, scale=0))
    ).in_append_mode().register_table_source("source")

    # 输出
    st_env.register_table_sink("result",
                               CsvTableSink(["a", "b", "rowtime"],
                                            [
                                                DataTypes.STRING(),
                                                DataTypes.DECIMAL(precision=38, scale=0),
                                                DataTypes.TIMESTAMP(),
                                            ],
                                            result_file))

    # st_env.scan("source").window(Tumble.over("1.seconds").on("rowtime").alias('w')) \
    #     .group_by("w, a") \
    #     .select("a, max(b) as b, rowtime").insert_into("result")

    # st_env.scan("source").select("a, rowtime").insert_into("result1")

    # st_env.sql_query('select a, b  from source ').insert_into('result')
    tb = st_env.sql_query("select a,  b, TUMBLE_END(rowtime, INTERVAL '1' second) as rowtime  from source group by a, b, TUMBLE(rowtime, INTERVAL '1' second)")
    tb.print_schema()
    tb.insert_into("result")



    st_env.execute("tumble time window streaming")
