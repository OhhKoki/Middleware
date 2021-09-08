package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import java.util.Properties;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 2. 获取 CK（我还没安装 HDFS，所以此处注释）
//        System.setProperty("HADOOP_USER_NAME", "alibaba" );    // 权限设置
//        environment.enableCheckpointing(5000L);  // 设置间隔,多久开启一次 checkpoint.
//        environment.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink1109/ck"));

        // 3. 获取 mysql cdc source
        Properties properties = new Properties();
        properties.setProperty("scan.startup.mode", "initial");
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("1.15.115.151")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("cdcservice")    // 监控的database
                .tableList("cdcservice.stuinfo", "cdcservice.stuinfo_bar")    // 监控的table
                .deserializer(new customDebeziumDS())    // 自定义返回数据格式
                .debeziumProperties(properties)
                .build();

        // 4. 获取 mysql 数据
        DataStreamSource<String> streamSource = environment.addSource(sourceFunction);

        // 5. 算子操作（在这里进行业务处理）
        BusinessProcess businessProcess = new BusinessProcess();
        streamSource.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String dataStr) throws Exception {
                // 进行业务处理
                businessProcess.processing(dataStr);
                return null;
            }
        });

        // 6. 打印
        streamSource.print();

        // 7. 执行任务
        environment.execute();

    }

}