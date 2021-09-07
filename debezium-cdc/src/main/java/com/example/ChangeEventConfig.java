package com.example;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;

import java.util.concurrent.*;

/**
 * @author lei
 * @create 2021-06-22 15:36
 * @desc sql server 实时同步
 **/
@Configuration
@Log4j2
public class ChangeEventConfig {

    private final ChangeEventHandler changeEventHandler;

    @Autowired
    public ChangeEventConfig(ChangeEventHandler changeEventHandler) {
        this.changeEventHandler = changeEventHandler;
    }

    /**
     * Debezium 配置
     */
    @Bean
    io.debezium.config.Configuration debeziumConfig() {
        return io.debezium.config.Configuration.create()
            // 连接器的Java类名称
            .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
            // 偏移量持久化，用来容错 默认值
            .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            // 偏移量持久化文件路径 默认 /tmp/offsets.dat，如果路径配置不正确可能导致无法存储偏移量，可能会导致重复消费变更
            // 如果连接器重新启动，它将使用最后记录的偏移量来知道它应该恢复读取源信息中的哪个位置。
            .with("offset.storage.file.filename", "C://MySolfware/tmp/offsets.dat")
            // 捕获偏移量的周期
            .with("offset.flush.interval.ms", "6000")
            // 连接器的唯一名称
            .with("name", "mysql-connector")
            // 数据库的hostname
            .with("database.hostname", "1.15.115.151")
            // 端口
            .with("database.port", "3306")
            // 用户名
            .with("database.user", "root")
            // 密码
            .with("database.password", "123456")
            // mysql.cnf 配置的 server-id
            .with("database.server.id", "1")
            // MySQL 服务器或集群的逻辑名称
            .with("database.server.name", "cdcservice")
            // 监控的数据库
            .with("database.include.list", "cdcservice")
            // 监控的表
            .with("table.whitelist", "cdcservice.stuinfo, cdcservice.stuinfo_bar")
            // 是否包含数据库表结构层面的变更，建议使用默认值true
            .with("include.schema.changes", "false")
            // test
            .with("snapshot.lock.timeout.ms", "-1")
            // 历史变更记录
            .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
            // 历史变更记录存储位置，存储DDL
            .with("database.history.file.filename", "C://MySolfware/tmp/dbhistory.dat")
            .build();
    }

    /**
     * 实时同步服务类，执行任务
     * @return
     */
    @Bean
    TimelyExecutor timelyExecutor(io.debezium.config.Configuration configuration) {
        TimelyExecutor timelyExecutor = new TimelyExecutor();
        DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine = DebeziumEngine
                .create(ChangeEventFormat.of(Connect.class))
                .using(configuration.asProperties())
                .notifying(changeEventHandler::handlePayload)
                .build();
        timelyExecutor.setDebeziumEngine(debeziumEngine);
        return timelyExecutor;
    }

    /**
     * 同步执行服务类
     */
    @Data
    @Log4j2
    public static class TimelyExecutor implements InitializingBean, SmartLifecycle {

        private final ExecutorService executor = ThreadPoolEnum.INSTANCE.getInstance();
        private DebeziumEngine<?> debeziumEngine;

        @Override
        public void start() {
            log.warn(ThreadPoolEnum.SQL_SERVER_LISTENER_POOL + "线程池开始执行 debeziumEngine 实时监听任务!");
            executor.execute(debeziumEngine);
        }

        @SneakyThrows
        @Override
        public void stop() {
            log.warn("debeziumEngine 监听实例关闭!");
            debeziumEngine.close();
            Thread.sleep(2000);
            log.warn(ThreadPoolEnum.SQL_SERVER_LISTENER_POOL + "线程池关闭!");
            executor.shutdown();
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public void afterPropertiesSet() {
            Assert.notNull(debeziumEngine, "DebeZiumEngine 不能为空!");
        }

        public enum ThreadPoolEnum {
            /**
             * 实例
             */
            INSTANCE;

            public static final String SQL_SERVER_LISTENER_POOL = "mysql-server-listener-pool";

            /**
             * 线程池单例
             */
            private final ExecutorService es;

            /**
             * 枚举 (构造器默认为私有）
             */
            ThreadPoolEnum() {
                final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(SQL_SERVER_LISTENER_POOL + "-%d").build();
                es = new ThreadPoolExecutor(8, 16, 60,
                        TimeUnit.SECONDS, new ArrayBlockingQueue<>(256),
                        threadFactory, new ThreadPoolExecutor.DiscardPolicy());
            }

            /**
             * 公有方法
             *
             * @return ExecutorService
             */
            public ExecutorService getInstance() {
                return es;
            }
        }
    }

}