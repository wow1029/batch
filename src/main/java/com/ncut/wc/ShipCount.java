package com.ncut.wc;

import com.ncut.wc.config.Constant;
import com.ncut.wc.entity.ResultData;
import com.ncut.wc.entity.ShipData;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;

public class ShipCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        String inputPath = "src/main/resources/0429.csv";
        DataStream<ShipData> ships = env.readTextFile(inputPath)
                .map(line -> {
                    try {
                        String[] fields = line.trim().split(",");
                        long mmsi = Long.parseLong(fields[0].trim().replace("\uFEFF", ""));
                        double lat = Double.parseDouble(fields[2].trim());
                        double lon = Double.parseDouble(fields[3].trim());
                        String timeStr = fields[4].trim();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm");
                        long timestamp = sdf.parse(timeStr).getTime();
                        return new ShipData(mmsi, lat, lon, timeStr, timestamp);
                    } catch (Exception e) {
                        System.err.println("跳过错误数据: " + line);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ShipData>forMonotonousTimestamps()
                                .withTimestampAssigner((ship, timestamp) -> ship.timestamp)
                );

        DataStream<ResultData> resultStream = ships
                .keyBy(ship -> ship.mmsi)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<ShipData, ResultData, Long, TimeWindow>() {
                    @Override
                    public void process(Long mmsi, Context context, Iterable<ShipData> elements, Collector<ResultData> out) {
                        Map<Long, Long> inList = new HashMap<>();
                        List<Long> resultList = new ArrayList<>();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm");
                        String windowStart = sdf.format(new Date(context.window().getStart()));

                        List<ShipData> sortedElements = new ArrayList<>();
                        elements.forEach(sortedElements::add);
                        sortedElements.sort(Comparator.comparingLong(ship -> ship.timestamp));

                        for (ShipData ship : sortedElements) {
                            boolean inRegion = ship.lat >= Constant.MIN_LAT && ship.lat <= Constant.MAX_LAT
                                    && ship.lon >= Constant.MIN_LON && ship.lon <= Constant.MAX_LON;

                            if (inRegion && !inList.containsKey(ship.mmsi)) {
                                inList.put(ship.mmsi, ship.timestamp);
                                String enterTimeStr = sdf.format(new Date(ship.timestamp));
                            }
                            else if (!inRegion && inList.containsKey(ship.mmsi)) {

                                long enterTime = inList.remove(ship.mmsi);
                                String enterTimeStr = sdf.format(new Date(enterTime));
                                String exitTimeStr = sdf.format(new Date(ship.timestamp));
                                resultList.add(ship.mmsi);

                                out.collect(new ResultData(ship.mmsi, windowStart,
                                        Constant.MIN_LAT, Constant.MAX_LAT, Constant.MIN_LON, Constant.MAX_LON));
                            }
                        }
                    }


                });
        resultStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO ais (mmsi, windowStart, latMin, latMax, lonMin, lonMax) VALUES (?, ?, ?, ?, ?, ?)",
                        (ps, data) -> {
                            ps.setLong(1, data.mmsi);
                            ps.setString(2, data.windowStart);
                            ps.setDouble(3, data.latMin);
                            ps.setDouble(4, data.latMax);
                            ps.setDouble(5, data.lonMin);
                            ps.setDouble(6, data.lonMax);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/ais_data?serverTimezone=UTC&useSSL=false")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("wjsoyc1029.")
                                .build()
                )
        );
        env.execute("Flink Batch Process from CSV");
    }
}
