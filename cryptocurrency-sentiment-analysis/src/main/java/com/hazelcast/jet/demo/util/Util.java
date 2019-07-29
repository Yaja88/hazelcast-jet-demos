package com.hazelcast.jet.demo.util;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.demo.common.CoinDefs;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.demo.common.CoinDefs.COIN_MAP;
import static com.hazelcast.jet.demo.common.CoinDefs.SYMBOL;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Contains utility methods to load properties files and starting/stopping console printer threads
 */
public class Util {

    public static final String MAP_NAME_30_SECONDS = "map30Seconds";
    public static final String MAP_NAME_1_MINUTE = "map1Min";
    public static final String MAP_NAME_5_MINUTE = "map5Min";

    private static volatile boolean running = true;
    private static final long PRINT_INTERNAL_MILLIS = 10_000L;

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";

    private Connection conn;

    public void connect() {
            Properties properties = new Properties();
            String propertiesFileName = "twitter-security.properties";
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propertiesFileName);
            if(inputStream != null) {
                try{
                    properties.load(inputStream);
                    this.conn = DriverManager.getConnection(properties.getProperty("postgres_url"));
                } catch (Exception ex) {
                    System.out.println(ex.toString());
                }
            }
    }

    public void close() {
        if (this.conn != null) {
            try {
                conn.close();
            } catch (Exception ex) {
                System.out.print(ex);
            }
        }
    }

    public static void startConsolePrinterThreadPrint(JetInstance jet) {
        new Thread(() -> {
            Map<String, Tuple2<Double, Long>> map30secs = jet.getMap(MAP_NAME_30_SECONDS);
            Map<String, Tuple2<Double, Long>> map1min = jet.getMap(MAP_NAME_1_MINUTE);
            Map<String, Tuple2<Double, Long>> map5min = jet.getMap(MAP_NAME_5_MINUTE);

            while (running) {
                Set<String> coins = new HashSet<>();

                if (map30secs.isEmpty()) {
                    continue;
                }

                coins.addAll(map30secs.keySet());
                coins.addAll(map1min.keySet());
                coins.addAll(map5min.keySet());

                System.out.println("\n");
                System.out.println("/----------------+---------------+---------------+----------------\\");
                System.out.println("|                |          Sentiment (tweet count)               |");
                System.out.println("| Coin           | Last 30 sec   | Last minute   | Last 5 minutes |");
                System.out.println("|----------------+---------------+---------------+----------------|");
                coins.forEach((coin) ->
                        System.out.format("| %s  | %s | %s | %s  |%n",
                            coinName(coin), format(map30secs.get(coin)), format(map1min.get(coin)), format(map5min.get(coin))));
                System.out.println("\\----------------+---------------+---------------+----------------/");

                LockSupport.parkNanos(MILLISECONDS.toNanos(PRINT_INTERNAL_MILLIS));
            }
        }).start();
    }

    public  void startConsolePrinterThread(JetInstance jet) {
        new Thread(() -> {
            Map<String, Tuple2<Double, Long>> map30secs = jet.getMap(MAP_NAME_30_SECONDS);
            Map<String, Tuple2<Double, Long>> map1min = jet.getMap(MAP_NAME_1_MINUTE);
            Map<String, Tuple2<Double, Long>> map5min = jet.getMap(MAP_NAME_5_MINUTE);

            while (running) {
                long currentUTCEpochTime = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond() * 1000;

                Set<String> coins = new HashSet<>();

                if (map30secs.isEmpty()) {
                    continue;
                }

                coins.addAll(map30secs.keySet());
                coins.addAll(map1min.keySet());
                coins.addAll(map5min.keySet());

                String insertStatement = "INSERT INTO %s (timestamp,symbol,seconds30,seconds60,seconds300) VALUES %s";
                StringBuilder values = new StringBuilder();
                for(String coin : coins) {
                    values.append(String.format("(%d,%s,%f,%f,%f),",
                           currentUTCEpochTime, "'"+coin+"'", map30secs.get(coin).f0(), map1min.get(coin).f0(), map5min.get(coin).f0()));
                }
                values.deleteCharAt(values.length()-1);
                try {
                    Statement statement = conn.createStatement();
                    String insertion = String.format(insertStatement, "sentiment_twitter", values.toString());
                    System.out.println(currentUTCEpochTime);
                    statement.executeUpdate(insertion);
                } catch (Exception ex) {
                    System.out.println(ex.toString());
                }
                LockSupport.parkNanos(MILLISECONDS.toNanos(PRINT_INTERNAL_MILLIS));
            }
        }).start();
    }

    /**
     * Gets the full name given the code and pads it to a length of 16 characters
     */
    private static Object coinName(String coin) {
        String name = COIN_MAP.get(coin).get(SYMBOL);
        for (int i = name.length(); i < 13; i++){
            name = name.concat(" ");
        }
        return name;
    }

    public void stopConsolePrinterThread() {
        running = false;
    }

    public static boolean isMissing(String test) {
        return test.isEmpty() || "REPLACE_THIS".equals(test);
    }

    public static List<String> loadTerms() {
        List<String> terms = new ArrayList<>();
        COIN_MAP.forEach((key, value) -> {
            terms.add(key);
            terms.addAll(value);
        });
        return terms;
    }

    public static Properties loadProperties() {
        Properties tokens = new Properties();
        try {
            tokens.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("twitter-security.properties"));
        } catch (IOException e) {
            throw rethrow(e);
        }
        return tokens;
    }

    private static String format(Tuple2<Double, Long> t) {
        if (t == null || t.f1() == 0) {
            return "             ";
        }
        String color = t.f0() > 0 ? ANSI_GREEN : t.f0() == 0 ? ANSI_YELLOW : ANSI_RED;
        return String.format("%s%7.4f (%3d)%s", color, t.f0(), t.f1(), ANSI_RESET);
    }

}
