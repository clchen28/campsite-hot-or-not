package io.github.ccincharge;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.shyiko.dotenv.DotEnv;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
import java.io.File;

/**
 *
 */
@SuppressWarnings("serial")
public class App {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        Map<String, String> dotEnv = DotEnv.load();

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // TODO: Don't hard code the filename
        // Get data from S3
        DataSource<String> rawDataSource = env.readTextFile(dotEnv.get("S3_RAW_DATA_BUCKET") + "2016-1.txt",
                "UTF-8");

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd HHmm");
        // Get all weather station locations from local file
        // Key is USAF|WBAN
        ObjectMapper mapper = new ObjectMapper();
        ClassLoader classLoader = App.class.getClassLoader();
        InputStream jsonFile = classLoader.getResourceAsStream("stations_latlon.json");
        HashMap<String, Location> stationLocations = mapper.readValue(jsonFile,
                new TypeReference<HashMap<String, Location>>(){});

        rawDataSource.map(new RawNOAAToTuple(stationLocations)).print();
        env.execute();

        /*
        // get input data
        DataSet<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            // get default test text data
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        // emit result
        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");
            // execute program
            env.execute("WordCount Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
        */

    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    public final static class RawNOAAToTuple implements MapFunction<String,
            Tuple3<Double, Double, Double>> {

        private static HashMap<String, Location> stationLocations;

        public RawNOAAToTuple(HashMap<String, Location> stations) throws Exception {
            stationLocations = stations;
        }

        private String parseUSAF(String rawData) {
            return rawData.substring(4, 10);
        }

        private String parseWBAN(String rawData) {
            return rawData.substring(10, 15);
        }

        /*
        private LocalDateTime parseTime(String rawData) {
            String rawDate = rawData.substring(15, 23) + " " + rawData.substring(23, 37);
            return LocalDateTime.parse(rawDate, this.formatter);
        }
        */

        private Double parseTemp(String rawData) {
            double temp = Float.parseFloat(rawData.substring(87, 92)) / 10.0;
            return temp;
        }

        private Location getStationLocation(String rawData) {
            String USAF = parseUSAF(rawData);
            String WBAN = parseWBAN(rawData);
            return stationLocations.get(USAF + "|" + WBAN);
        }

        public Tuple3<Double, Double, Double> map(String lineFromS3) {
            // LocalDateTime time = parseTime(lineFromS3);
            Location stationLocation = getStationLocation(lineFromS3);
            Double lat = stationLocation.lat;
            Double lon = stationLocation.lon;
            Double temp = parseTemp(lineFromS3);

            Tuple3<Double, Double, Double> output = new Tuple3<>();
            output.setFields(lat, lon, temp);
            return output;
        }
    }
}
