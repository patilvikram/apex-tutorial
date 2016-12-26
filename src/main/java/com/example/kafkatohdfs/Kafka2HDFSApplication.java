package com.example.kafkatohdfs;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.filter.FilterOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.*;
import org.apache.hadoop.conf.Configuration;


/**
 * Created by vikram on 21/12/16.
 */

@ApplicationAnnotation(name = "Kafka2Hdfs")
public class Kafka2HDFSApplication implements StreamingApplication {
    @Override
    public void populateDAG(DAG dag, Configuration conf) {
            /*
              Creating Kafka Input Operator
             *
             *
             * */

        KafkaSinglePortInputOperator inputOperator = dag.addOperator("KafkaInputOperator", new KafkaSinglePortInputOperator());
        CsvParser csvParser = dag.addOperator("CSVParser", new CsvParser());
        CsvFormatter csvFormatter = dag.addOperator("CSVFormatter", new CsvFormatter());
        StringFileOutputOperator outputOperator = dag.addOperator("HDFSFileOutputOperator", new StringFileOutputOperator());
        FilterOperator filterOperator = dag.addOperator("FilterOperator", new FilterOperator());

        dag.addStream("dataIn", inputOperator.outputPort, csvParser.in);
        dag.addStream("filterIn", csvParser.out, filterOperator.input);
        dag.addStream("filterOut", filterOperator.truePort, csvFormatter.in);
        dag.addStream("dataOut", csvFormatter.out, outputOperator.input);


    }
}
