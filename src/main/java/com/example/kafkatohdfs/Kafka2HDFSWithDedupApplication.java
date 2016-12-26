package com.example.kafkatohdfs;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.filter.FilterOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.partitioner.StatelessThroughputBasedPartitioner;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Created by vikram on 23/12/16.
 */
@ApplicationAnnotation(name = "Kafka2HDFSWithDedup")
public class Kafka2HDFSWithDedupApplication implements StreamingApplication{
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

//        GenericFileOutputOperator.StringFileOutputOperator outputOperator = dag.addOperator("HDFSFileOutputOperator", new GenericFileOutputOperator.StringFileOutputOperator());
        FilterOperator filterOperator = dag.addOperator("FilterOperator", new FilterOperator());
        DedupOperator  dedupOperator = dag.addOperator("DedupOperator", new DedupOperator());
        ConsoleOutputOperator consoleOutputOperator = dag.addOperator("ConsoleOperator", new ConsoleOutputOperator());
        dag.addStream("dataIn", inputOperator.outputPort, csvParser.in).setLocality(DAG.Locality.THREAD_LOCAL);
        dag.addStream("filterIn", csvParser.out, filterOperator.input).setLocality(DAG.Locality.THREAD_LOCAL);
        dag.addStream("filterOut", filterOperator.truePort, dedupOperator.input);
        dag.addStream("dedupOutput",dedupOperator.unique,csvFormatter.in);
//        dag.addStream("dataOut", csvFormatter.out, outputOperator.input).setLocality(DAG.Locality.THREAD_LOCAL);
        dag.addStream(  "dataConsoleOut",csvFormatter.out,consoleOutputOperator.input);
        StatelessThroughputBasedPartitioner partitioner = new StatelessThroughputBasedPartitioner();
        partitioner.setInitialPartitionCount(1);
        partitioner.setMinimumEvents(10);
        partitioner.setMaximumEvents(100);
        partitioner.setCooldownMillis(30000);

        dag.setAttribute(dedupOperator, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));

    }
}
