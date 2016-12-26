package com.example.kafkatohdfs;

import com.datatorrent.api.*;
import com.datatorrent.api.Operator.*;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.dedup.DeduperStreamCodec;
import org.apache.apex.shaded.ning19.com.ning.http.client.multipart.Part;

import java.util.*;

/**
 * Created by vikram on 22/12/16.
 */
public class DedupOperator extends BaseOperator implements ActivationListener,Partitioner<DedupOperator> ,StatsListener {

    public transient Class pojoClazz;


    public HashSet uniqueSet = new HashSet();

    public transient PojoUtils.Getter getter = null;

    private String keyExpr = null;
    private transient  StreamCodec inputCodec;


    public final transient DefaultOutputPort<Object> unique = new DefaultOutputPort();
    public final transient DefaultOutputPort<Object> duplicate = new DefaultOutputPort();


    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

        @Override
        public void setup(Context.PortContext context) {
            System.out.println(context.getAttributes().get(Context.PortContext.TUPLE_CLASS));
            pojoClazz = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
            inputCodec = new DeduperStreamCodec(keyExpr);
        }


        @Override
        public void process(Object inputObject) {
            if (!checkAndUpdate(inputObject)) {
                duplicate.emit(inputObject);
            } else {
                unique.emit(inputObject);
            }
        }


        @Override
        public StreamCodec<Object> getStreamCodec(){
            return inputCodec;
        }

    };


    public boolean checkAndUpdate(Object o) {
        if (uniqueSet.contains(getter.get(o))) {
            System.out.println("DUPLICATE RECEIVED"+getter.get(o));

            return false;
        }
        System.out.println("UNIQUE RECEIVED"+getter.get(o));

        return uniqueSet.add(getter.get(o));

    }

    @Override
    public void activate(Context context) {
        System.out.println("CREATING GETTER "+keyExpr+ " "+pojoClazz);

        getter = PojoUtils.createGetter(pojoClazz, keyExpr, Object.class);
        System.out.println("CREATED GETTER");
    }

    @Override
    public void deactivate() {

    }


    public String getKeyExpr() {
        return keyExpr;
    }

    public void setKeyExpr(String keyExpr) {
        this.keyExpr = keyExpr;
    }


    public int getRequiredPartions() {
        return requiredPartions;
    }

    public void setRequiredPartions(int requiredPartions) {
        this.requiredPartions = requiredPartions;
    }

    public int getActualPartitions() {
        return actualPartitions;
    }

    public void setActualPartitions(int actualPartitions) {
        this.actualPartitions = actualPartitions;
    }

    int requiredPartions=1,actualPartitions=1;



    public HashSet getUniqueSet() {
        return uniqueSet;
    }

    public void setUniqueSet(HashSet uniqueSet) {
        this.uniqueSet = uniqueSet;
    }



    @Override
    public Collection<Partition<DedupOperator>> definePartitions(Collection<Partition<DedupOperator>> partitions, PartitioningContext context) {
        if( partitions.size() == requiredPartions){
            return partitions;
        }

        List<Partition<DedupOperator>> newPartitions = new ArrayList<Partition<DedupOperator>>(requiredPartions);
        // Create combined states from all existing operators;

        HashSet<Object> combinedSet = new HashSet<>();
        for( Partition<DedupOperator> existingPartition: partitions){
            DedupOperator dedup = existingPartition.getPartitionedInstance();
            combinedSet.addAll(dedup.getUniqueSet());
        }
        for( int i=0;i<requiredPartions;i++){
            DedupOperator dedup = new DedupOperator();
            dedup.setKeyExpr(keyExpr);
            dedup.setUniqueSet(combinedSet);
            Partition<DedupOperator> part = new DefaultPartition<DedupOperator>(dedup);
            newPartitions.add(part);
        }

        DefaultPartition.assignPartitionKeys(newPartitions, input);
        return newPartitions;
    }

    @Override
    public void partitioned(Map<Integer, Partition<DedupOperator>> partitions) {
        actualPartitions = partitions.size();
    }

    @Override
    public Response processStats(BatchedOperatorStats stats) {
        if (getRequiredPartions() != actualPartitions) {
            Response resp = new Response();
            resp.repartitionRequired = true;
            return resp;
        }
        return null;

    }
}
