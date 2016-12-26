package com.example.kafkatohdfs;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.*;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.dedup.DeduperStreamCodec;

import java.util.HashSet;

/**
 * Created by vikram on 22/12/16.
 */
public class DedupOperator extends BaseOperator implements ActivationListener {

    public transient Class pojoClazz;

    public HashSet uniqueSet = new HashSet();

    public PojoUtils.Getter getter = null;

    private String keyExpr = null;
    private StreamCodec inputCodec;


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




}
