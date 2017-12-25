package pku;

import java.io.IOException;
import java.util.*;
import java.util.zip.DataFormatException;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {
    DemoMessageStore demoMessageStore = new DemoMessageStore();
    List<String> topics = new LinkedList<>();
    int readPos = 0;
    String queue;
    //记录每个topic的读位置
    HashMap<String,Integer> topic_readPos= new HashMap<>();
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
        for(String topic:t){
        	topic_readPos.put(topic, 0);
        }
    }

    public ByteMessage poll() throws IOException, DataFormatException {
        ByteMessage re = null;
        //当前topic的读位置
        int tp = topic_readPos.get(topics.get(readPos));
        re = demoMessageStore.pull(topics.get(readPos),tp);
        if(re==null){
            readPos++;
            if(readPos<topics.size()){
            	return poll();
            } 
            return null;
        }else {
        	//读到了，tp加1
        	topic_readPos.put(topics.get(readPos), tp + 1);
            return re;
        }

    }
}
