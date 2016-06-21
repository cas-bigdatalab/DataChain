/**
 * liliang@cnic.cn
 * TransformInterceptor for the parser of format such as json, csv, regex logs.
 */

package org.apache.flume.interceptor;

import com.github.casbigdatalab.datachain.transformer.transformer;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flume.interceptor.TransformInterceptor.Constants.MAPPING_FILE;

public class TransformInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory
            .getLogger(StaticInterceptor.class);

      private  final transformer transformer;

    /**
     * Only {@link TransformInterceptor.Builder} can build me
     */
    private TransformInterceptor(transformer trans) {
        this.transformer = trans;
    }

    @Override
    public void initialize() {
        // no-op
    }

    @Override
    /**
     * Returns the event where the body carries parsed results.
     */
    public Event intercept(Event event) {
        // We've already ensured here that at most one of Transformers.
        ArrayList parseout = transformer.transform(new String(event.getBody()));
        String eventmsg = new String();
        for(int i = 0; i < parseout.size(); i++) {
            eventmsg = eventmsg + parseout.get(i) + " ";
        }
        event.setBody(eventmsg.getBytes());
        //System.out.println(new String(event.getBody()));
        return event;
    }

    /**
     * Returns the set of events, according to
     * {@link #intercept(Event)}.
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> out = Lists.newArrayList();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if (outEvent != null) { out.add(outEvent); }
        }
        return out;
    }

    @Override
    public void close() {
        // no-op
    }

    /**
     * Builder which builds new instance of the StaticInterceptor.
     */
    public static class Builder implements Interceptor.Builder {

        private String mappingFile = null;

        @Override
        public void configure(Context context) {
            mappingFile = context.getString(MAPPING_FILE);
        }

        @Override
        public Interceptor build() {
            logger.info(String.format(
                    "Creating TransformInterceptor"));
            System.out.println("mammping_file " + mappingFile);
            if(mappingFile == null) {
                throw new IllegalArgumentException( "mapping_file(" + mappingFile + ") now is null, please set mappingFile! for TransformInterceptor");
            }
            transformer parser = new transformer(mappingFile);
            return new TransformInterceptor(parser);
        }
    }

    public static class Constants {
        public static final String MAPPING_FILE = "mappingFile";
    }
}