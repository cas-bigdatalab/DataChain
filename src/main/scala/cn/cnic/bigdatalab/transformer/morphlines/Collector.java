package cn.cnic.bigdatalab.transformer.morphlines;

import java.util.ArrayList;
import java.util.List;

import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.Record;
import com.google.common.base.Preconditions;

public class Collector implements Command {

    private final List<Record> results = new ArrayList();

    public List<Record> getRecords() {
        return results;
    }

    public void reset() {
        results.clear();
    }

    @Override
    public Command getParent() {
        return null;
    }

    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
        Preconditions.checkNotNull(record);
        results.add(record);
        return true;
    }

}

