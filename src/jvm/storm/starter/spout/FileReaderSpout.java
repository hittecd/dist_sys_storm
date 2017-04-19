package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by hitte on 4/18/17.
 */
public class FileReaderSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    String _filename;

    public FileReaderSpout(String filename) {
        this._filename = filename;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        //Get file from resources folder
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(_filename).getFile());

        try {
            Scanner scanner = new Scanner(file);

            while (scanner.hasNextLine()) {
                String sentence = scanner.nextLine();

                sentence = sentence.replaceAll("[^A-Za-z]", "").toLowerCase();

                _collector.emit(new Values(sentence));
            }

            scanner.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
