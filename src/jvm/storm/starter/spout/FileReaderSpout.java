package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by hitte on 4/18/17.
 */
public class FileReaderSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    String _filename;

    public FileReaderSpout(String filename) {
        super();
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

        try {
            InputStream i = classLoader.getResourceAsStream("kjv.txt");
            BufferedReader r = new BufferedReader(new InputStreamReader(i));


            // reads each line
            String sentence;
            while((sentence = r.readLine()) != null) {
                sentence = sentence.replaceAll("[^A-Za-z]", "").toLowerCase();

                _collector.emit(new Values(sentence));
            }

            i.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
