/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.sakserv.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SequenceFileReader {
    
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileReader.class);
    
    public static void main(String[] args) {

        String inputFile = args[0];
        
        Configuration conf = new Configuration();
        try {

            Path seqFilePath = new Path(inputFile);

            SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(seqFilePath));

            Text key = new Text();
            IntWritable val = new IntWritable();

            while (reader.next(key, val)) {
                LOG.info("Sequence File Data: Key: " + key + "\tValue: " + val);
            }

            reader.close();
        } catch(IOException e) {
            LOG.error("ERROR: Could not load hadoop configuration");
            e.printStackTrace();
        }
        
    }
}
