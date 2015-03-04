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

import com.github.sakserv.minicluster.config.ConfigVars;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class SequenceFileTest {
    
    HdfsLocalCluster hdfsLocalCluster;
    
    @Before
    public void setUp() {
        // Start HDFS
        hdfsLocalCluster = new HdfsLocalCluster.Builder()
                .setHdfsNamenodePort(12345)
                .setHdfsTempDir("embedded_hdfs")
                .setHdfsNumDatanodes(1)
                .setHdfsEnablePermissions(false)
                .setHdfsFormat(true)
                .setHdfsConfig(new Configuration())
                .build();
        hdfsLocalCluster.start();
    }
    
    @After
    public void tearDown() {
        hdfsLocalCluster.stop();
    }
    
    @Test
    public void testSequenceFile() throws IOException {
        Configuration conf = hdfsLocalCluster.getHdfsConfig();
        FileSystem fs = hdfsLocalCluster.getHdfsFileSystemHandle();
        

        Path seqFileDir = new Path("/tmp/seq_file_test");
        fs.mkdirs(seqFileDir);
        
        Path seqFilePath = new Path(seqFileDir + "/file.seq");
        
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(seqFilePath), SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(IntWritable.class));

        writer.append(new Text("key1"), new IntWritable(1));
        writer.append(new Text("key2"), new IntWritable(2));

        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(seqFilePath));

        Text key = new Text();
        IntWritable val = new IntWritable();

        while (reader.next(key, val)) {
            System.out.println("SEQFILE KEY: " + key + "\t" + val);
        }
        
        fs.mkdirs(new Path("/tmp/seq_file_test"));

        reader.close();
    }
}
