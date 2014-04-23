package ru.hh.hadoop.testing.junit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HadoopTestContext {
  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRCluster mrCluster = null;

  public void setUpCluster() throws IOException {
    Properties props = new Properties();
    props.put("dfs.datanode.data.dir.perm", "775");
    props.put("hadoop.job.history.location", "/tmp");
    System.setProperty("hadoop.log.dir", "/tmp");
    startCluster(true, props);
  }

  protected synchronized void startCluster(boolean reformatDFS, Properties props) throws IOException {
    if (dfsCluster == null) {
      JobConf conf = createInitialConf(props);
      dfsCluster = new MiniDFSCluster(conf, 2, reformatDFS, null);

      ConfigurableMiniMRCluster.setConfiguration(props);
      // noinspection deprecation
      mrCluster = new ConfigurableMiniMRCluster(2, getFileSystem().getName(), 1, conf);
    }
  }

  private JobConf createInitialConf(Properties props) {
    JobConf conf = new JobConf();
    if (props != null) {
      for (Map.Entry entry : props.entrySet()) {
        conf.set((String) entry.getKey(), (String) entry.getValue());
      }
    }
    return conf;
  }

  public void stopCluster() throws Exception {
    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  public void afterTestClass() throws IOException {
    reformatCluster();
  }

  public void reformatCluster() throws IOException {
    dfsCluster.formatDataNodeDirs();
    NameNode.format(createInitialConf(new Properties()));
  }

  public static FileSystem getFileSystem() throws IOException {
    return dfsCluster.getFileSystem();
  }

  private static class ConfigurableMiniMRCluster extends MiniMRCluster {
    private static Properties config;

    public ConfigurableMiniMRCluster(int numTaskTrackers, String namenode, int numDir, JobConf conf) throws IOException {
      super(0, 0, numTaskTrackers, namenode, numDir, null, null, null, conf);
    }

    public static void setConfiguration(Properties props) {
      config = props;
    }

    public JobConf createJobConf() {
      JobConf conf = super.createJobConf();
      if (config != null) {
        for (Map.Entry entry : config.entrySet()) {
          conf.set((String) entry.getKey(), (String) entry.getValue());
        }
      }
      return conf;
    }
  }
}
