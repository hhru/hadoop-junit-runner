package ru.hh.hadoop.testing.junit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.Assert.assertTrue;
import static ru.hh.hadoop.testing.junit.HadoopTestContext.getFileSystem;

public class HadoopTestUtils {
  private HadoopTestUtils() {
    // util class
  }

  public static String getHdfsUrl() throws IOException {
    return getFileSystem().getUri().toString();
  }

  public static List<String> getOutputLines(File outputFile) throws IOException {
    if (outputFile.isFile() && !outputFile.getName().endsWith("crc")) {
      return FileUtils.readLines(outputFile);
    }

    return Collections.emptyList();
  }

  public static void prepareAndCopyToCluster(String testFixtureFile, String hdfsInputFile) throws IOException {
    String inputLocal = new File(testFixtureFile).getAbsolutePath();

    getFileSystem().copyFromLocalFile(new Path(inputLocal), new Path(hdfsInputFile));
  }

  public static void copyOutputFromCluster(String hdfsFile, String localOutputName) throws IOException {
    Path hdfsOutput = new Path(hdfsFile);
    assertTrue(getFileSystem().exists(hdfsOutput));

    if (getFileSystem().isFile(hdfsOutput)) {
      getFileSystem().copyToLocalFile(hdfsOutput, new Path(localOutputName));
    } else {
      FileStatus[] hdfsOutputFileStatuses = getFileSystem().listStatus(hdfsOutput);
      for (FileStatus fileStatus : hdfsOutputFileStatuses) {
        if (!fileStatus.isDir()) {
          getFileSystem().copyToLocalFile(fileStatus.getPath(), new Path(localOutputName + "/" + fileStatus.getPath().getName()));
        }
      }
    }
  }

  public static List<String> getResultsFromCluster(String inputFileName, String hdfsFileName, boolean shouldCleanLocalResults) throws IOException {
    String localOutputName = "out-" + inputFileName;
    copyOutputFromCluster(hdfsFileName, localOutputName);

    List<String> actualResult = new ArrayList<String>();

    File localOutputFile = new File(localOutputName);
    if (localOutputFile.isFile()) {
      actualResult.addAll(getOutputLines(localOutputFile));
    } else {
      for (File outputFile : localOutputFile.listFiles()) {
        actualResult.addAll(getOutputLines(outputFile));
      }
    }

    if (shouldCleanLocalResults) {
      localOutputFile.deleteOnExit();
    }

    return actualResult;
  }
}
