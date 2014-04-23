package ru.hh.hadoop.testing;

import org.junit.Test;
import org.junit.runner.RunWith;
import ru.hh.hadoop.testing.junit.HadoopJunitRunner;
import java.util.List;
import static org.junit.Assert.assertEquals;
import static ru.hh.hadoop.testing.junit.HadoopTestUtils.getHdfsUrl;
import static ru.hh.hadoop.testing.junit.HadoopTestUtils.getResultsFromCluster;
import static ru.hh.hadoop.testing.junit.HadoopTestUtils.prepareAndCopyToCluster;

@RunWith(HadoopJunitRunner.class)
public class HadoopJunitRunnerTest {
  @Test
  public void testUploadAndDownloadFile() throws Exception {
    String testFile = "fixture/test.txt";

    String hdfsInputFile = getHdfsUrl() + "/" + testFile;
    prepareAndCopyToCluster(testFile, hdfsInputFile);

    List<String> actualResult = getResultsFromCluster(testFile, getHdfsUrl() + "/" + testFile, true);

    assertEquals(3, actualResult.size());
    assertEquals("string 1", actualResult.get(0));
    assertEquals("string 2", actualResult.get(1));
    assertEquals("string 3", actualResult.get(2));
  }
}
