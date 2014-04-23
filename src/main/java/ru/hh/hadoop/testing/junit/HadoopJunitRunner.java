package ru.hh.hadoop.testing.junit;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import java.io.IOException;

public class HadoopJunitRunner extends BlockJUnit4ClassRunner {
  private HadoopTestContext testContext;

  /**
   * Creates a HadoopJunitRunner to run {@code klass}
   *
   * @throws  org.junit.runners.model.InitializationError  if the test class is malformed.
   */
  public HadoopJunitRunner(Class<?> klass) throws InitializationError, IOException {
    super(klass);
    if (testContext == null) {
      testContext = new HadoopTestContext();
// try {
      testContext.setUpCluster();
// } catch (IOException e) {
// throw new InitializationError(e);
// }
    }
  }

  @Override
  protected Statement withAfterClasses(Statement statement) {
    Statement junitAfterClasses = super.withAfterClasses(statement);
    return new RunAfterTestClassCallbacks(junitAfterClasses, testContext);
  }
}
