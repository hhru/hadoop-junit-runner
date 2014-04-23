package ru.hh.hadoop.testing.junit;

import org.junit.internal.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import java.util.ArrayList;
import java.util.List;

public class RunAfterTestClassCallbacks extends Statement {
  private final Statement next;
  private final HadoopTestContext testContext;

  public RunAfterTestClassCallbacks(Statement next, HadoopTestContext testContext) {
    this.next = next;
    this.testContext = testContext;
  }

  @Override
  public void evaluate() throws Throwable {
    List<Throwable> errors = new ArrayList<Throwable>();
    try {
      this.next.evaluate();
    } catch (Throwable e) {
      errors.add(e);
    }

    try {
      testContext.reformatCluster();
    } catch (Exception e) {
      errors.add(e);
    }

    if (errors.isEmpty()) {
      return;
    }
    if (errors.size() == 1) {
      throw errors.get(0);
    }
    throw new MultipleFailureException(errors);
  }
}
