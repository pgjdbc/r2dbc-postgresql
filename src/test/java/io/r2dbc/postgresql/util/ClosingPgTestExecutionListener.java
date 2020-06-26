package io.r2dbc.postgresql.util;

import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class ClosingPgTestExecutionListener implements TestExecutionListener {

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        if (PostgresqlServerExtension.containerInstance != null) {
            PostgresqlServerExtension.containerInstance.stop();
        }
        if (PostgresqlServerExtension.containerNetwork != null) {
            PostgresqlServerExtension.containerNetwork.close();
        }
    }

}
