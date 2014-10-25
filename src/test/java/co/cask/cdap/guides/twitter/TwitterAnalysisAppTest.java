/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.guides.twitter;

import co.cask.cdap.packs.twitter.Tweet;
import co.cask.cdap.packs.twitter.TweetCollectorFlowlet;
import co.cask.cdap.packs.twitter.TweetCollectorTestUtil;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ProcedureManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.TestBase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TwitterAnalysisAppTest extends TestBase {

  @Test
  public void testSentimentProcedure() throws Exception {
    ApplicationManager appManager = deployApplication(TwitterAnalysisApp.class);

    File srcFile = TweetCollectorTestUtil.writeToTempFile(ImmutableList.of(
      new Tweet("123456789", 1000),
      new Tweet("123", 2000),
      new Tweet("123456", 3000)
    ).iterator());

    ApplicationManager applicationManager = deployApplication(TwitterAnalysisApp.class);
    applicationManager.startFlow(AnalysisFlow.NAME,
                                 ImmutableMap.of(TweetCollectorFlowlet.ARG_TWITTER4J_DISABLED, "true",
                                                 TweetCollectorFlowlet.ARG_SOURCE_FILE, srcFile.getPath()));

    RuntimeMetrics countMetrics = RuntimeStats.getFlowletMetrics(TwitterAnalysisApp.NAME,
                                                                 AnalysisFlow.NAME,
                                                                 "recordStats");

    countMetrics.waitForProcessed(3, 3, TimeUnit.SECONDS);

    // Start procedure and verify
    ProcedureManager procedureManager = appManager.startProcedure(StatsProcedure.class.getSimpleName());
    String response = procedureManager.getClient().query("avgSize", Collections.<String, String>emptyMap());

    Assert.assertEquals("6", response);
  }
}
