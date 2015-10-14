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

import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.packs.twitter.Tweet;
import co.cask.cdap.packs.twitter.TweetCollectorFlowlet;
import co.cask.cdap.packs.twitter.TweetCollectorTestUtil;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TwitterAnalysisAppTest extends TestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager appManager = deployApplication(TwitterAnalysisApp.class);

    File srcFile = TweetCollectorTestUtil.writeToTempFile(ImmutableList.of(
      new Tweet("123456789", 1000),
      new Tweet("123", 2000),
      new Tweet("123456", 3000)
    ).iterator());

    ApplicationManager applicationManager = deployApplication(TwitterAnalysisApp.class);
    FlowManager flowManager = applicationManager.getFlowManager(TweetAnalysisFlow.NAME).start(
      ImmutableMap.of(TweetCollectorFlowlet.ARG_TWITTER4J_DISABLED, "true",
                      TweetCollectorFlowlet.ARG_SOURCE_FILE, srcFile.getPath()));

    try {

      RuntimeMetrics countMetrics = flowManager.getFlowletMetrics("recordStats");
      countMetrics.waitForProcessed(3, 3, TimeUnit.SECONDS);

      // Start service and verify
      ServiceManager serviceManager = appManager.getServiceManager(TwitterAnalysisApp.SERVICE_NAME).start();
      serviceManager.waitForStatus(true);
      try {
        URL serviceUrl = serviceManager.getServiceURL();

        URL url = new URL(serviceUrl, "v1/avgSize");
        HttpRequest request = HttpRequest.get(url).build();
        HttpResponse response = HttpRequests.execute(request);
        Assert.assertEquals(200, response.getResponseCode());
        // 6 is the avg tweet size
        Assert.assertEquals("6", response.getResponseBodyAsString());
      } finally {
        serviceManager.stop();
        serviceManager.waitForStatus(false);
      }
    } finally {
      flowManager.stop();
    }
  }
}
