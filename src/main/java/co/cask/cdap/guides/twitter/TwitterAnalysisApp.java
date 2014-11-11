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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * Application that consumes a live tweet stream and computes the average size of a tweet.
 */
public class TwitterAnalysisApp extends AbstractApplication {
  static final String NAME = "TwitterAnalysis";
  static final String TABLE_NAME = "tweetStats";
  static final String SERVICE_NAME = "TweetStatsService";

  @Override
  public void configure() {
    setName(NAME);
    createDataset(TABLE_NAME, KeyValueTable.class);
    addFlow(new TweetAnalysisFlow());
    addService(SERVICE_NAME, new TweetStatsHandler());
  }
}
