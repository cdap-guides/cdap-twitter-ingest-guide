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

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.packs.twitter.Tweet;

/**
 * Updates the timeseries table with sentiments received.
 */
public class StatsRecorderFlowlet extends AbstractFlowlet {
  @UseDataSet(TwitterAnalysisApp.TABLE_NAME)
  private KeyValueTable statsTable;

  @ProcessInput
  public void process(Tweet tweet) {
    statsTable.increment(Bytes.toBytes("totalCount"), 1);
    statsTable.increment(Bytes.toBytes("totalSize"), tweet.getText().length());
  }
}