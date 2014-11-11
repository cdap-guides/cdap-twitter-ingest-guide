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

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.packs.twitter.TweetCollectorFlowlet;

/**
 * Flow for consuming tweets and persisting their count and size in a dataset.
 */
public class TweetAnalysisFlow implements Flow {
  static final String NAME = "TweetAnalysisFlow";
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(NAME)
      .setDescription("Collects simple tweet stats")
      .withFlowlets()
        .add("collect", new TweetCollectorFlowlet())
        .add("recordStats", new StatsRecorderFlowlet())
      .connect()
        .from("collect").to("recordStats")
      .build();
  }
}
