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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Service to access tweets stats.
 */
@Path("/v1")
public class TweetStatsHandler extends AbstractHttpServiceHandler {

  @UseDataSet(TwitterAnalysisApp.TABLE_NAME)
  private KeyValueTable statsTable;

  @Path("avgSize")
  @GET
  public void sentimentAggregates(HttpServiceRequest request, HttpServiceResponder responder) throws Exception {
    long totalCount = statsTable.incrementAndGet(Bytes.toBytes("totalCount"), 0);
    long totalSize = statsTable.incrementAndGet(Bytes.toBytes("totalSize"), 0);
    responder.sendJson(totalCount > 0 ? totalSize / totalCount : 0);
  }
}
