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

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;

/**
 * Procedure that returns the aggregates timeseries sentiment data.
 */
public class StatsProcedure extends AbstractProcedure {

  @UseDataSet(TwitterAnalysisApp.TABLE_NAME)
  private KeyValueTable statsTable;

  @Handle("avgSize")
  public void sentimentAggregates(ProcedureRequest request, ProcedureResponder response) throws Exception {
    long totalCount = statsTable.incrementAndGet(Bytes.toBytes("totalCount"), 0);
    long totalSize = statsTable.incrementAndGet(Bytes.toBytes("totalSize"), 0);
    response.sendJson(ProcedureResponse.Code.SUCCESS, totalCount > 0 ? totalSize / totalCount : 0);
  }
}
