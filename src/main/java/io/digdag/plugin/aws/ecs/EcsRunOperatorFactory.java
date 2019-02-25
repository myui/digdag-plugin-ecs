/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.digdag.plugin.aws.ecs;

import static java.util.stream.Collectors.toList;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import java.util.List;
import javax.annotation.Nonnull;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.PlacementConstraint;
import com.amazonaws.services.ecs.model.PlacementStrategy;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.amazonaws.services.ecs.model.transform.RunTaskRequestMarshaller;

public final class EcsRunOperatorFactory implements OperatorFactory {

  public EcsRunOperatorFactory() {}

  @Override
  public String getType() {
    return "ecs_run";
  }

  @Override
  public Operator newOperator(OperatorContext context) {
    return new RunOperator(context);
  }

  private static class RunOperator extends EcsBaseOperator {

    public RunOperator(OperatorContext context) {
      super(context);
    }

    @Override
    public TaskResult runTask() {
      Config config =
          request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("ecs"));

      AmazonECSClient client = getEcsClient();
      
      

      return TaskResult.defaultBuilder(request).build();
    }

  }
  
  private RunTaskRequest buildRunTaskRequest(@Nonnull Config config) {
    RunTaskRequest request = null;
    
    
    
    return request;
}


}
