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

import static io.digdag.standards.operator.state.PollingRetryExecutor.pollingRetryExecutor;
import static java.nio.charset.StandardCharsets.UTF_8;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import java.io.BufferedReader;
import java.time.Duration;
import javax.annotation.Nonnull;
import org.eclipse.jetty.http.HttpStatus;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionResult;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionResult;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.google.common.io.CharStreams;

public final class EcsRegisterOperatorFactory implements OperatorFactory {

  public EcsRegisterOperatorFactory(@Nonnull TemplateEngine templateEngine) {}

  @Override
  public String getType() {
    return "ecs_register";
  }

  @Override
  public Operator newOperator(@Nonnull OperatorContext context) {
    return new RegisterOperator(context);
  }

  private static class RegisterOperator extends EcsBaseOperator {

    RegisterOperator(@Nonnull OperatorContext context) {
      super(context);
    }

    @Override
    public TaskResult runTask() {
      Config config =
          request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty("ecs"));

      AmazonECSClient client = getEcsClient();

      TaskDefinition taskDefinition = pollingRetryExecutor(state, "start")
          .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
          .retryUnless(AmazonServiceException.class,
              EcsRegisterOperatorFactory::isDeterministicException)
          .runOnce(TaskDefinition.class, (TaskState state) -> {
            return getOrCreateTaskDefinition(client, config);
          });

      return buildResult(taskDefinition);
    }

    @Nonnull
    TaskDefinition getOrCreateTaskDefinition(@Nonnull AmazonECSClient client,
        @Nonnull Config config) {
      String taskDefinition = config.get("task-definition", String.class);
      String fileName = config.get("file", String.class);

      final TaskDefinition taskDef;
      if (taskDefinition != null && fileName == null) {
        taskDef = getTaskDefinition(client, taskDefinition);
      } else if (fileName != null && taskDefinition == null) {
        taskDef = registerTaskDefinition(client, fileName);
      } else {
        throw new ConfigException("Please specify one of 'task-definition' and 'file' option");
      }
      return taskDef;
    }

    @Nonnull
    TaskDefinition registerTaskDefinition(@Nonnull AmazonECSClient client,
        @Nonnull String fileName) {
      try (BufferedReader reader = workspace.newBufferedReader(fileName, UTF_8)) {
        String json = CharStreams.toString(reader);
        RegisterTaskDefinitionRequest request =
            EcsUtils.unmarshallRegisterTaskDefinitionRequest(json);
        RegisterTaskDefinitionResult result = client.registerTaskDefinition(request);
        return result.getTaskDefinition();
      } catch (Exception ex) {
        throw new ConfigException("Failed to run register-task-definition: " + fileName, ex);
      }
    }

    @Nonnull
    TaskResult buildResult(@Nonnull TaskDefinition taskDefinition) {
      ConfigFactory cf = request.getConfig().getFactory();
      Config result = cf.create();
      Config ecs = result.getNestedOrSetEmpty("ecs");

      ecs.set("last_task", taskDefinition.getFamily() + ":" + taskDefinition.getRevision());

      return TaskResult.defaultBuilder(request).storeParams(result)
          .addResetStoreParams(ConfigKey.of("ecs", "last_task")).build();
    }
  }

  static boolean isDeterministicException(@Nonnull AmazonServiceException ex) {
    int statusCode = ex.getStatusCode();
    switch (statusCode) {
      case HttpStatus.TOO_MANY_REQUESTS_429:
      case HttpStatus.REQUEST_TIMEOUT_408:
        return false;
      default:
        return statusCode >= 400 && statusCode < 500;
    }
  }

  @Nonnull
  static TaskDefinition getTaskDefinition(@Nonnull AmazonECSClient client,
      @Nonnull String taskDefinition) {
    DescribeTaskDefinitionRequest request =
        new DescribeTaskDefinitionRequest().withTaskDefinition(taskDefinition);
    DescribeTaskDefinitionResult result = client.describeTaskDefinition(request);
    return result.getTaskDefinition();
  }

}
