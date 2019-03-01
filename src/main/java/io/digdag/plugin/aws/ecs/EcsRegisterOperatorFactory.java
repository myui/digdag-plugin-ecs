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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionResult;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionResult;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.google.common.io.CharStreams;

public final class EcsRegisterOperatorFactory implements OperatorFactory {
  private static final Logger logger = LoggerFactory.getLogger(RegisterOperator.class);

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

      AmazonECSClient client = getEcsClient(config);

      TaskDefinition taskDefinition = pollingRetryExecutor(state, "start")
          .withErrorMessage("PollingRetryExecutor failed running getOrCreateTaskDefinition")
          .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
          .retryUnless(AmazonServiceException.class,
              EcsRegisterOperatorFactory::isDeterministicException)
          .runOnce(TaskDefinition.class, (TaskState state) -> {
            return getOrCreateTaskDefinition(client, config);
          });

      // store a task definition in ${ecs/last-task}
      return buildResult(taskDefinition);
    }

    @Nonnull
    TaskDefinition getOrCreateTaskDefinition(@Nonnull AmazonECSClient client,
        @Nonnull Config config) {
      String taskDefinition = config.get("task-definition", String.class);
      String fileName = config.get("file", String.class);

      final TaskDefinition taskDef;
      if (taskDefinition != null && fileName == null) {
        // get an existing task definition
        taskDef = getTaskDefinition(client, taskDefinition);
      } else if (fileName != null && taskDefinition == null) {
        // register a new task definition
        taskDef = registerTaskDefinition(client, fileName, config);
      } else {
        throw new ConfigException("Please specify one of 'task-definition' and 'file' option");
      }
      return taskDef;
    }

    @Nonnull
    TaskDefinition registerTaskDefinition(@Nonnull AmazonECSClient client, @Nonnull String fileName,
        @Nonnull Config config) {
      final RegisterTaskDefinitionRequest request;
      // read task definition from file
      try (BufferedReader reader = workspace.newBufferedReader(fileName, UTF_8)) {
        String json = CharStreams.toString(reader);
        request = EcsUtils.unmarshallRegisterTaskDefinitionRequest(json);
      } catch (Exception ex) {
        throw new ConfigException("Failed to run register-task-definition: " + fileName, ex);
      }
      // overwrite task definition by task config
      

      logger.info("Registering a task family definition {}", request.getFamily());
      RegisterTaskDefinitionResult result = client.registerTaskDefinition(request);

      return result.getTaskDefinition();
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
    final int statusCode = ex.getStatusCode();
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

    logger.info("Describing a task definition {}", taskDefinition);
    DescribeTaskDefinitionResult result = client.describeTaskDefinition(request);

    return result.getTaskDefinition();
  }

}
