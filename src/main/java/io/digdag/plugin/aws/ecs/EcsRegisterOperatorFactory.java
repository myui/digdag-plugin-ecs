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
import java.time.Duration;
import javax.annotation.Nonnull;
import org.eclipse.jetty.http.HttpStatus;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.protocol.json.SdkStructuredPlainJsonFactory;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.transform.TaskDefinitionJsonUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.JsonUnmarshallerContextImpl;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonParser;

public final class EcsRegisterOperatorFactory implements OperatorFactory {

  @Nonnull
  private final TemplateEngine templateEngine;

  public EcsRegisterOperatorFactory(@Nonnull TemplateEngine templateEngine) {
    this.templateEngine = templateEngine;
  }

  public String getType() {
    return "ecs_register";
  }

  @Override
  public Operator newOperator(@Nonnull OperatorContext context) {
    return new RegisterOperator(context);
  }

  private class RegisterOperator extends EcsBaseOperator {

    public RegisterOperator(@Nonnull OperatorContext context) {
      super(context);
    }

    @Override
    public TaskResult runTask() {
      TaskDefinition taskDefinition = pollingRetryExecutor(state, "start")
          .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
          .retryUnless(AmazonServiceException.class,
              EcsRegisterOperatorFactory::isDeterministicException)
          .runOnce(TaskDefinition.class, (TaskState state) -> {
            return getOrCreateTaskDefinition();
          });

      return buildResult(taskDefinition);
    }

    @Nonnull
    TaskDefinition getOrCreateTaskDefinition() {


    }

    @Nonnull
    TaskResult buildResult(@Nonnull TaskDefinition taskDefinition) {
      ConfigFactory cf = request.getConfig().getFactory();
      Config result = cf.create();
      Config ecs = result.getNestedOrSetEmpty("ecs");

      ecs.set("last_task_family", taskDefinition.getFamily() + ":" + taskDefinition.getRevision());

      return TaskResult.defaultBuilder(request).storeParams(result)
          .addResetStoreParams(ConfigKey.of("ecs", "last_task_family")).build();
    }
  }

  @Nonnull
  static TaskDefinition unmarslallTaskDefinition(@Nonnull String json) {
    final TaskDefinition taskDefinition;
    try {
      JsonParser jsonParser = Jackson.getObjectMapper().getFactory().createParser(json);
      JsonUnmarshallerContext unmarshallerContext = new JsonUnmarshallerContextImpl(jsonParser,
          SdkStructuredPlainJsonFactory.JSON_SCALAR_UNMARSHALLERS, null);
      taskDefinition = TaskDefinitionJsonUnmarshaller.getInstance().unmarshall(unmarshallerContext);
    } catch (Exception e) {
      throw new ConfigException("Can't unmarshall task definition");
    }
    return taskDefinition;
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

}
