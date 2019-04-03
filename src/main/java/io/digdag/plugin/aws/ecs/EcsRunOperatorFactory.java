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
import static io.digdag.standards.operator.state.PollingWaiter.pollingWaiter;
import static java.util.stream.Collectors.toList;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import java.time.Duration;
import java.util.List;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.AssignPublicIp;
import com.amazonaws.services.ecs.model.AwsVpcConfiguration;
import com.amazonaws.services.ecs.model.ContainerOverride;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.NetworkConfiguration;
import com.amazonaws.services.ecs.model.PlacementConstraint;
import com.amazonaws.services.ecs.model.PlacementStrategy;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.Task;
import com.amazonaws.services.ecs.model.TaskOverride;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

public final class EcsRunOperatorFactory implements OperatorFactory {
  private static final Logger logger = LoggerFactory.getLogger(EcsRunOperatorFactory.class);

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

      AmazonECSClient client = getEcsClient(config);
      RunTaskRequest runTaskRequest = buildRunTaskRequest(config);

      @SuppressWarnings("unchecked")
      List<String> taskArns = pollingRetryExecutor(state, "submit")
          .withErrorMessage("ECS task %s failed to register", runTaskRequest.getTaskDefinition())
          .withRetryInterval(DurationInterval.of(Duration.ofSeconds(30), Duration.ofMinutes(5)))
          .retryUnless(AmazonServiceException.class, EcsUtils::isDeterministicException)
          .runOnce(List.class, (TaskState state) -> {
            logger.info("Running ECS task {}", runTaskRequest.getTaskDefinition());
            return client.runTask(runTaskRequest).getTasks().stream().map(t -> t.getTaskArn())
                .collect(toList());
          });

      boolean waitForCompletion = config.get("wait", Boolean.class, Boolean.FALSE).booleanValue();
      if (waitForCompletion) {
        waitForTaskCompletion(client, config.get("cluster", String.class), taskArns);
      }

      return TaskResult.defaultBuilder(request).build();
    }

    private void waitForTaskCompletion(@Nonnull AmazonECSClient client, @Nonnull String cluster,
        @Nonnull List<String> taskArns) {
      String lastTaskArn = Iterables.getLast(taskArns);

      pollingWaiter(state, "wait").withWaitMessage("ECS tasks still running")
          .withPollInterval(DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(10)))
          .awaitOnce(String.class,
              pollState -> checkTaskCompletion(client, cluster, lastTaskArn, pollState));
    }
  }

  /**
   * https://docs.aws.amazon.com/cli/latest/reference/ecs/run-task.html
   */
  @Nonnull
  @VisibleForTesting
  static RunTaskRequest buildRunTaskRequest(@Nonnull Config config) {
    RunTaskRequest runTaskRequest =
        new RunTaskRequest().withCluster(config.get("cluster", String.class, null))
            .withTaskDefinition(config.get("task_definition", String.class))
            .withCount(config.get("count", Integer.class, 1))
            .withStartedBy(config.get("started_by", String.class, "digdag"))
            .withGroup(config.get("group", String.class, null))
            .withPlacementConstraints(config.getListOrEmpty("placement_constraints", Config.class)
                .stream().map(EcsRunOperatorFactory::buildPlacementConstraint).collect(toList()))
            .withPlacementStrategy(config.getListOrEmpty("placement_strategies", Config.class)
                .stream().map(EcsRunOperatorFactory::buildPlacementStrategy).collect(toList()))
            .withLaunchType(config.get("launch_type", String.class, "FARGATE"))
            .withPlatformVersion(config.get("platform_version", String.class, "LATEST"));

    Config overrides = config.getNestedOrGetEmpty("overrides");
    if (!overrides.isEmpty()) {
      runTaskRequest.withOverrides(buildTaskOverride(overrides));
    }

    Config networkConfig = config.getNestedOrGetEmpty("networkConfiguration");
    if (!networkConfig.isEmpty()) {
      runTaskRequest.withNetworkConfiguration(buildNetworkConfiguration(networkConfig));
    }

    return runTaskRequest;
  }

  @Nonnull
  private static PlacementConstraint buildPlacementConstraint(@Nonnull Config config) {
    return new PlacementConstraint().withExpression(config.get("expression", String.class))
        .withType(config.get("type", String.class));
  }

  @Nonnull
  private static PlacementStrategy buildPlacementStrategy(@Nonnull Config config) {
    return new PlacementStrategy().withType(config.get("type", String.class))
        .withField(config.get("field", String.class));
  }

  @Nonnull
  private static NetworkConfiguration buildNetworkConfiguration(@Nonnull Config config) {
    NetworkConfiguration networkConfig = new NetworkConfiguration();

    Config awsvpcConfig = config.getNestedOrGetEmpty("awsvpcConfiguration");
    if (!awsvpcConfig.isEmpty()) {
      networkConfig.withAwsvpcConfiguration(new AwsVpcConfiguration()
          .withSubnets(awsvpcConfig.getListOrEmpty("subnets", String.class))
          .withSecurityGroups(awsvpcConfig.getListOrEmpty("securityGroups", String.class))
          .withAssignPublicIp(AssignPublicIp.fromValue(awsvpcConfig.get("assignPublicIp",
              String.class, AssignPublicIp.DISABLED.toString()))));
    }

    return networkConfig;
  }

  @Nonnull
  private static TaskOverride buildTaskOverride(@Nonnull Config config) {
    List<ContainerOverride> overrides = config.getListOrEmpty("container_overrides", Config.class)
        .stream().map(EcsRunOperatorFactory::buildContainerOverride).collect(toList());

    TaskOverride taskOverride =
        new TaskOverride().withTaskRoleArn(config.get("task_role_arn", String.class, null))
            .withContainerOverrides(overrides);

    return taskOverride;
  }

  @Nonnull
  private static ContainerOverride buildContainerOverride(@Nonnull Config config) {
    List<KeyValuePair> kvPairs = config.getListOrEmpty("environment", Config.class).stream()
        .map(EcsUtils::buildKvPair).collect(toList());

    return new ContainerOverride().withName(config.get("name", String.class, null))
        .withCommand(config.getListOrEmpty("command", String.class)).withEnvironment(kvPairs)
        .withCpu(config.get("cpu", Integer.class, null))
        .withMemory(config.get("memory", Integer.class, null))
        .withMemoryReservation(config.get("memory_reservation", Integer.class, null));
  }

  @Nonnull
  private static Optional<String> checkTaskCompletion(@Nonnull AmazonECSClient client,
      @Nonnull String cluster, @Nonnull String taskArn, @Nonnull TaskState pollState) {
    return pollingRetryExecutor(pollState, "poll")
        .withRetryInterval(DurationInterval.of(Duration.ofSeconds(5), Duration.ofSeconds(15)))
        .retryUnless(AmazonServiceException.class, EcsUtils::isDeterministicException).run(s -> {
          DescribeTasksRequest describeTask =
              new DescribeTasksRequest().withTasks(taskArn).withCluster(cluster);
          DescribeTasksResult describeResult = client.describeTasks(describeTask);
          Task taskState = Iterables.getLast(describeResult.getTasks());
          String currentTaskState = taskState.getLastStatus();

          switch (currentTaskState) {
            case "PENDING":
            case "RUNNING":
              return Optional.absent();
            case "STOPPED":
              String stoppedReason = taskState.getStoppedReason();

              List<String> containerStopReasons =
                  taskState.getContainers().stream().filter((container) -> {
                    return java.util.Optional.of(container).map(c -> c.getReason())
                        .map(String::toLowerCase).map(reason -> reason.contains("error"))
                        .orElse(false);
                  }).map((container) -> container.getReason()).collect(toList());

              if (stoppedReason.toLowerCase().contains("error")) {
                throw new TaskExecutionException("ECS task failed with reason: " + stoppedReason);
              } else if (!containerStopReasons.isEmpty()) {
                throw new TaskExecutionException("ECS containers failed with reasons: "
                    + String.join(", ", containerStopReasons));
              } else {
                return Optional.of(currentTaskState);
              }
            default:
              throw new RuntimeException("Unknown ECS task state: " + currentTaskState);
          }
        });
  }


}
