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
import static java.util.stream.Collectors.toList;
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
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.Compatibility;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.DescribeTaskDefinitionResult;
import com.amazonaws.services.ecs.model.HostEntry;
import com.amazonaws.services.ecs.model.HostVolumeProperties;
import com.amazonaws.services.ecs.model.KeyValuePair;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsRequest;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsResult;
import com.amazonaws.services.ecs.model.LogConfiguration;
import com.amazonaws.services.ecs.model.MountPoint;
import com.amazonaws.services.ecs.model.NetworkMode;
import com.amazonaws.services.ecs.model.PortMapping;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionResult;
import com.amazonaws.services.ecs.model.SortOrder;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.ecs.model.TaskDefinitionPlacementConstraint;
import com.amazonaws.services.ecs.model.Ulimit;
import com.amazonaws.services.ecs.model.Volume;
import com.amazonaws.services.ecs.model.VolumeFrom;
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
          .retryUnless(AmazonServiceException.class, EcsUtils::isDeterministicException)
          .runOnce(TaskDefinition.class, (TaskState state) -> {
            return getOrCreateTaskDefinition(client, config);
          });

      // store a task definition in ${ecs/last-task}
      return buildResult(taskDefinition);
    }

    @Nonnull
    TaskDefinition getOrCreateTaskDefinition(@Nonnull AmazonECSClient client,
        @Nonnull Config config) {
      String taskDefinition = config.get("task-definition", String.class, null);

      final TaskDefinition taskDef;
      if (taskDefinition != null) {
        // get an existing task definition
        taskDef = getTaskDefinition(client, taskDefinition);
      } else {
        // register a new task definition
        taskDef = getOrRegisterTaskDefinition(client, config);
      }
      return taskDef;
    }

    @Nonnull
    TaskDefinition getOrRegisterTaskDefinition(@Nonnull AmazonECSClient client,
        @Nonnull Config config) {
      RegisterTaskDefinitionRequest request = getRegisterTaskDefinitionRequest(config);

      final TaskDefinition taskDef;
      DescribeTaskDefinitionResult describeResult = listAndDescribeTaskDefinition(client, request);
      if (describeResult != null && EcsUtils.isReusableTaskDef(describeResult, request)) {
        taskDef = describeResult.getTaskDefinition();
        logger.info("Reusing an existing task definition {}", taskDef.getTaskDefinitionArn());
      } else {
        logger.info("Registering a task family {}", request.getFamily());
        RegisterTaskDefinitionResult result = client.registerTaskDefinition(request);
        taskDef = result.getTaskDefinition();
        logger.info("Registered a task definition {}", taskDef.getTaskDefinitionArn());
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Got a task definition: " + taskDef);
      }

      return taskDef;
    }

    /**
     * https://docs.aws.amazon.com/cli/latest/reference/ecs/register-task-definition.html
     */
    @Nonnull
    RegisterTaskDefinitionRequest getRegisterTaskDefinitionRequest(@Nonnull Config config) {
      String fileName = config.get("file", String.class, null);

      final RegisterTaskDefinitionRequest request;
      if (fileName == null) {
        request = new RegisterTaskDefinitionRequest().withFamily(config.get("family", String.class))
            .withTaskRoleArn(config.get("task_role_arn", String.class, null))
            .withExecutionRoleArn(config.get("executionRoleArn", String.class, null))
            .withNetworkMode(
                config.get("network_mode", String.class, NetworkMode.Awsvpc.toString()))
            .withPlacementConstraints(
                config.getListOrEmpty("placement_constraints", Config.class).stream()
                    .map(EcsRegisterOperatorFactory::buildPlacementConstraint).collect(toList()))
            .withVolumes(config.getListOrEmpty("volumes", Config.class).stream()
                .map(EcsRegisterOperatorFactory::buildVolume).collect(toList()))
            .withContainerDefinitions(
                config.getListOrEmpty("container_definitions", Config.class).stream()
                    .map(EcsRegisterOperatorFactory::buildContainerDefinition).collect(toList()))
            .withRequiresCompatibilities(buildCompatibility(config))
            .withCpu(config.get("cpu", String.class, "256"))
            .withMemory(config.get("memory", String.class, "512"));
      } else {
        // read task definition from file
        try (BufferedReader reader = workspace.newBufferedReader(fileName, UTF_8)) {
          String json = CharStreams.toString(reader);
          request = EcsUtils.unmarshallRegisterTaskDefinitionRequest(json);
        } catch (Exception ex) {
          throw new ConfigException("Failed to run register-task-definition: " + fileName, ex);
        }
      }
      return request;
    }

    @Nullable
    DescribeTaskDefinitionResult listAndDescribeTaskDefinition(@Nonnull AmazonECSClient client,
        @Nonnull RegisterTaskDefinitionRequest request) {
      String familyName = request.getFamily();

      String lastToken = null;
      ListTaskDefinitionsResult listTaskDefinitions = client.listTaskDefinitions(
          new ListTaskDefinitionsRequest().withFamilyPrefix(familyName).withMaxResults(1)
              .withStatus("ACTIVE").withSort(SortOrder.DESC).withNextToken(lastToken));

      DescribeTaskDefinitionResult describeTaskDefinition = null;
      if (listTaskDefinitions.getTaskDefinitionArns().size() > 0) {
        describeTaskDefinition = client.describeTaskDefinition((new DescribeTaskDefinitionRequest()
            .withTaskDefinition(listTaskDefinitions.getTaskDefinitionArns().get(0))));
      }
      return describeTaskDefinition;
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

  @Nonnull
  static TaskDefinition getTaskDefinition(@Nonnull AmazonECSClient client,
      @Nonnull String taskDefinition) {
    DescribeTaskDefinitionRequest request =
        new DescribeTaskDefinitionRequest().withTaskDefinition(taskDefinition);

    logger.info("Describing a task definition {}", taskDefinition);
    DescribeTaskDefinitionResult result = client.describeTaskDefinition(request);

    return result.getTaskDefinition();
  }

  @Nonnull
  private static Volume buildVolume(@Nonnull Config config) {
    String name = config.get("name", String.class);

    HostVolumeProperties host = new HostVolumeProperties()
        .withSourcePath(config.get("host_volume_properties.source_path", String.class));

    return new Volume().withName(name).withHost(host);
  }

  @Nonnull
  private static TaskDefinitionPlacementConstraint buildPlacementConstraint(
      @Nonnull Config config) {
    return new TaskDefinitionPlacementConstraint()
        .withExpression(config.get("expression", String.class))
        .withType(config.get("type", String.class));
  }

  @Nonnull
  private static Compatibility[] buildCompatibility(@Nonnull Config config) {
    List<Compatibility> list = config.getListOrEmpty("requiresCompatibilities", String.class)
        .stream().map(Compatibility::fromValue).collect(toList());
    if (list.isEmpty()) {
      return new Compatibility[] {Compatibility.FARGATE};
    }
    return list.toArray(new Compatibility[list.size()]);
  }

  @Nonnull
  private static ContainerDefinition buildContainerDefinition(@Nonnull Config config) {
    List<KeyValuePair> kvPairs = config.getListOrEmpty("environment", Config.class).stream()
        .map(EcsUtils::buildKvPair).collect(toList());

    List<HostEntry> extraHosts = config.getListOrEmpty("extra_hosts", Config.class).stream()
        .map(EcsRegisterOperatorFactory::buildHostEntry).collect(toList());

    LogConfiguration logConfiguration =
        buildLogConfiguration(config.parseNestedOrGetEmpty("log_configuration"));

    List<MountPoint> mountPoints = config.getListOrEmpty("mount_points", Config.class).stream()
        .map(EcsRegisterOperatorFactory::buildMountPoint).collect(toList());

    List<PortMapping> portMappings = config.getListOrEmpty("port_mappings", Config.class).stream()
        .map(EcsRegisterOperatorFactory::buildPortMapping).collect(toList());

    List<Ulimit> ulimits = config.getListOrEmpty("ulimits", Config.class).stream()
        .map(EcsRegisterOperatorFactory::buildUlimit).collect(toList());

    List<VolumeFrom> volumesFrom = config.getListOrEmpty("volumes_from", Config.class).stream()
        .map(EcsRegisterOperatorFactory::buildVolumeFrom).collect(toList());

    ContainerDefinition containerDefinition = new ContainerDefinition()
        .withCommand(config.getListOrEmpty("command", String.class))
        .withCpu(config.get("cpu", Integer.class, null))
        .withDisableNetworking(config.get("disabled_networking", Boolean.class, null))
        .withDnsSearchDomains(config.getListOrEmpty("dns_search_domains", String.class))
        .withDnsServers(config.getListOrEmpty("dns_servers", String.class))
        .withDockerSecurityOptions(config.getListOrEmpty("docker_security_options", String.class))
        .withEntryPoint(config.getListOrEmpty("entry_point", String.class)).withEnvironment(kvPairs)
        .withEssential(config.get("essential", Boolean.class, null)).withExtraHosts(extraHosts)
        .withHostname(config.get("hostname", String.class, null))
        .withImage(config.get("image", String.class))
        .withLinks(config.getListOrEmpty("links", String.class))
        .withLogConfiguration(logConfiguration)
        .withMemory(config.get("memory", Integer.class, null))
        .withMemoryReservation(config.get("memory_reservation", Integer.class, null))
        .withMountPoints(mountPoints).withName(config.get("name", String.class))
        .withPortMappings(portMappings)
        .withPrivileged(config.get("privileged", Boolean.class, null))
        .withReadonlyRootFilesystem(config.get("readonly_root_filesystem", Boolean.class, null))
        .withUlimits(ulimits).withUser(config.get("user", String.class, null))
        .withVolumesFrom(volumesFrom)
        .withWorkingDirectory(config.get("working_directory", String.class, null));

    Map<String, String> dockerLabels =
        config.getMapOrEmpty("docker_labels", String.class, String.class);

    if (!dockerLabels.isEmpty()) {
      containerDefinition.withDockerLabels(dockerLabels);
    }

    return containerDefinition;
  }

  @Nonnull
  private static HostEntry buildHostEntry(@Nonnull Config config) {
    return new HostEntry().withHostname(config.get("hostname", String.class))
        .withIpAddress(config.get("ip_address", String.class));
  }

  @Nonnull
  private static Ulimit buildUlimit(@Nonnull Config config) {
    return new Ulimit().withName(config.get("name", String.class))
        .withSoftLimit(config.get("soft_limit", Integer.class))
        .withHardLimit(config.get("hard_limit", Integer.class));
  }

  @Nonnull
  private static VolumeFrom buildVolumeFrom(@Nonnull Config config) {
    return new VolumeFrom().withSourceContainer(config.get("source_container", String.class))
        .withReadOnly(config.get("read_only", Boolean.class));
  }

  @Nonnull
  private static MountPoint buildMountPoint(@Nonnull Config config) {
    return new MountPoint().withContainerPath(config.get("container_path", String.class))
        .withSourceVolume(config.get("source_volume", String.class))
        .withReadOnly(config.get("read_only", Boolean.class));
  }

  @Nonnull
  private static PortMapping buildPortMapping(@Nonnull Config config) {
    return new PortMapping().withContainerPort(config.get("container_port", Integer.class))
        .withHostPort(config.get("host_port", Integer.class))
        .withProtocol(config.get("protocol", String.class));
  }

  @Nullable
  private static LogConfiguration buildLogConfiguration(@Nonnull Config config) {
    if (config.isEmpty()) {
      return null;
    }
    return new LogConfiguration().withLogDriver(config.get("log_driver", String.class, null))
        .withOptions(config.getMapOrEmpty("options", String.class, String.class));
  }

}
