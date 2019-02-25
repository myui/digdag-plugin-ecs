package io.digdag.plugin.aws.ecs;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.END_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.FIELD_NAME;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.fasterxml.jackson.core.JsonToken.VALUE_NULL;
import javax.annotation.Nonnull;
import com.amazonaws.protocol.json.SdkStructuredPlainJsonFactory;
import com.amazonaws.services.ecs.model.ContainerDefinition;
import com.amazonaws.services.ecs.model.PlacementConstraint;
import com.amazonaws.services.ecs.model.PlacementStrategy;
import com.amazonaws.services.ecs.model.RegisterTaskDefinitionRequest;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.TaskDefinitionPlacementConstraint;
import com.amazonaws.services.ecs.model.Volume;
import com.amazonaws.services.ecs.model.transform.ContainerDefinitionJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.NetworkConfigurationJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.PlacementConstraintJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.PlacementStrategyJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.TagJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.TaskDefinitionPlacementConstraintJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.TaskOverrideJsonUnmarshaller;
import com.amazonaws.services.ecs.model.transform.VolumeJsonUnmarshaller;
import com.amazonaws.transform.JsonUnmarshallerContext;
import com.amazonaws.transform.JsonUnmarshallerContextImpl;
import com.amazonaws.transform.ListUnmarshaller;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public final class EcsUtils {

  private EcsUtils() {}

  @Nonnull
  public static RegisterTaskDefinitionRequest unmarshallRegisterTaskDefinitionRequest(
      @Nonnull final String json) throws Exception {
    JsonParser jsonParser = Jackson.getObjectMapper().getFactory().createParser(json);
    final JsonUnmarshallerContext context = new JsonUnmarshallerContextImpl(jsonParser,
        SdkStructuredPlainJsonFactory.JSON_SCALAR_UNMARSHALLERS, null);

    int originalDepth = context.getCurrentDepth();
    String currentParentElement = context.getCurrentParentElement();
    int targetDepth = originalDepth + 1;

    JsonToken token = context.getCurrentToken();
    if (token == null)
      token = context.nextToken();
    if (token == VALUE_NULL) {
      return null;
    }

    final RegisterTaskDefinitionRequest request = new RegisterTaskDefinitionRequest();
    while (true) {
      if (token == null)
        break;

      if (token == FIELD_NAME || token == START_OBJECT) {
        if (context.testExpression("family", targetDepth)) {
          context.nextToken();
          request.setFamily(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("taskRoleArn", targetDepth)) {
          context.nextToken();
          request.setTaskRoleArn(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("executionRoleArn", targetDepth)) {
          context.nextToken();
          request.setExecutionRoleArn(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("networkMode", targetDepth)) {
          context.nextToken();
          request.setNetworkMode(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("containerDefinitions", targetDepth)) {
          context.nextToken();
          request.setContainerDefinitions(new ListUnmarshaller<ContainerDefinition>(
              ContainerDefinitionJsonUnmarshaller.getInstance()).unmarshall(context));
        }
        if (context.testExpression("volumes", targetDepth)) {
          context.nextToken();
          request.setVolumes(new ListUnmarshaller<Volume>(VolumeJsonUnmarshaller.getInstance())
              .unmarshall(context));
        }
        if (context.testExpression("placementConstraints", targetDepth)) {
          context.nextToken();
          request.setPlacementConstraints(new ListUnmarshaller<TaskDefinitionPlacementConstraint>(
              TaskDefinitionPlacementConstraintJsonUnmarshaller.getInstance()).unmarshall(context));
        }
        if (context.testExpression("requiresCompatibilities", targetDepth)) {
          context.nextToken();
          request.setRequiresCompatibilities(
              new ListUnmarshaller<String>(context.getUnmarshaller(String.class))
                  .unmarshall(context));
        }
        if (context.testExpression("cpu", targetDepth)) {
          context.nextToken();
          request.setCpu(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("memory", targetDepth)) {
          context.nextToken();
          request.setMemory(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("tags", targetDepth)) {
          context.nextToken();
          request.setTags(
              new ListUnmarshaller<Tag>(TagJsonUnmarshaller.getInstance()).unmarshall(context));
        }
        if (context.testExpression("pidMode", targetDepth)) {
          context.nextToken();
          request.setPidMode(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("ipcMode", targetDepth)) {
          context.nextToken();
          request.setIpcMode(context.getUnmarshaller(String.class).unmarshall(context));
        }
      } else if (token == END_ARRAY || token == END_OBJECT) {
        if (context.getLastParsedParentElement() == null
            || context.getLastParsedParentElement().equals(currentParentElement)) {
          if (context.getCurrentDepth() <= originalDepth)
            break;
        }
      }
      token = context.nextToken();
    }

    return request;
  }

  @Nonnull
  public static RunTaskRequest unmarshallRunTaskRequest(@Nonnull final String json)
      throws Exception {
    JsonParser jsonParser = Jackson.getObjectMapper().getFactory().createParser(json);
    final JsonUnmarshallerContext context = new JsonUnmarshallerContextImpl(jsonParser,
        SdkStructuredPlainJsonFactory.JSON_SCALAR_UNMARSHALLERS, null);

    int originalDepth = context.getCurrentDepth();
    String currentParentElement = context.getCurrentParentElement();
    int targetDepth = originalDepth + 1;

    JsonToken token = context.getCurrentToken();
    if (token == null)
      token = context.nextToken();
    if (token == VALUE_NULL) {
      return null;
    }

    final RunTaskRequest request = new RunTaskRequest();
    while (true) {
      if (token == null)
        break;

      if (token == FIELD_NAME || token == START_OBJECT) {
        if (context.testExpression("cluster", targetDepth)) {
          context.nextToken();
          request.setCluster(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("taskDefinition", targetDepth)) {
          context.nextToken();
          request.setTaskDefinition(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("overrides", targetDepth)) {
          context.nextToken();
          request.setOverrides(TaskOverrideJsonUnmarshaller.getInstance().unmarshall(context));
        }
        if (context.testExpression("count", targetDepth)) {
          context.nextToken();
          request.setCount(context.getUnmarshaller(Integer.class).unmarshall(context));
        }
        if (context.testExpression("startedBy", targetDepth)) {
          context.nextToken();
          request.setStartedBy(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("group", targetDepth)) {
          context.nextToken();
          request.setGroup(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("placementConstraints", targetDepth)) {
          context.nextToken();
          request.setPlacementConstraints(new ListUnmarshaller<PlacementConstraint>(
              PlacementConstraintJsonUnmarshaller.getInstance()).unmarshall(context));
        }
        if (context.testExpression("placementStrategy", targetDepth)) {
          context.nextToken();
          request.setPlacementStrategy(new ListUnmarshaller<PlacementStrategy>(
              PlacementStrategyJsonUnmarshaller.getInstance()).unmarshall(context));
        }
        if (context.testExpression("launchType", targetDepth)) {
          context.nextToken();
          request.setLaunchType(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("platformVersion", targetDepth)) {
          context.nextToken();
          request.setPlatformVersion(context.getUnmarshaller(String.class).unmarshall(context));
        }
        if (context.testExpression("networkConfiguration", targetDepth)) {
          context.nextToken();
          request.setNetworkConfiguration(
              NetworkConfigurationJsonUnmarshaller.getInstance().unmarshall(context));
        }
        if (context.testExpression("tags", targetDepth)) {
          context.nextToken();
          request.setTags(
              new ListUnmarshaller<Tag>(TagJsonUnmarshaller.getInstance()).unmarshall(context));
        }
        if (context.testExpression("enableECSManagedTags", targetDepth)) {
          context.nextToken();
          request
              .setEnableECSManagedTags(context.getUnmarshaller(Boolean.class).unmarshall(context));
        }
        if (context.testExpression("propagateTags", targetDepth)) {
          context.nextToken();
          request.setPropagateTags(context.getUnmarshaller(String.class).unmarshall(context));
        }
      } else if (token == END_ARRAY || token == END_OBJECT) {
        if (context.getLastParsedParentElement() == null
            || context.getLastParsedParentElement().equals(currentParentElement)) {
          if (context.getCurrentDepth() <= originalDepth)
            break;
        }
      }
      token = context.nextToken();
    }

    return request;
  }

}
