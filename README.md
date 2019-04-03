# digdag-plugin-ecs
[![Jitpack](https://jitpack.io/v/myui/digdag-plugin-ecs.svg)](https://jitpack.io/#myui/digdag-plugin-ecs) [![Digdag](https://img.shields.io/badge/digdag-v0.9.28-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.28)

# Task execution using local repository

## 1) build

```sh
./gradlew clean publish
```

Artifacts are build on local repos: `./build/repo`.

aws --profile engineering logs create-log-group --log-group-name /myui/digdag-ecs-plugin-test --region us-east-1

## 2) run an example

```sh
digdag selfupdate

digdag secrets --local --set aws.profile
digdag secrets --local --set aws.access_key_id
digdag secrets --local --set aws.secret_access_key
digdag secrets --local --set aws.region

digdag secrets --local --set aws.execution_role_arn
digdag secrets --local --set aws.vpc.subnets
digdag secrets --local --set aws.vpc.security_groups

rm -rf .digdag/plugins
digdag run --project sample plugin.dig -p repos=`pwd`/build/repo --rerun

export execution_role_arn=xxx
export subnets=xxx
export security_groups=xxx
digdag run --project sample plugin.dig --rerun \
	-p repos=`pwd`/build/repo \
	-p aws.execution_role_arn=$execution_role_arn \
	-p aws.vpc.subnets=$subnets \
	-p aws.vpc.security_groups=$security_groups
```

# Publishing your plugin using Github and Jitpack

[Jitpack](https://jitpack.io/) is useful for publishing your github repository as a maven repository.

```sh
git tag v0.1.3
git push origin v0.1.3
```

https://jitpack.io/#myui/digdag-plugin-example/v0.1.3

Now, you can load the artifact from a github repository in [a dig file](https://github.com/myui/digdag-plugin-example/blob/master/sample/plugin.dig) as follows:

```
_export:
  plugin:
    repositories:
      # - file://${repos}
      - https://jitpack.io
    dependencies:
      # - io.digdag.plugin:digdag-plugin-ecs:0.1.3
      - com.github.myui:digdag-plugin-ecs:v0.1.3
```

