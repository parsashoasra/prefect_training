from hello_world import main
from prefect.deployments import Deployment
from prefect.filesystems import GitHub

gh = GitHub.load("prefect-training-git")

deploy_gh = Deployment.build_from_flow(
    flow=main,
    name="GH Python Deploy",
    storage=gh,
)

if __name__ == "__main__":
    deploy_gh.apply()