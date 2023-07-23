from prefect.deployments import Deployment  
from prefect.infrastructure.container import DockerContainer
from parametrized_flow import etl_parent_flow

docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='docker-flow',
    infrastructure=docker_block
)