"""Weights & Biases resource for Dagster."""

from dagster import ConfigurableResource, get_dagster_logger


class WandBResource(ConfigurableResource):
    """W&B resource for publishing ML artifacts."""

    entity: str | None = None  # W&B team/user (None = default)
    project: str = "flood-forecasting"

    def publish_artifact(
        self,
        file_path: str,
        artifact_name: str,
        artifact_type: str = "dataset",
        description: str | None = None,
        metadata: dict | None = None,
    ) -> str:
        """Publish a file as a W&B artifact.

        Args:
            file_path: Path to the file to upload
            artifact_name: Name for the artifact
            artifact_type: Type of artifact (dataset, model, etc.)
            description: Optional description
            metadata: Optional metadata dict to attach

        Returns:
            The artifact version string (e.g., "v0", "v1")
        """
        import wandb

        logger = get_dagster_logger()

        run = wandb.init(
            entity=self.entity,
            project=self.project,
            job_type=f"publish-{artifact_type}",
        )

        artifact = wandb.Artifact(
            name=artifact_name,
            type=artifact_type,
            description=description,
            metadata=metadata or {},
        )
        artifact.add_file(file_path)

        run.log_artifact(artifact)
        artifact.wait()  # Wait for upload to complete

        version = artifact.version
        logger.info(f"Published {artifact_name}:{version} to W&B")

        run.finish()
        return version

    def get_latest_artifact(
        self,
        artifact_name: str,
        artifact_type: str = "dataset",
    ) -> str | None:
        """Download the latest version of an artifact.

        Args:
            artifact_name: Name of the artifact
            artifact_type: Type of artifact

        Returns:
            Path to the downloaded artifact directory, or None if not found
        """
        import wandb

        logger = get_dagster_logger()

        api = wandb.Api()

        if self.entity:
            artifact_path = f"{self.entity}/{self.project}/{artifact_name}:latest"
        else:
            artifact_path = f"{self.project}/{artifact_name}:latest"

        try:
            artifact = api.artifact(artifact_path, type=artifact_type)
            artifact_dir = artifact.download()
            logger.info(f"Downloaded {artifact_path}")
            return artifact_dir
        except wandb.errors.CommError:
            logger.info(f"Artifact {artifact_path} not found")
            return None
