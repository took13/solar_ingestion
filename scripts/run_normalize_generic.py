from src.main import build_app
from src.normalize_jobs.generic_normalize_job import GenericNormalizeJob


def main():
    app = build_app()

    limit = app.app_config.get("pipeline", {}).get("generic_metrics", {}).get("pending_limit", 100)

    job = GenericNormalizeJob(
        conn=app.conn,
        metadata_repo=app.metadata_repo,
        chunk_size=app.app_config.get("pipeline", {}).get("generic_metrics", {}).get("normalize_chunk_size", 5000),
    )
    job.run(limit=limit)


if __name__ == "__main__":
    main()