import os

from pathlib import Path

from prefect import flow
from prefect_dbt.cli import DbtCoreOperation


@flow(log_prints=True)
def trigger_dbt_flow():
    project_dir = Path(os.path.dirname(os.path.realpath(__file__))) / "../../dbt/cycling"
    print("Project dir:", project_dir)
    print("List dir:", *project_dir.glob('*'), sep='\n\t')

    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt run"],
        project_dir=project_dir,
        profiles_dir=project_dir,
        overwrite_profiles=False,
    ).run()
    return result


if __name__ == "__main__":
    trigger_dbt_flow()