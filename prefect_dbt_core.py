from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow

@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt build -t dev"],
        project_dir="datapipe",
        profiles_dir="C:/Users/manda/.dbt"
    ).run()
    return result

if __name__ == "__main__":
    trigger_dbt_flow()
