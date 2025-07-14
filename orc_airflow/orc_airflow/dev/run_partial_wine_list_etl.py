import subprocess
import logging

logger = logging.getLogger(__name__)
logging.basicConfig


def main():
    task_list = ["extract_doc_data", "add_line_numbers", "aggregate_lines"]

    for task in task_list:
        result = subprocess.run(
            f"airflow tasks test -m wine_list_etl {task}",
            shell=True,
        )

    logger.info(f"Ran {len(task_list)} tasks: {', '.join(task_list)}")


if __name__ == "__main__":
    main()
