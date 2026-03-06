"""
Notification helpers for pipeline monitoring
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def send_success_alert(dag_id, execution_date, run_id):
    """Send success notification"""
    message = f"""
Pipeline Success
================
DAG: {dag_id}
Run ID: {run_id}
Time: {execution_date}
Status: COMPLETED
    """

    # TODO: Add Slack webhook integration
    # slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    # if slack_webhook:
    #     requests.post(slack_webhook, json={"text": message})

    logger.info(message)
    print("✓ Pipeline completed successfully")


def send_failure_alert(dag_id, execution_date, run_id, error):
    """Send failure notification"""
    message = f"""
Pipeline Failed
===============
DAG: {dag_id}
Run ID: {run_id}
Time: {execution_date}
Error: {error}
    """

    # TODO: Add Slack/email integration

    logger.error(message)
    print(f"✗ Pipeline failed: {error}")


def log_pipeline_stats(stats):
    """Log pipeline statistics"""
    print("\nPipeline Statistics:")
    print("=" * 40)
    for key, value in stats.items():
        print(f"{key}: {value:,}")
    print("=" * 40)
