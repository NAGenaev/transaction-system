# common/constants.py
# Kafka Topics
TOPIC_API_TO_ORCH = "transaction-events"  # API → Orchestrator
TOPIC_ORCH_TO_WORKER = "transaction-processed"  # Orchestrator → Worker