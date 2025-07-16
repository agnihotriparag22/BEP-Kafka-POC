EVENT_TYPES = {
    "policy": [
        "policy_created", "policy_updated", "policy_deleted", "policy_renewed", "policy_expired", "policy_suspended"
    ],
    "claim": [
        "claim_created", "claim_updated", "claim_deleted", "claim_approved", "claim_rejected", "claim_reviewed"
    ],
    "payment": [
        "payment_created", "payment_updated", "payment_deleted", "payment_processed", "payment_failed", "payment_refunded"
    ]
}

# Flatten all event names for provider
all_event_names = [name for names in EVENT_TYPES.values() for name in names] 