EVENT_TYPES = {
    "Policy": [
        "AONE.Completed",
        "AONE.Initiated",
        "AONE.Updated",
        "AONE.Cancelled",
        "AONE.Renewed"
    ],
    "User": [
        "BTWO.Initiated",
        "BTWO.Registered",
        "BTWO.Updated",
        "BTWO.Deleted",
        "BTWO.Locked"
    ],
    "Claim": [
        "CTHREE.Failed",
        "CTHREE.Submitted",
        "CTHREE.Approved",
        "CTHREE.Rejected",
        "CTHREE.Closed"
    ],
    "Document": [
        "DFOUR.Updated",
        "DFOUR.Uploaded",
        "DFOUR.Deleted",
        "DFOUR.Verified",
        "DFOUR.Shared"
    ],
    "Account": [
        "EFIVE.Created",
        "EFIVE.Updated",
        "EFIVE.Deleted",
        "EFIVE.Suspended",
        "EFIVE.Reactivated"
    ]
}

EVENT_SENDERS = ['MS', 'PORTAL', 'API', 'SYSTEM', 'USER']
EVENT_TENANTS = ['TDB', 'TST', 'PRD', 'DEV', 'QA'] 